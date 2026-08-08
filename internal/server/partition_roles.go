package server

import (
	"context"
	"errors"
	"log/slog"
	"slices"

	"github.com/maksim/camu/internal/storage"
)

type PartitionRole int

const (
	PartitionRoleNone PartitionRole = iota
	PartitionRoleLeader
	PartitionRoleFollower
)

type PartitionIdentity struct {
	Topic       string
	Partition   int
	Role        PartitionRole
	Leader      string
	LeaderEpoch uint64
	Replicas    []string
	StorageMode string
}

func (s *Server) ResolvePartitionIdentity(ctx context.Context, topic string, partition int) (PartitionIdentity, error) {
	identity := PartitionIdentity{
		Topic:     topic,
		Partition: partition,
	}

	if tc, err := s.topicStore.Get(ctx, topic); err == nil {
		identity.StorageMode = tc.StorageMode
	} else if !errors.Is(err, storage.ErrNotFound) {
		return PartitionIdentity{}, err
	}

	assigned, err := s.readAssignments(ctx, topic)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return PartitionIdentity{}, err
		}
		s.assignmentsMu.RLock()
		local := s.myPartitions[topic][partition]
		s.assignmentsMu.RUnlock()
		if local.Owned {
			identity.Role = PartitionRoleLeader
			identity.Leader = s.instanceID
			identity.LeaderEpoch = local.LeaderEpoch
			identity.Replicas = []string{s.instanceID}
		}
		return identity, nil
	}

	pa, ok := assigned.Partitions[partition]
	if !ok {
		return identity, nil
	}

	identity.Leader = pa.Leader
	identity.LeaderEpoch = pa.LeaderEpoch
	identity.Replicas = append([]string(nil), pa.Replicas...)

	switch {
	case pa.Leader == s.instanceID:
		identity.Role = PartitionRoleLeader
	case slices.Contains(pa.Replicas, s.instanceID):
		identity.Role = PartitionRoleFollower
	default:
		identity.Role = PartitionRoleNone
	}

	return identity, nil
}

func (s *Server) CanRunOwnerJob(topic string, partition int, expectedOwner string, expectedEpoch uint64) bool {
	identity, err := s.ResolvePartitionIdentity(context.Background(), topic, partition)
	if err != nil {
		return false
	}
	return identity.Role == PartitionRoleLeader &&
		identity.Leader == expectedOwner &&
		identity.LeaderEpoch == expectedEpoch
}

// canBecomeLeader reports whether this node may become the partition leader,
// given its durable log end. A replica may only lead a replicated partition
// while its durable log end covers the partition's committed high watermark:
// promoting a node whose log end is below the committed prefix would let it set
// an epoch boundary at that (shorter) log end and truncate committed data held
// by remaining ISR members.
//
// The committed watermark is read from the authoritative ISR store (the
// controller's in-memory view is only a reconciliation-time snapshot that is
// never refreshed during operation, so it cannot be trusted for a fencing
// decision). Membership in the ISR is NOT required: the ISR store is only
// written on membership change, so a caught-up follower can legitimately be
// absent from it at failover time. What matters is that the node's durable log
// end covers the committed prefix.
//
// A partition with no ISR yet (first leader bootstrap) is allowed: the first
// leader creates the ISR. rf=1 topics have no ISR tracking and are allowed. An
// ISR read error fails closed (the promotion is refused) so durability is never
// assumed.
func (s *Server) canBecomeLeader(ctx context.Context, topic string, pid int, durableLogEnd uint64) bool {
	if s.topicStore == nil || s.isrStore == nil {
		return false
	}
	tc, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		slog.Warn("can_become_leader_topic_failed", "topic", topic, "partition", pid, "error", err)
		return false
	}
	if tc.ReplicationFactor <= 1 || tc.UncleanLeaderElection {
		return true
	}
	isrState, err := s.isrStore.Read(ctx, topic, pid)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return true // bootstrap: the first leader creates the ISR
		}
		slog.Warn("can_become_leader_isr_failed", "topic", topic, "partition", pid, "error", err)
		return false
	}
	if durableLogEnd >= isrState.HighWatermark {
		return true
	}
	slog.Warn("can_become_leader_log_end_below_committed",
		"topic", topic, "partition", pid,
		"log_end", durableLogEnd, "committed_hw", isrState.HighWatermark)
	return false
}
