package server

import (
	"context"
	"errors"
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
