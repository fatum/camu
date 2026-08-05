package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

// becomeLeader is the unified leader-promotion path driven by a controller push.
// It replaces the TOCTOU-racy initPartitionAsLeader and the data-race-prone
// attemptPartitionLeadership with a single, lock-correct implementation that
// trusts the controller-provided HW and epoch history.
func (s *Server) becomeLeader(ctx context.Context, topic string, pid int, req pushAssignmentRequest) error {
	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps == nil {
		return fmt.Errorf("partition state not found for %s/%d", topic, pid)
	}

	// 1. Under ps.mu.Lock — check if already leader at this or higher epoch.
	ps.mu.Lock()
	if ps.isLeader && ps.epoch >= req.Epoch {
		ps.mu.Unlock()
		return nil // already leader at this or higher epoch
	}
	// Capture and nil out fetch loop handles so we can cancel outside the lock.
	existingCancel := ps.fetchCancel
	existingDone := ps.fetchDone
	ps.fetchCancel = nil
	ps.fetchDone = nil
	ps.fetchAssignmentEpoch = 0
	ps.mu.Unlock()

	// 2. Cancel existing fetch loop (outside the lock, with captured handles).
	if existingCancel != nil {
		existingCancel()
		if existingDone != nil {
			<-existingDone
		}
	}

	// 3. Refresh index from S3 so we see segments flushed by the old leader.
	s.partitionManager.RefreshIndex(ctx, topic, pid)
	if err := s.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("becomeLeader: ensure active segment", "topic", topic, "partition", pid, "error", err)
	}

	// 4. Recover the true local log end from native storage.
	logEnd := s.partitionManager.recoverLocalLogEnd(topic, pid)

	// 5. Recover the most advanced local ISR tail. The persisted controller HW
	// is asynchronous and can lag acknowledged replicated writes.
	recoveredHW := req.HW
	ps.mu.RLock()
	indexNext := ps.index.NextOffset()
	ps.mu.RUnlock()
	if logEnd > recoveredHW {
		recoveredHW = logEnd
	}
	if indexNext > recoveredHW {
		recoveredHW = indexNext
	}

	// 6. Build epoch history. Load from S3 (authoritative) first, then merge
	// the controller-provided history. The controller's PartitionMeta may be
	// stale when rebalanceLeaders changed leaders without calling ElectLeader.
	eh := &replication.EpochHistory{}
	s3eh, err := s.isrStore.ReadEpochHistory(ctx, topic, pid)
	if err != nil {
		return fmt.Errorf("read epoch history from S3: %w", err)
	}
	if s3eh != nil {
		for _, entry := range s3eh.Entries {
			eh.Append(entry)
		}
	}
	for _, entry := range req.EpochHistory {
		_ = eh.Ensure(replication.EpochEntry{Epoch: entry.Epoch, StartOffset: entry.StartOffset})
	}
	// Controller history usually already records this epoch. Do not create a
	// duplicate boundary when it does.
	hasCurrentEpoch := false
	for _, entry := range eh.Entries {
		if entry.Epoch == req.Epoch {
			hasCurrentEpoch = true
			break
		}
	}
	if !hasCurrentEpoch {
		if err := eh.Ensure(replication.EpochEntry{Epoch: req.Epoch, StartOffset: logEnd}); err != nil {
			return fmt.Errorf("validate controller epoch history: %w", err)
		}
	}

	// 7. Final state update under ps.mu.Lock.
	ps.mu.Lock()
	ps.epochHistory = eh
	ps.isLeader = true
	ps.leaderID = ""
	ps.epoch = req.Epoch
	ps.index.SetHighWatermark(recoveredHW)
	if logEnd > ps.nextOffset {
		ps.nextOffset = logEnd
	}
	ps.mu.Unlock()

	// Persist the leader epoch locally so a later state reload (on demotion or
	// restart) reports the correct epoch of this node's active tail instead of a
	// stale follower epoch. This is a failover-time promotion, not the startup
	// path.
	s.partitionManager.PersistLocalEpoch(topic, pid, req.Epoch)

	// Persist epoch history locally and to S3.
	ehPath := s.partitionManager.EpochHistoryPath(topic, pid)
	if err := eh.SaveToFile(ehPath); err != nil {
		slog.Warn("becomeLeader: save epoch history locally", "topic", topic, "partition", pid, "error", err)
	}
	if err := s.isrStore.WriteEpochHistory(ctx, topic, pid, eh); err != nil {
		slog.Warn("becomeLeader: save epoch history to S3", "topic", topic, "partition", pid, "error", err)
	}

	// 8. Create ReplicaState if replication factor > 1.
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		slog.Error("becomeLeader: get topic config", "topic", topic, "pid", pid, "error", err)
		return fmt.Errorf("get topic config: %w", err)
	}
	if topicCfg.ReplicationFactor > 1 {
		ps.mu.Lock()
		ps.replicaState = replication.NewReplicaState(s.instanceID, recoveredHW, topicCfg.MinInsyncReplicas, s.cfg.Coordination.ISRExpansionThresholdValue())
		ps.replicaState.SetEpochHistory(ps.epochHistory)
		for _, r := range req.Replicas {
			if r != s.instanceID {
				ps.replicaState.AddFollower(r)
			}
		}
		ps.mu.Unlock()

		// Write ISR = [self] to S3 so recovery has a consistent source of truth.
		// The guarded update refuses to clobber a higher-epoch leader's state;
		// a stale-epoch rejection aborts the promotion entirely.
		if err := s.isrStore.Update(ctx, topic, pid, req.Epoch, func(_ replication.ISRState) (replication.ISRState, error) {
			return replication.ISRState{
				ISR:           []string{s.instanceID},
				Leader:        s.instanceID,
				HighWatermark: recoveredHW,
			}, nil
		}); err != nil {
			if s.abortPromotionOnStaleISR(ctx, topic, pid, err, ps) {
				return fmt.Errorf("becomeLeader: %w", err)
			}
		}
	}

	// 9. Recover producer idempotency state from checkpoint plus any local tail.
	checkpointKey := fmt.Sprintf("%s/%d/producers.checkpoint", topic, pid)
	if data, err := s.s3Client.Get(ctx, checkpointKey); err == nil && len(data) > 0 {
		ps.mu.Lock()
		ps.loadProducerCheckpoint(data)
		ps.mu.Unlock()
		slog.Info("idempotency_checkpoint_loaded", "topic", topic, "partition", pid, "size", len(data))
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		slog.Warn("idempotency_checkpoint_load_failed", "topic", topic, "partition", pid, "error", err)
	}

	if source, n := s.partitionManager.RebuildProducerStateFromLocalTail(topic, pid); n > 0 {
		slog.Info("idempotency_local_tail_recovery", "topic", topic, "partition", pid, "source", source, "batches", n)
	}

	// 10. Recovery flush if local native data has not been sealed yet.
	ps.mu.RLock()
	indexNextOffset := ps.index.NextOffset()
	logNextOffset := ps.nextOffset
	ps.mu.RUnlock()
	if recoveredHW > indexNextOffset {
		if err := s.partitionManager.flushRecoveredTail(topic, pid); err != nil {
			slog.Warn("becomeLeader: flush recovered tail",
				"topic", topic,
				"partition", pid,
				"epoch", req.Epoch,
				"recovered_hw", recoveredHW,
				"index_next_offset", indexNextOffset,
				"error", err,
			)
		}
	}

	// 11. Update ownership cache.
	s.assignmentsMu.Lock()
	if s.myPartitions == nil {
		s.myPartitions = make(map[string]map[int]localPartitionAssignment)
	}
	if s.myPartitions[topic] == nil {
		s.myPartitions[topic] = make(map[int]localPartitionAssignment)
	}
	s.myPartitions[topic][pid] = localPartitionAssignment{
		Owned:       true,
		LeaderEpoch: req.Epoch,
	}
	s.assignmentsMu.Unlock()

	slog.Info("becomeLeader: promoted",
		"topic", topic, "partition", pid,
		"epoch", req.Epoch, "hw", recoveredHW,
		"next_offset", logNextOffset, "replicas", len(req.Replicas))

	return nil
}

// reconfigureFollower handles a controller push that tells this node to follow
// a (possibly new) leader. It cancels any existing fetch loop if the leader or
// epoch changed, and starts a new one pointing at the pushed leader.
func (s *Server) reconfigureFollower(ctx context.Context, req pushAssignmentRequest) {
	s.partitionFollower().reconfigureFollower(ctx, req)
}

// containsString reports whether ss contains s.
func containsString(ss []string, s string) bool {
	return slices.Contains(ss, s)
}
