package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

// startController initializes the ControllerState, loads the latest checkpoint
// from S3, reconciles with live assignment data, and launches the background
// checkpoint loop. It is idempotent: if the controller is already running the
// call is a no-op.
func (s *Server) startController(ctx context.Context) {
	if s.controllerState.Load() != nil {
		return // already running
	}
	slog.Info("starting controller")
	cs := coordination.NewControllerState()

	// Load checkpoint from S3.
	if err := coordination.ReadCheckpoint(ctx, s.s3Client, cs); err != nil {
		slog.Warn("no checkpoint found, starting fresh", "err", err)
	}

	// Store before reconcile so reconcileControllerState can use it.
	s.controllerState.Store(cs)

	// Reconcile with live S3 assignment objects.
	s.reconcileControllerState(ctx)

	// Launch checkpoint loop with its own cancellable context so it can be
	// stopped independently when the lease is lost.
	s.controllerCtx, s.controllerCancel = context.WithCancel(context.Background())
	go s.controllerCheckpointLoop(s.controllerCtx)
}

// controllerCheckpointLoop writes the controller state to S3 every 5 seconds
// until ctx is cancelled.
func (s *Server) controllerCheckpointLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cs := s.controllerState.Load()
			if cs == nil {
				return
			}
			if err := coordination.WriteCheckpoint(ctx, s.s3Client, cs); err != nil {
				slog.Error("checkpoint write failed", "err", err)
			}
		}
	}
}

// stopController cancels the checkpoint loop and clears the controller state.
// Called when this node loses the leader lease.
func (s *Server) stopController() {
	if s.controllerCancel != nil {
		s.controllerCancel()
	}
	s.controllerState.Store(nil)
	s.controllerCtx = nil
	s.controllerCancel = nil
	slog.Info("controller stopped")
}

// reconcileControllerState seeds ControllerState from existing S3 assignment
// objects. This provides backwards compatibility: the controller can start with
// correct partition metadata even before all nodes use the push model.
//
// For each known topic, the topic's TopicAssignments are read from S3. Any
// partition that is not already present in the controller state (i.e. was not
// in the last checkpoint) is inserted as a PartitionMeta with the leader and
// replica set taken from the assignment. ISR is initialised to the full replica
// set as a safe approximation; the push model will update it on the next
// report cycle.
//
// TODO: In a future task, poll nodes via GET /v1/internal/node-state instead.
func (s *Server) reconcileControllerState(ctx context.Context) {
	cs := s.controllerState.Load()
	if cs == nil {
		return
	}

	topics, err := s.topicStore.List(ctx)
	if err != nil {
		slog.Warn("reconcileControllerState: list topics failed", "err", err)
		return
	}

	for _, tc := range topics {
		ta, err := s.assignmentStore.Read(ctx, tc.Name)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			slog.Warn("reconcileControllerState: read assignments failed",
				"topic", tc.Name, "err", err)
			continue
		}

		for pid, pa := range ta.Partitions {
			existing := cs.GetPartition(tc.Name, pid)
			if existing != nil && existing.Epoch >= pa.LeaderEpoch && existing.Leader == pa.Leader {
				continue // checkpoint is up-to-date
			}

			isr := make([]string, len(pa.Replicas))
			copy(isr, pa.Replicas)
			meta := &coordination.PartitionMeta{
				Leader:   pa.Leader,
				Epoch:    pa.LeaderEpoch,
				Replicas: pa.Replicas,
				ISR:      isr,
			}

			// Recover HW from the partition's state.json on S3.
			if stateData, err := s.s3Client.Get(ctx, log.StateKey(tc.Name, pid)); err == nil {
				var ps log.PartitionState
				if err := ps.Unmarshal(stateData); err == nil {
					meta.HW = ps.HighWatermark
					epochs := make([]coordination.EpochEntry, len(ps.EpochHistory))
					for i, e := range ps.EpochHistory {
						epochs[i] = coordination.EpochEntry{Epoch: e.Epoch, StartOffset: e.StartOffset}
					}
					meta.EpochHistory = epochs
				}
			}

			cs.SetPartition(tc.Name, pid, meta)
		}
	}
}

// isNotFound reports whether err wraps storage.ErrNotFound.
func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrNotFound)
}
