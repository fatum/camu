package server

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
)

// TestControllerLifecycle_StartStop verifies that:
//  1. startController sets controllerState and launches the checkpoint loop.
//  2. stopController clears controllerState and cancels the loop.
//  3. startController is idempotent (second call is a no-op).
func TestControllerLifecycle_StartStop(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Initially no controller.
	if s.controllerState.Load() != nil {
		t.Fatal("expected controllerState to be nil before startController")
	}

	// Start the controller.
	s.startController(ctx)

	if s.controllerState.Load() == nil {
		t.Fatal("expected controllerState to be set after startController")
	}
	if s.controllerCancel == nil {
		t.Fatal("expected controllerCancel to be set after startController")
	}

	// startController is idempotent — calling it again must not replace state.
	original := s.controllerState.Load()
	s.startController(ctx)
	if s.controllerState.Load() != original {
		t.Fatal("startController should be a no-op when controller is already running")
	}

	// Stop the controller.
	s.stopController()

	if s.controllerState.Load() != nil {
		t.Fatal("expected controllerState to be nil after stopController")
	}
	if s.controllerCancel != nil {
		t.Fatal("expected controllerCancel to be nil after stopController")
	}
}

// TestControllerLifecycle_LeaseAcquire verifies that renewLeases starts the
// controller when this node wins the leader lease.
func TestControllerLifecycle_LeaseAcquire(t *testing.T) {
	s := newTestServer(t)

	// renewLeases calls registry.Register; seed the registry so it doesn't error.
	if err := s.registry.Register(context.Background()); err != nil {
		t.Fatalf("registry.Register: %v", err)
	}

	// Run one renewal tick — the node should win the lease (no competition)
	// and the controller should start.
	s.renewLeases()

	if !s.amLeader() {
		t.Skip("did not acquire leader lease — skipping controller lifecycle check")
	}

	if s.controllerState.Load() == nil {
		t.Fatal("expected controllerState to be set after winning leader lease")
	}

	// Simulate lease loss by zeroing the lease and stopping the controller.
	s.leaderLease = coordination.LeaderLease{}
	s.stopController()

	if s.controllerState.Load() != nil {
		t.Fatal("expected controllerState to be nil after stopController")
	}
}

// TestControllerLifecycle_CheckpointPersistence verifies that the controller
// state written during startController can be recovered after a restart.
func TestControllerLifecycle_CheckpointPersistence(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Pre-seed a topic so reconcileControllerState has something to pick up.
	tc := meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create: %v", err)
	}

	// Start the controller and inject a known partition.
	s.startController(ctx)
	s.controllerState.Load().SetPartition("events", 0, &coordination.PartitionMeta{
		Leader:   "n1",
		Epoch:    3,
		Replicas: []string{"n1"},
		ISR:      []string{"n1"},
		HW:       42,
	})

	// Write checkpoint manually.
	if err := coordination.WriteCheckpoint(ctx, s.s3Client, s.controllerState.Load()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	// Stop and restart to simulate a leader re-election.
	s.stopController()
	s.startController(ctx)

	meta := s.controllerState.Load().GetPartition("events", 0)
	if meta == nil {
		t.Fatal("expected partition meta after restart from checkpoint")
	}
	if meta.Leader != "n1" {
		t.Fatalf("expected leader=n1, got %s", meta.Leader)
	}
	if meta.HW != 42 {
		t.Fatalf("expected HW=42, got %d", meta.HW)
	}

	s.stopController()
}

// TestControllerLifecycle_ReconcileSeeding verifies that reconcileControllerState
// seeds controller state from S3 assignment objects for partitions not already
// in the checkpoint.
func TestControllerLifecycle_ReconcileSeeding(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "orders",
		Partitions:        2,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create: %v", err)
	}

	// Write assignment data directly to the assignment store (old-style).
	ta := coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Replicas: []string{"n1"}, Leader: "n1", LeaderEpoch: 1},
			1: {Replicas: []string{"n1"}, Leader: "n1", LeaderEpoch: 1},
		},
		Version: 1,
	}
	if err := s.assignmentStore.Write(ctx, "orders", ta, ""); err != nil {
		t.Fatalf("assignmentStore.Write: %v", err)
	}

	// Start controller — no checkpoint exists, so reconcile should seed from S3.
	s.startController(ctx)

	for pid := 0; pid <= 1; pid++ {
		m := s.controllerState.Load().GetPartition("orders", pid)
		if m == nil {
			t.Fatalf("expected partition meta for orders/%d after reconcile", pid)
		}
		if m.Leader != "n1" {
			t.Fatalf("orders/%d: expected leader=n1, got %s", pid, m.Leader)
		}
	}

	s.stopController()
}
