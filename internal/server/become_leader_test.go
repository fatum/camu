package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

// newTestServerForBecomeLeader creates a minimal Server wired for becomeLeader tests.
func newTestServerForBecomeLeader(t *testing.T) (*Server, *PartitionManager) {
	t.Helper()

	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.Cache.Directory = t.TempDir()
	cfg.Segments.MaxSize = 1
	cfg.Segments.MaxAge = "1h"

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager() error = %v", err)
	}

	topicStore := meta.NewTopicStore(s3Client)
	isrStore := replication.NewISRStore(s3Client)

	srv := &Server{
		cfg:              cfg,
		s3Client:         s3Client,
		topicStore:       topicStore,
		isrStore:         isrStore,
		instanceID:       "node-A",
		myPartitions:     make(map[string]map[int]localPartitionAssignment),
		partitionManager: pm,
	}
	return srv, pm
}

func TestBecomeLeader_SetsPartitionState(t *testing.T) {
	srv, pm := newTestServerForBecomeLeader(t)

	topic := "orders"
	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	ctx := context.Background()
	if err := srv.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	ps := pm.GetPartitionState(topic, 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	requireActive := func() {
		t.Helper()
		if err := pm.ensureActiveSegment(topic, 0); err != nil {
			t.Fatalf("ensureActiveSegment: %v", err)
		}
	}
	requireActive()
	if err := ps.activeSegment.Append(log.EncodeRecordBatch(0, []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
	})); err != nil {
		t.Fatalf("activeSegment.Append: %v", err)
	}
	ps.nextOffset = 2

	req := pushAssignmentRequest{
		Topic:     topic,
		Partition: 0,
		Leader:    "node-A",
		Epoch:     5,
		Replicas:  []string{"node-A"},
		ISR:       []string{"node-A"},
		HW:        2,
		EpochHistory: []coordination.EpochEntry{
			{Epoch: 1, StartOffset: 0},
		},
	}

	if err := srv.becomeLeader(ctx, topic, 0, req); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}

	// Verify partition state.
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if !ps.isLeader {
		t.Error("expected isLeader=true")
	}
	if ps.epoch != 5 {
		t.Errorf("epoch = %d, want 5", ps.epoch)
	}
	if ps.leaderID != "" {
		t.Errorf("leaderID = %q, want empty", ps.leaderID)
	}
	hw := ps.index.HighWatermark()
	if hw != 2 {
		t.Errorf("HW = %d, want 2", hw)
	}
	if ps.nextOffset < 2 {
		t.Errorf("nextOffset = %d, want >= 2", ps.nextOffset)
	}
}

func TestBecomeLeaderPreservesRecoveredISRLocalTail(t *testing.T) {
	srv, pm := newTestServerForBecomeLeader(t)
	topic := "orders"
	tc := meta.TopicConfig{Name: topic, Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1}
	ctx := context.Background()
	if err := srv.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}
	if err := pm.ensureActiveSegment(topic, 0); err != nil {
		t.Fatalf("ensure active: %v", err)
	}
	ps := pm.GetPartitionState(topic, 0)
	if err := ps.activeSegment.Append(log.EncodeRecordBatch(0, []log.Message{{Offset: 0}, {Offset: 1}, {Offset: 2}})); err != nil {
		t.Fatalf("append tail: %v", err)
	}
	ps.nextOffset = 3
	req := pushAssignmentRequest{Topic: topic, Partition: 0, Leader: "node-A", Epoch: 2, HW: 1, Replicas: []string{"node-A"}, EpochHistory: []coordination.EpochEntry{{Epoch: 1, StartOffset: 0}, {Epoch: 2, StartOffset: 1}}}
	if err := srv.becomeLeader(ctx, topic, 0, req); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.nextOffset != 3 || ps.index.HighWatermark() != 3 {
		t.Fatalf("next/HW = %d/%d, want 3/3", ps.nextOffset, ps.index.HighWatermark())
	}
	if got := ps.epochHistory.Entries; len(got) != 2 || got[1].Epoch != 2 || got[1].StartOffset != 1 {
		t.Fatalf("epoch history = %+v, want controller boundaries", got)
	}
}

func TestBecomeLeader_AlreadyLeaderAtEpoch(t *testing.T) {
	srv, pm := newTestServerForBecomeLeader(t)

	topic := "orders"
	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	ctx := context.Background()
	if err := srv.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	ps := pm.GetPartitionState(topic, 0)
	// Pre-set as leader at epoch 5.
	ps.mu.Lock()
	ps.isLeader = true
	ps.epoch = 5
	ps.mu.Unlock()

	req := pushAssignmentRequest{
		Topic:     topic,
		Partition: 0,
		Leader:    "node-A",
		Epoch:     5,
		Replicas:  []string{"node-A"},
		ISR:       []string{"node-A"},
		HW:        0,
	}

	if err := srv.becomeLeader(ctx, topic, 0, req); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}

	// Should be a no-op — still at epoch 5.
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.epoch != 5 {
		t.Errorf("epoch = %d, want 5", ps.epoch)
	}
}

func TestBecomeLeader_WithReplication(t *testing.T) {
	srv, pm := newTestServerForBecomeLeader(t)

	topic := "replicated"
	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	ctx := context.Background()
	if err := srv.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	req := pushAssignmentRequest{
		Topic:     topic,
		Partition: 0,
		Leader:    "node-A",
		Epoch:     3,
		Replicas:  []string{"node-A", "node-B", "node-C"},
		ISR:       []string{"node-A"},
		HW:        0,
		EpochHistory: []coordination.EpochEntry{
			{Epoch: 1, StartOffset: 0},
		},
	}

	if err := srv.becomeLeader(ctx, topic, 0, req); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}

	ps := pm.GetPartitionState(topic, 0)
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if !ps.isLeader {
		t.Error("expected isLeader=true")
	}
	if ps.epoch != 3 {
		t.Errorf("epoch = %d, want 3", ps.epoch)
	}
	if ps.replicaState == nil {
		t.Fatal("expected replicaState to be initialized")
	}
}

func TestBecomeLeader_PartitionNotFound(t *testing.T) {
	srv, _ := newTestServerForBecomeLeader(t)

	req := pushAssignmentRequest{
		Topic:     "nonexistent",
		Partition: 0,
		Leader:    "node-A",
		Epoch:     1,
	}

	err := srv.becomeLeader(context.Background(), "nonexistent", 0, req)
	if err == nil {
		t.Fatal("expected error for nonexistent partition")
	}
}

func TestContainsString(t *testing.T) {
	if !containsString([]string{"a", "b", "c"}, "b") {
		t.Error("expected true for 'b' in [a,b,c]")
	}
	if containsString([]string{"a", "b", "c"}, "d") {
		t.Error("expected false for 'd' in [a,b,c]")
	}
	if containsString(nil, "a") {
		t.Error("expected false for nil slice")
	}
}

// TestBecomeLeader_AbortsOnStaleISREpoch verifies that a promotion whose ISR
// write is rejected because a newer leader epoch already owns the partition
// aborts entirely: the local partition is rolled back and the ownership cache
// is not granted, so the stale node cannot acknowledge writes.
func TestBecomeLeader_AbortsOnStaleISREpoch(t *testing.T) {
	srv, pm := newTestServerForBecomeLeader(t)

	topic := "orders"
	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	ctx := context.Background()
	if err := srv.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	// A newer leader already owns the partition at epoch 5.
	if err := srv.isrStore.Update(ctx, topic, 0, 5, func(_ replication.ISRState) (replication.ISRState, error) {
		return replication.ISRState{
			ISR:           []string{"node-B"},
			Leader:        "node-B",
			HighWatermark: 0,
		}, nil
	}); err != nil {
		t.Fatalf("seed newer ISR: %v", err)
	}

	// Attempt a stale promotion at epoch 3: it must be rejected and rolled back.
	err := srv.becomeLeader(ctx, topic, 0, pushAssignmentRequest{
		Topic:     topic,
		Partition: 0,
		Leader:    "node-A",
		Epoch:     3,
		Replicas:  []string{"node-A", "node-B", "node-C"},
		ISR:       []string{"node-A"},
		HW:        0,
		EpochHistory: []coordination.EpochEntry{
			{Epoch: 1, StartOffset: 0},
		},
	})
	if err == nil {
		t.Fatal("expected promotion to abort on a stale ISR epoch")
	}

	ps := pm.GetPartitionState(topic, 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}
	ps.mu.RLock()
	isLeader := ps.isLeader
	ps.mu.RUnlock()
	if isLeader {
		t.Fatal("partition must not remain marked as leader after an aborted promotion")
	}
	if srv.isOwnedPartition(topic, 0) {
		t.Fatal("ownership must not be granted after an aborted promotion")
	}
}

func TestBecomeLeader_PersistsLeaderEpochSidecar(t *testing.T) {
	srv, pm := newTestServerForBecomeLeader(t)

	topic := "orders"
	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	ctx := context.Background()
	if err := srv.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	// Simulate the stale epoch sidecar a follower leaves behind: the node
	// observed leader epoch 2, then got promoted to epoch 5. Before the fix,
	// promotion never rewrote the sidecar, so a later state reload reported
	// epoch 2 with the node's epoch-5 tail and the next leader's divergence
	// check fenced it, truncating committed data.
	pm.PersistLocalEpoch(topic, 0, 2)

	if err := srv.becomeLeader(ctx, topic, 0, pushAssignmentRequest{
		Topic:     topic,
		Partition: 0,
		Leader:    "node-A",
		Epoch:     5,
		Replicas:  []string{"node-A"},
		ISR:       []string{"node-A"},
		HW:        0,
		EpochHistory: []coordination.EpochEntry{
			{Epoch: 1, StartOffset: 0},
		},
	}); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}

	epochFile := filepath.Join(pm.localPartitionDir(topic, 0), "epoch")
	data, err := os.ReadFile(epochFile)
	if err != nil {
		t.Fatalf("read epoch sidecar: %v", err)
	}
	if got, want := strings.TrimSpace(string(data)), "5"; got != want {
		t.Fatalf("epoch sidecar = %q, want %q (promotion must persist the leader epoch)", got, want)
	}
}

// TestBecomeLeader_ResetsStaleEpochHistory verifies that a stale epoch history
// from a previous topic generation (epochs higher than the current controller
// epoch — the delete/recreate signature) cannot wedge leader initialization:
// becomeLeader resets the history and promotes instead of failing, because the
// ISR store's epoch CAS is the real fencing mechanism.
func TestBecomeLeader_ResetsStaleEpochHistory(t *testing.T) {
	srv, pm := newTestServerForBecomeLeader(t)

	topic := "recreated"
	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	ctx := context.Background()
	if err := srv.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	// Seed a stale epoch history from the deleted topic's generation: a much
	// higher epoch with an advanced start offset.
	stale := &replication.EpochHistory{Entries: []replication.EpochEntry{
		{Epoch: 11, StartOffset: 1901930},
	}}
	if err := srv.isrStore.WriteEpochHistory(ctx, topic, 0, stale); err != nil {
		t.Fatalf("seed stale epoch history: %v", err)
	}

	req := pushAssignmentRequest{
		Topic:     topic,
		Partition: 0,
		Leader:    "node-A",
		Epoch:     1, // the recreated topic's fresh epoch
		Replicas:  []string{"node-A"},
		ISR:       []string{"node-A"},
	}
	if err := srv.becomeLeader(ctx, topic, 0, req); err != nil {
		t.Fatalf("becomeLeader with stale epoch history must not fail, got: %v", err)
	}

	ps := pm.GetPartitionState(topic, 0)
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if !ps.isLeader {
		t.Fatal("expected the partition to be leader")
	}

	// The reset history must be persisted to S3 so the next recovery starts clean.
	persisted, err := srv.isrStore.ReadEpochHistory(ctx, topic, 0)
	if err != nil {
		t.Fatalf("read persisted epoch history: %v", err)
	}
	if len(persisted.Entries) != 1 || persisted.Entries[0].Epoch != 1 {
		t.Fatalf("persisted epoch history = %+v, want a single fresh entry for epoch 1", persisted.Entries)
	}
}
