package server

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
)

func TestWALReplay_RecoversLogEnd(t *testing.T) {
	pm := newTestPartitionManager(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := pm.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	ps.isLeader = true

	msgs := []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
	}
	if err := ps.wal.AppendBatch(msgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}

	walEnd := ps.nextOffset
	if replayed, err := ps.wal.Replay(); err == nil && len(replayed) > 0 {
		lastOffset := replayed[len(replayed)-1].Offset
		if lastOffset+1 > walEnd {
			walEnd = lastOffset + 1
		}
	}

	if walEnd != 3 {
		t.Fatalf("expected WAL end=3, got %d", walEnd)
	}
}

func TestWALReplay_EmptyWAL(t *testing.T) {
	pm := newTestPartitionManager(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := pm.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	walEnd := ps.nextOffset
	if replayed, err := ps.wal.Replay(); err == nil && len(replayed) > 0 {
		lastOffset := replayed[len(replayed)-1].Offset
		if lastOffset+1 > walEnd {
			walEnd = lastOffset + 1
		}
	}

	if walEnd != 0 {
		t.Fatalf("expected WAL end=0 (empty), got %d", walEnd)
	}
}

func TestWALReplay_NextOffsetHigherThanWAL(t *testing.T) {
	pm := newTestPartitionManager(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := pm.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	msgs := []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
	}
	if err := ps.wal.AppendBatch(msgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}

	ps.nextOffset = 10

	walEnd := ps.nextOffset
	if replayed, err := ps.wal.Replay(); err == nil && len(replayed) > 0 {
		lastOffset := replayed[len(replayed)-1].Offset
		if lastOffset+1 > walEnd {
			walEnd = lastOffset + 1
		}
	}

	if walEnd != 10 {
		t.Fatalf("expected WAL end=10 (nextOffset=10 > WAL last+1=1, data likely flushed to S3), got %d", walEnd)
	}
}

func TestWALReplay_NextOffsetLowerThanWAL(t *testing.T) {
	pm := newTestPartitionManager(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := pm.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	msgs := []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
	}
	if err := ps.wal.AppendBatch(msgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}

	ps.nextOffset = 1

	walEnd := ps.nextOffset
	if replayed, err := ps.wal.Replay(); err == nil && len(replayed) > 0 {
		lastOffset := replayed[len(replayed)-1].Offset
		if lastOffset+1 > walEnd {
			walEnd = lastOffset + 1
		}
	}

	if walEnd != 3 {
		t.Fatalf("expected WAL end=3 (WAL has more msgs than nextOffset), got %d", walEnd)
	}
}

func TestReplicaState_NewReplicaStateWithHW(t *testing.T) {
	rs := replication.NewReplicaState("n1", 5, 1)
	if rs.HighWatermark() != 5 {
		t.Fatalf("expected HW=5, got %d", rs.HighWatermark())
	}
}

func TestReplicaState_HWAdvancesWithISRExpansion(t *testing.T) {
	rs := replication.NewReplicaState("n1", 0, 2)

	rs.SetLeaderOffset(10)

	if rs.HighWatermark() != 0 {
		t.Fatalf("expected HW=0 (ISR not ready, minISR=2), got %d", rs.HighWatermark())
	}

	rs.AddFollower("n2")
	rs.AddToISR("n2")
	rs.UpdateFollower("n2", 10)
	if rs.HighWatermark() != 10 {
		t.Fatalf("expected HW=10 (n2 added to ISR and caught up), got %d", rs.HighWatermark())
	}
}

func TestReplicaState_HWWithMultipleFollowers(t *testing.T) {
	rs := replication.NewReplicaState("n1", 0, 2)

	rs.SetLeaderOffset(10)
	rs.AddFollower("n2")
	rs.AddFollower("n3")
	rs.AddToISR("n2")
	rs.AddToISR("n3")

	rs.UpdateFollower("n2", 8)
	rs.UpdateFollower("n3", 10)

	if rs.HighWatermark() != 8 {
		t.Fatalf("expected HW=8 (min of ISR), got %d", rs.HighWatermark())
	}
}

func TestReplicaState_HWWithOneFollowerBehind(t *testing.T) {
	rs := replication.NewReplicaState("n1", 0, 2)

	rs.SetLeaderOffset(10)
	rs.AddFollower("n2")
	rs.AddFollower("n3")
	rs.AddToISR("n2")
	rs.AddToISR("n3")

	rs.UpdateFollower("n2", 10)
	rs.UpdateFollower("n3", 5)

	if rs.HighWatermark() != 5 {
		t.Fatalf("expected HW=5 (n3 behind), got %d", rs.HighWatermark())
	}

	rs.UpdateFollower("n3", 10)
	if rs.HighWatermark() != 10 {
		t.Fatalf("expected HW=10 (all caught up), got %d", rs.HighWatermark())
	}
}

func TestLeadershipChange_PreservesHW(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1"},
		Leader:      "n1",
		LeaderEpoch: 1,
	})

	ps.isLeader = true
	ps.epoch = 1

	msgs := []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
	}
	if err := ps.wal.AppendBatch(msgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}

	walEnd := ps.nextOffset
	if replayed, err := ps.wal.Replay(); err == nil && len(replayed) > 0 {
		lastOffset := replayed[len(replayed)-1].Offset
		if lastOffset+1 > walEnd {
			walEnd = lastOffset + 1
		}
	}

	if walEnd != 3 {
		t.Fatalf("expected WAL end=3, got %d", walEnd)
	}

	rs := replication.NewReplicaState("n1", walEnd, 1)
	if rs.HighWatermark() != 3 {
		t.Fatalf("expected new replicaState HW=3, got %d", rs.HighWatermark())
	}
}

func TestLeadershipChange_RF3PreservesHW(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	msgs := []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 3, Key: []byte("k3"), Value: []byte("v3")},
		{Offset: 4, Key: []byte("k4"), Value: []byte("v4")},
	}
	if err := ps.wal.AppendBatch(msgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}

	// Reproduce assignment-driven failover with stale persisted metadata.
	ps.nextOffset = 0
	ps.index.SetHighWatermark(0)
	if err := s.isrStore.Write(context.Background(), "topic", replication.ISRState{
		Partition:     0,
		ISR:           []string{"n1", "n2"},
		Leader:        "n2",
		LeaderEpoch:   1,
		HighWatermark: 0,
	}, ""); err != nil {
		t.Fatalf("isrStore.Write() error = %v", err)
	}

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 2,
	})

	if got := ps.nextOffset; got != 5 {
		t.Fatalf("nextOffset = %d, want 5 after WAL replay", got)
	}
	if ps.replicaState == nil {
		t.Fatal("expected replicaState after leader init")
	}
	if got := ps.replicaState.HighWatermark(); got != 5 {
		t.Fatalf("replicaState.HighWatermark() = %d, want 5", got)
	}

	isrState, err := s.isrStore.Read(context.Background(), "topic", 0)
	if err != nil {
		t.Fatalf("isrStore.Read() error = %v", err)
	}
	if got := isrState.HighWatermark; got != 5 {
		t.Fatalf("persisted ISR HighWatermark = %d, want 5", got)
	}
}

func TestInitPartitionAsLeader_FlushesRecoveredWALForReads(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}
	if err := s.assignmentStore.Write(context.Background(), "topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {
				Replicas:    []string{"n1", "n2", "n3"},
				Leader:      "n1",
				LeaderEpoch: 2,
			},
		},
		Version: 2,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}
	if err := ps.wal.AppendBatch([]log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
	}); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}
	ps.nextOffset = 0

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 2,
	})

	if got := ps.index.NextOffset(); got != 3 {
		t.Fatalf("index.NextOffset() = %d, want 3", got)
	}
	if got := ps.index.HighWatermark(); got != 3 {
		t.Fatalf("index.HighWatermark() = %d, want 3", got)
	}
}
