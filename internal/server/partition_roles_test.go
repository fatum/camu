package server

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

func newTestServerForCanBecomeLeader(t *testing.T) *Server {
	t.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}
	cfg := &config.Config{}
	srv := &Server{
		cfg:          cfg,
		s3Client:     s3Client,
		topicStore:   meta.NewTopicStore(s3Client),
		isrStore:     replication.NewISRStore(s3Client),
		instanceID:   "node-B",
		myPartitions: make(map[string]map[int]localPartitionAssignment),
	}
	return srv
}

func TestCanBecomeLeader(t *testing.T) {
	ctx := context.Background()

	t.Run("rf1 always allowed", func(t *testing.T) {
		s := newTestServerForCanBecomeLeader(t)
		tc := meta.TopicConfig{Name: "t", Partitions: 1, ReplicationFactor: 1}
		if err := s.topicStore.Create(ctx, tc); err != nil {
			t.Fatal(err)
		}
		if !s.canBecomeLeader(ctx, "t", 0, 0) {
			t.Fatal("rf=1 must always be allowed")
		}
	})

	t.Run("unclean election allowed regardless of durability", func(t *testing.T) {
		s := newTestServerForCanBecomeLeader(t)
		tc := meta.TopicConfig{Name: "t", Partitions: 1, ReplicationFactor: 3, MinInsyncReplicas: 2, UncleanLeaderElection: true}
		if err := s.topicStore.Create(ctx, tc); err != nil {
			t.Fatal(err)
		}
		// ISR exists with a high committed HW; durable log end is far below.
		if err := s.isrStore.Update(ctx, "t", 0, 1, func(_ replication.ISRState) (replication.ISRState, error) {
			return replication.ISRState{ISR: []string{"node-A"}, Leader: "node-A", HighWatermark: 1000}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if !s.canBecomeLeader(ctx, "t", 0, 1) {
			t.Fatal("unclean election must bypass the durability gate")
		}
	})

	t.Run("bootstrap with no ISR allowed", func(t *testing.T) {
		s := newTestServerForCanBecomeLeader(t)
		tc := meta.TopicConfig{Name: "t", Partitions: 1, ReplicationFactor: 3, MinInsyncReplicas: 2}
		if err := s.topicStore.Create(ctx, tc); err != nil {
			t.Fatal(err)
		}
		if !s.canBecomeLeader(ctx, "t", 0, 0) {
			t.Fatal("bootstrap (no ISR yet) must be allowed")
		}
	})

	t.Run("durable log end at or above committed watermark allowed", func(t *testing.T) {
		s := newTestServerForCanBecomeLeader(t)
		tc := meta.TopicConfig{Name: "t", Partitions: 1, ReplicationFactor: 3, MinInsyncReplicas: 2}
		if err := s.topicStore.Create(ctx, tc); err != nil {
			t.Fatal(err)
		}
		if err := s.isrStore.Update(ctx, "t", 0, 1, func(_ replication.ISRState) (replication.ISRState, error) {
			return replication.ISRState{ISR: []string{"node-A"}, Leader: "node-A", HighWatermark: 100}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if !s.canBecomeLeader(ctx, "t", 0, 100) {
			t.Fatal("log end == committed HW must be allowed")
		}
		if !s.canBecomeLeader(ctx, "t", 0, 200) {
			t.Fatal("log end > committed HW must be allowed")
		}
	})

	t.Run("durable log end below committed watermark refused", func(t *testing.T) {
		s := newTestServerForCanBecomeLeader(t)
		tc := meta.TopicConfig{Name: "t", Partitions: 1, ReplicationFactor: 3, MinInsyncReplicas: 2}
		if err := s.topicStore.Create(ctx, tc); err != nil {
			t.Fatal(err)
		}
		if err := s.isrStore.Update(ctx, "t", 0, 1, func(_ replication.ISRState) (replication.ISRState, error) {
			return replication.ISRState{ISR: []string{"node-A"}, Leader: "node-A", HighWatermark: 1000}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if s.canBecomeLeader(ctx, "t", 0, 100) {
			t.Fatal("log end below committed HW must refuse promotion (would truncate committed data)")
		}
	})

	t.Run("ISR read error fails closed", func(t *testing.T) {
		s := newTestServerForCanBecomeLeader(t)
		tc := meta.TopicConfig{Name: "t", Partitions: 1, ReplicationFactor: 3, MinInsyncReplicas: 2}
		if err := s.topicStore.Create(ctx, tc); err != nil {
			t.Fatal(err)
		}
		s.s3Client.SetFaultInjector(func(op string) error {
			return context.Canceled
		})
		if s.canBecomeLeader(ctx, "t", 0, 1000) {
			t.Fatal("ISR read error must fail closed (refuse promotion)")
		}
	})

	t.Run("retention_aged committed watermark zeroed by cleanup allows promotion", func(t *testing.T) {
		// A partition whose committed HW has been fully retained (ISR store HW
		// at 0) must allow the first follower to promote even with a short log.
		s := newTestServerForCanBecomeLeader(t)
		tc := meta.TopicConfig{Name: "t", Partitions: 1, ReplicationFactor: 3, MinInsyncReplicas: 2, Retention: time.Hour}
		if err := s.topicStore.Create(ctx, tc); err != nil {
			t.Fatal(err)
		}
		if err := s.isrStore.Update(ctx, "t", 0, 1, func(_ replication.ISRState) (replication.ISRState, error) {
			return replication.ISRState{ISR: []string{"node-A"}, Leader: "node-A", HighWatermark: 0}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if !s.canBecomeLeader(ctx, "t", 0, 0) {
			t.Fatal("log end 0 with committed HW 0 must be allowed")
		}
	})
}
