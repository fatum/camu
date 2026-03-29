package server

import (
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

func newTestPartitionManager(t *testing.T) *PartitionManager {
	t.Helper()
	return newTestPartitionManagerWithSegmentMaxSize(t, 1)
}

func newTestPartitionManagerWithSegmentMaxSize(t *testing.T, maxSize int64) *PartitionManager {
	t.Helper()

	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.WAL.Directory = filepath.Join(t.TempDir(), "wal")
	cfg.WAL.Fsync = false
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = maxSize
	cfg.Segments.MaxAge = "1h"

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager() error = %v", err)
	}
	return pm
}

func TestNewPartitionManager_UsesConfiguredWALFsync(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.WAL.Directory = filepath.Join(t.TempDir(), "wal")
	cfg.WAL.Fsync = true
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = 1 << 20
	cfg.Segments.MaxAge = "1h"

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager() error = %v", err)
	}
	if !pm.walFsync {
		t.Fatal("expected PartitionManager to propagate WAL.Fsync=true")
	}
}

func TestPartitionManagerAppendBatch_ConcurrentWritesPreserveWALOrder(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)

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

	const goroutines = 32
	if current := runtime.GOMAXPROCS(0); current < 4 {
		runtime.GOMAXPROCS(4)
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := pm.AppendBatch(context.Background(), "topic", 0, []log.Message{
				{Key: []byte("k"), Value: []byte{byte(i)}},
			})
			if err != nil {
				t.Errorf("AppendBatch() error = %v", err)
			}
		}(i)
	}
	wg.Wait()

	msgs, err := ps.wal.Replay()
	if err != nil {
		t.Fatalf("wal.Replay() error = %v", err)
	}
	if len(msgs) != goroutines {
		t.Fatalf("wal.Replay() returned %d messages, want %d", len(msgs), goroutines)
	}
	for i, msg := range msgs {
		if msg.Offset != uint64(i) {
			t.Fatalf("wal message %d has offset %d, want %d", i, msg.Offset, i)
		}
	}
}

func TestPartitionManagerAppendBatch_PersistsHighWatermarkBeforeFlush(t *testing.T) {
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
	ps.replicaState = replication.NewReplicaState("n1", 0, 1, 1000)

	_, err := pm.AppendBatch(context.Background(), "topic", 0, []log.Message{
		{Key: []byte("k"), Value: []byte("value")},
	})
	if err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}

	if got := ps.replicaState.HighWatermark(); got != 1 {
		t.Fatalf("replicaState.HighWatermark() = %d, want 1", got)
	}
	if got := ps.index.HighWatermark(); got != 1 {
		t.Fatalf("index.HighWatermark() = %d, want 1", got)
	}

	msgs, err := ps.wal.Replay()
	if err != nil {
		t.Fatalf("wal.Replay() error = %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected WAL to be truncated after flush, found %d messages", len(msgs))
	}
}

// TestPartitionManagerOnFlush_IndexCASExhaustionKeepsWAL was removed:
// index.json CAS loop has been replaced by a simple state.json PUT.

func TestPartitionManagerScanAndRebuildProducerState(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)

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

	if err := ps.wal.AppendBatchWithMeta(log.Batch{
		ProducerID: 1,
		Sequence:   0,
		Messages: []log.Message{
			{Offset: 0, Key: []byte("k0"), Value: []byte("v0"), Headers: map[string]string{"a": "1"}},
			{Offset: 1, Key: []byte("k1"), Value: []byte("v1"), Headers: map[string]string{"b": "2"}},
		},
	}); err != nil {
		t.Fatalf("AppendBatchWithMeta(first) error = %v", err)
	}
	if err := ps.wal.AppendBatchWithMeta(log.Batch{
		ProducerID: 2,
		Sequence:   5,
		Messages: []log.Message{
			{Offset: 2, Key: []byte("k2"), Value: []byte("v2"), Headers: map[string]string{"c": "3"}},
		},
	}); err != nil {
		t.Fatalf("AppendBatchWithMeta(second) error = %v", err)
	}

	// Only batches at or above flushedOffset are scanned.
	ps.flushedOffset = 2

	n := pm.ScanAndRebuildProducerState("topic", 0)
	if n != 1 {
		t.Fatalf("ScanAndRebuildProducerState() rebuilt %d batches, want 1", n)
	}

	// Verify producer 2's sequence state was rebuilt.
	state, ok := ps.producerSeqs[2]
	if !ok {
		t.Fatal("expected producerSeqs entry for producer 2")
	}
	if state.NextSeq != 6 { // sequence 5 + batch size 1
		t.Fatalf("producer 2 NextSeq = %d, want 6", state.NextSeq)
	}

	// Producer 1 should NOT be present (its batch was below flushedOffset).
	if _, ok := ps.producerSeqs[1]; ok {
		t.Fatal("producer 1 should not be in producerSeqs (below flushedOffset)")
	}
}
