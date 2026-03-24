package server

import (
	"context"
	"errors"
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
	ps.replicaState = replication.NewReplicaState("n1", 0, 1)

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

func TestPartitionManagerOnFlush_IndexCASExhaustionKeepsWAL(t *testing.T) {
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
	ps.isLeader = true
	ps.replicaState = replication.NewReplicaState("n1", 0, 1)

	_, err := pm.AppendBatch(context.Background(), "topic", 0, []log.Message{
		{Key: []byte("k"), Value: []byte("value")},
	})
	if err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}

	var casCalls int
	pm.conditionalPutIndex = func(ctx context.Context, key string, data []byte, etag string) (string, error) {
		casCalls++
		return "", storage.ErrConflict
	}

	err = pm.onFlush(pm.getGlobalID("topic", 0))
	if err == nil {
		t.Fatal("onFlush() should fail when index CAS exhausts retries")
	}
	if casCalls != 5 {
		t.Fatalf("conditionalPutIndex called %d times, want 5", casCalls)
	}
	if !errors.Is(err, storage.ErrConflict) && err.Error() != "flush index exhausted after retries" {
		t.Fatalf("onFlush() error = %v, want index exhaustion", err)
	}

	msgs, err := ps.wal.Replay()
	if err != nil {
		t.Fatalf("wal.Replay() error = %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected WAL to retain 1 message after failed flush, found %d", len(msgs))
	}
	if ps.flushedOffset != 0 {
		t.Fatalf("flushedOffset = %d, want 0 after failed flush", ps.flushedOffset)
	}

	indexKey := "topic/0/index.json"
	if _, err := pm.s3Client.Get(context.Background(), indexKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("index.json should not be written on failed flush, got err=%v", err)
	}

	segmentKeys, err := pm.s3Client.List(context.Background(), "topic/0/")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(segmentKeys) != 1 {
		t.Fatalf("expected uploaded segment to remain for diagnosis/retry, got %v", segmentKeys)
	}
}
