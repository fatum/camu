package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/idempotency"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/producer"
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
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = maxSize
	cfg.Segments.MaxAge = "1h"

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager() error = %v", err)
	}
	// Tests use tiny MaxSize to force fast flushes, which yields a negligible
	// per-partition high-water mark (maxSize*8) that would spuriously reject
	// normal batches. Disable backpressure here; it is exercised directly
	// against the batcher in its own package.
	maxAge, err := cfg.Segments.MaxAgeDuration()
	if err != nil {
		t.Fatalf("MaxAgeDuration() error = %v", err)
	}
	pm.batcher = producer.NewBatcher(producer.BatcherConfig{
		MaxSize: maxSize,
		MaxAge:  maxAge,
		OnFlush: pm.onFlushDispatch,
	})
	return pm
}

func TestNewPartitionManager_UsesCacheBackedLocalDir(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = 1 << 20
	cfg.Segments.MaxAge = "1h"

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager() error = %v", err)
	}
	if pm.localDir == "" {
		t.Fatal("expected PartitionManager localDir to be initialized")
	}
}

func TestPartitionManagerAppendBatch_ConcurrentWritesPreserveOffsetOrder(t *testing.T) {
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

	ps.mu.RLock()
	nextOffset := ps.nextOffset
	indexHW := ps.index.HighWatermark()
	ps.mu.RUnlock()
	if nextOffset != goroutines {
		t.Fatalf("nextOffset = %d, want %d", nextOffset, goroutines)
	}
	if indexHW != goroutines {
		t.Fatalf("index.HighWatermark() = %d, want %d", indexHW, goroutines)
	}
}

func TestRecoverLocalLogEnd_PrefersNativeData(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	now := time.Now().UnixMilli()
	nativeBatch := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k0"), Value: []byte("v0"), Timestamp: now},
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now + 1},
	})
	if err := as.Append(nativeBatch); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.nextOffset = 0
	ps.mu.Unlock()

	if got := pm.recoverLocalLogEnd("topic", 0); got != 2 {
		t.Fatalf("recoverLocalLogEnd() = %d, want 2 from native data", got)
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

	raw, _, err := pm.ReadRawBatches(context.Background(), "topic", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches() error = %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("expected native storage to expose appended batch")
	}
}

// TestPartitionManagerOnFlush_IndexCASExhaustionKeepsWAL was removed:
// index.json CAS loop has been replaced by a simple state.json PUT.

func TestPartitionManagerScanAndRebuildProducerStateFromActiveSegment(t *testing.T) {
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

	as, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "topic-0-active"), 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.activeSegment = as

	first := log.EncodeRecordBatchWithMeta(0, log.Batch{
		ProducerID: 1,
		Sequence:   0,
		Messages: []log.Message{
			{Offset: 0, Key: []byte("k0"), Value: []byte("v0"), Headers: map[string]string{"a": "1"}},
			{Offset: 1, Key: []byte("k1"), Value: []byte("v1"), Headers: map[string]string{"b": "2"}},
		},
	})
	if err := as.Append(first); err != nil {
		t.Fatalf("activeSegment.Append(first) error = %v", err)
	}
	second := log.EncodeRecordBatchWithMeta(2, log.Batch{
		ProducerID: 2,
		Sequence:   5,
		Messages: []log.Message{
			{Offset: 2, Key: []byte("k2"), Value: []byte("v2"), Headers: map[string]string{"c": "3"}},
		},
	})
	if err := as.Append(second); err != nil {
		t.Fatalf("activeSegment.Append(second) error = %v", err)
	}

	ps.flushedOffset = 1
	ps.nextOffset = 3

	n := pm.ScanAndRebuildProducerStateFromActiveSegment("topic", 0)
	if n != 1 {
		t.Fatalf("ScanAndRebuildProducerStateFromActiveSegment() rebuilt %d batches, want 1", n)
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

func TestPartitionStateLoadProducerCheckpoint_DoesNotImmediateExpire(t *testing.T) {
	ps := &partitionState{
		producerSeqs: make(map[uint64]*producerPartitionState),
	}

	var buf []byte
	line, err := json.Marshal(producerCheckpointEntry{
		ProducerID: 7,
		NextSeq:    11,
		LastOffset: 10,
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	buf = append(buf, line...)
	buf = append(buf, '\n')

	ps.loadProducerCheckpoint(buf)

	if got := ps.evictStaleProducers(time.Hour); got != 0 {
		t.Fatalf("evictStaleProducers() = %d, want 0 immediately after load", got)
	}
	state, ok := ps.producerSeqs[7]
	if !ok {
		t.Fatal("expected restored producer state")
	}
	if state.LastActiveAt.IsZero() {
		t.Fatal("expected LastActiveAt to be populated on checkpoint load")
	}
}

func TestPartitionStateRebuildProducerSeqsFromBatches_DoesNotImmediateExpire(t *testing.T) {
	ps := &partitionState{
		producerSeqs: make(map[uint64]*producerPartitionState),
	}

	ps.rebuildProducerSeqsFromBatches([]log.BatchMeta{
		{ProducerID: 5, Sequence: 3, MessageCount: 2},
	})

	if got := ps.evictStaleProducers(time.Hour); got != 0 {
		t.Fatalf("evictStaleProducers() = %d, want 0 immediately after rebuild", got)
	}
	state, ok := ps.producerSeqs[5]
	if !ok {
		t.Fatal("expected rebuilt producer state")
	}
	if got := state.NextSeq; got != 5 {
		t.Fatalf("state.NextSeq = %d, want 5", got)
	}
	if state.LastActiveAt.IsZero() {
		t.Fatal("expected LastActiveAt to be populated on producer-state rebuild")
	}
}

func TestAppendRawBatch_BasicOffsetAssignment(t *testing.T) {
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

	// Set up activeSegment and mark as leader.
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.mu.Unlock()

	// Build a raw RecordBatch with 3 messages.
	now := time.Now().UnixMilli()
	msgs := []log.Message{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now},
		{Key: []byte("k2"), Value: []byte("v2"), Timestamp: now + 1},
		{Key: []byte("k3"), Value: []byte("v3"), Timestamp: now + 2},
	}
	rawBatch := log.EncodeRecordBatch(0, msgs)

	// Append first batch.
	baseOffset, err := pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch)
	if err != nil {
		t.Fatalf("AppendRawBatch() error = %v", err)
	}
	if baseOffset != 0 {
		t.Fatalf("expected baseOffset=0, got %d", baseOffset)
	}

	// Build and append second batch.
	msgs2 := []log.Message{
		{Key: []byte("k4"), Value: []byte("v4"), Timestamp: now + 3},
		{Key: []byte("k5"), Value: []byte("v5"), Timestamp: now + 4},
	}
	rawBatch2 := log.EncodeRecordBatch(0, msgs2)

	baseOffset2, err := pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch2)
	if err != nil {
		t.Fatalf("AppendRawBatch() second batch error = %v", err)
	}
	if baseOffset2 != 3 {
		t.Fatalf("expected baseOffset=3, got %d", baseOffset2)
	}

	// Verify nextOffset advanced correctly.
	ps.mu.RLock()
	nextOff := ps.nextOffset
	ps.mu.RUnlock()
	if nextOff != 5 {
		t.Fatalf("expected nextOffset=5, got %d", nextOff)
	}

	// Verify the active segment has the data by reading back the header.
	entries := as.OffsetIndex()
	if len(entries) != 2 {
		t.Fatalf("expected 2 index entries, got %d", len(entries))
	}
	if entries[0].BaseOffset != 0 {
		t.Fatalf("first entry baseOffset: got %d, want 0", entries[0].BaseOffset)
	}
	if entries[1].BaseOffset != 3 {
		t.Fatalf("second entry baseOffset: got %d, want 3", entries[1].BaseOffset)
	}
}

func TestAppendRawBatch_NotLeaderReturnsError(t *testing.T) {
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

	// Set up activeSegment but do NOT mark as leader.
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = false
	ps.mu.Unlock()

	rawBatch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})
	_, err = pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch)
	if err == nil {
		t.Fatal("expected error for non-leader append")
	}
}

func TestAppendRawBatch_NilActiveSegmentReturnsError(t *testing.T) {
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
	ps.mu.Lock()
	ps.isLeader = true
	// activeSegment is nil
	ps.mu.Unlock()

	rawBatch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})
	_, err := pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch)
	if err == nil {
		t.Fatal("expected error for nil active segment")
	}
}

func TestAppendRawBatch_DuplicateSequenceReturnsPriorOffset(t *testing.T) {
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
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.mu.Unlock()

	now := time.Now().UnixMilli()
	msgs := []log.Message{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now},
		{Key: []byte("k2"), Value: []byte("v2"), Timestamp: now + 1},
	}
	// Idempotent batch: producer 42, sequence 0.
	rawBatch := log.EncodeRecordBatchWithMeta(0, log.Batch{
		ProducerID: 42,
		Sequence:   0,
		Messages:   msgs,
	})

	baseOffset, err := pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch)
	if err != nil {
		t.Fatalf("AppendRawBatch() first error = %v", err)
	}
	if baseOffset != 0 {
		t.Fatalf("first baseOffset = %d, want 0", baseOffset)
	}

	// Retry the identical batch with the same producer/sequence.
	baseOffset2, err := pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch)
	if err != nil {
		t.Fatalf("AppendRawBatch() duplicate error = %v, want success with prior offset", err)
	}
	if baseOffset2 != 0 {
		t.Fatalf("duplicate baseOffset = %d, want prior offset 0", baseOffset2)
	}

	// The duplicate must not have advanced the log.
	ps.mu.RLock()
	nextOff := ps.nextOffset
	ps.mu.RUnlock()
	if nextOff != 2 {
		t.Fatalf("nextOffset after duplicate = %d, want 2", nextOff)
	}
}

func TestDuplicateBaseOffset_EvictedProducerDoesNotPanic(t *testing.T) {
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

	// Seed the producer's batch metadata.
	ps.mu.Lock()
	ps.checkAndAdvanceSeq(7, 0, 2)
	ps.recordAppendedBatch(7, 0, 2, 0, 1)
	ps.mu.Unlock()

	// Simulate eviction concurrent with a duplicate retry: the map entry is
	// removed before duplicateBaseOffset looks it up. This must not panic and
	// must report "not a known duplicate" (false) instead of confirming.
	ps.mu.Lock()
	delete(ps.producerSeqs, 7)
	ps.mu.Unlock()

	// A retried duplicate arriving after eviction must be handled gracefully.
	prior, ok := pm.duplicateBaseOffset(ps, 7, 0, 2)
	if ok {
		t.Fatalf("duplicateBaseOffset ok = true, want false after eviction (prior=%d)", prior)
	}
}

func TestDuplicateBaseOffset_ConcurrentAppendNoDataRace(t *testing.T) {
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

	// Seed the producer's sequence state.
	ps.mu.Lock()
	ps.checkAndAdvanceSeq(99, 0, 2)
	ps.recordAppendedBatch(99, 0, 2, 0, 1)
	ps.mu.Unlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer: replace LastBatch under the partition lock, simulating appends.
	wg.Add(1)
	go func() {
		defer wg.Done()
		seq := uint64(2)
		for {
			select {
			case <-stop:
				return
			default:
			}
			ps.mu.Lock()
			ps.recordAppendedBatch(99, seq, 2, int64(seq), uint64(seq)+1)
			ps.mu.Unlock()
			seq += 2
			runtime.Gosched()
		}
	}()

	// Reader: concurrent duplicate resolution must read LastBatch safely.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			pm.duplicateBaseOffset(ps, 99, 0, 2)
			runtime.Gosched()
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestAppendRawBatch_DuplicateOfEarlierSequenceRangeIsRejected(t *testing.T) {
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
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.mu.Unlock()

	now := time.Now().UnixMilli()
	// Batch A: producer 7, sequence 0, 2 records -> offsets 0,1 (NextSeq=2).
	batchA := log.EncodeRecordBatchWithMeta(0, log.Batch{
		ProducerID: 7,
		Sequence:   0,
		Messages: []log.Message{
			{Key: []byte("a1"), Value: []byte("v1"), Timestamp: now},
			{Key: []byte("a2"), Value: []byte("v2"), Timestamp: now + 1},
		},
	})
	if _, err := pm.AppendRawBatch(context.Background(), "topic", 0, batchA); err != nil {
		t.Fatalf("AppendRawBatch(A) error = %v", err)
	}

	// Batch B: producer 7, sequence 2, 1 record -> offset 2 (NextSeq=3).
	batchB := log.EncodeRecordBatchWithMeta(2, log.Batch{
		ProducerID: 7,
		Sequence:   2,
		Messages: []log.Message{
			{Key: []byte("b1"), Value: []byte("v3"), Timestamp: now + 2},
		},
	})
	if _, err := pm.AppendRawBatch(context.Background(), "topic", 0, batchB); err != nil {
		t.Fatalf("AppendRawBatch(B) error = %v", err)
	}

	// Exact retry of batch A (sequence 0, 2 records) is NOT an exact match of
	// the most recent batch (batch B: sequence 2, 1 record). It must not be
	// acknowledged with batch B's offset; it must surface the duplicate error
	// rather than silently confirming an overlapping range.
	_, err = pm.AppendRawBatch(context.Background(), "topic", 0, batchA)
	if err == nil {
		t.Fatal("expected error for overlapping non-identical duplicate, got success")
	}
	if !errors.Is(err, idempotency.ErrDuplicateSequence) {
		t.Fatalf("AppendRawBatch() error = %v, want ErrDuplicateSequence", err)
	}

	// The log must not have advanced: still offsets 0,1,2.
	ps.mu.RLock()
	nextOff := ps.nextOffset
	ps.mu.RUnlock()
	if nextOff != 3 {
		t.Fatalf("nextOffset after rejected overlap = %d, want 3", nextOff)
	}

	// An exact retry of batch B (the most recent batch) must confirm with B's
	// own base offset (2), not be rejected.
	prior, err := pm.AppendRawBatch(context.Background(), "topic", 0, batchB)
	if err != nil {
		t.Fatalf("AppendRawBatch(B retry) error = %v, want success", err)
	}
	if prior != 2 {
		t.Fatalf("B retry baseOffset = %d, want 2", prior)
	}
}

func TestOnFlushActiveSegment_OpenReplacementFailureKeepsOldSegmentUsable(t *testing.T) {
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
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	if err := as.Append(log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.nextOffset = 1
	ps.mu.Unlock()

	// Block opening the replacement segment by putting a directory at its path.
	replacementPath := filepath.Join(segDir, log.SegmentFilename(1))
	if err := os.Mkdir(replacementPath, 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	err = pm.onFlushActiveSegment("topic", 0)
	if err == nil {
		t.Fatal("expected open new active segment error")
	}
	if !strings.Contains(err.Error(), "open new active segment") {
		t.Fatalf("err = %v, want open-new-segment error", err)
	}

	// The old segment must still be the active one and still writable.
	ps.mu.RLock()
	got := ps.activeSegment
	ps.mu.RUnlock()
	if got != as {
		t.Fatal("ps.activeSegment changed after failed flush; want the old segment")
	}
	if err := as.Append(log.EncodeRecordBatch(1, []log.Message{{Key: []byte("k2"), Value: []byte("v2"), Timestamp: 2}})); err != nil {
		t.Fatalf("old segment no longer writable after failed flush: %v", err)
	}
}

func TestOnFlushActiveSegment_SealFailureCleansUpReplacementSegment(t *testing.T) {
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
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	if err := as.Append(log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.nextOffset = 1
	ps.mu.Unlock()

	// Make Seal fail at sidecar creation (after sync+close succeed) by putting
	// a directory at the sidecar path. The replacement segment opens BEFORE
	// Seal at a different path, so it succeeds; only the sidecar create fails.
	if err := os.Mkdir(filepath.Join(segDir, log.SidecarFilename(0)), 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	err = pm.onFlushActiveSegment("topic", 0)
	if err == nil {
		t.Fatal("expected seal failure error")
	}
	if !strings.Contains(err.Error(), "seal active segment") {
		t.Fatalf("err = %v, want seal failure", err)
	}

	// The replacement segment's orphan .log file must have been removed: the
	// only .log file in the dir is the original at base offset 0.
	if _, err := os.Stat(filepath.Join(segDir, log.SegmentFilename(0))); err != nil {
		t.Fatalf("original segment file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(segDir, log.SegmentFilename(1))); err == nil {
		t.Fatal("replacement segment file leaked after failed seal; want it removed")
	}

	// The old segment must have been reopened (self-heal) and remain writable.
	ps.mu.RLock()
	got := ps.activeSegment
	ps.mu.RUnlock()
	if got == nil {
		t.Fatal("ps.activeSegment is nil after failed seal")
	}
	if err := got.Append(log.EncodeRecordBatch(1, []log.Message{{Key: []byte("k2"), Value: []byte("v2"), Timestamp: 2}})); err != nil {
		t.Fatalf("reopened segment not writable after failed seal: %v", err)
	}
}

func TestAppendRawBatch_ClosedActiveSegmentReturnsSegmentNotReady(t *testing.T) {
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
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	if err := as.Append(log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
	}
	// Seal closes the file, simulating a retired-but-still-active segment.
	if _, _, err := as.Seal(); err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.nextOffset = 1
	ps.mu.Unlock()

	rawBatch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k2"), Value: []byte("v2"), Timestamp: 2}})
	_, err = pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch)
	if err == nil {
		t.Fatal("expected error for closed active segment")
	}
	if !errors.Is(err, errKafkaSegmentNotReady) {
		t.Fatalf("AppendRawBatch() error = %v, want errKafkaSegmentNotReady", err)
	}
}

func TestReadRawBatches_ActiveSegment(t *testing.T) {
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

	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.mu.Unlock()

	// Append two batches.
	now := time.Now().UnixMilli()
	msgs1 := []log.Message{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now},
		{Key: []byte("k2"), Value: []byte("v2"), Timestamp: now + 1},
	}
	rawBatch1 := log.EncodeRecordBatch(0, msgs1)
	if _, err := pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch1); err != nil {
		t.Fatalf("AppendRawBatch(1) error = %v", err)
	}

	msgs2 := []log.Message{
		{Key: []byte("k3"), Value: []byte("v3"), Timestamp: now + 2},
	}
	rawBatch2 := log.EncodeRecordBatch(0, msgs2)
	if _, err := pm.AppendRawBatch(context.Background(), "topic", 0, rawBatch2); err != nil {
		t.Fatalf("AppendRawBatch(2) error = %v", err)
	}

	// Read all from offset 0.
	data, hw, err := pm.ReadRawBatches(context.Background(), "topic", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches() error = %v", err)
	}
	if hw != 3 {
		t.Fatalf("expected hw=3, got %d", hw)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}

	// Decode the returned bytes — should contain both batches.
	decoded, err := log.DecodeRecordBatch(data[:len(rawBatch1)])
	if err != nil {
		t.Fatalf("DecodeRecordBatch(batch1) error = %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 messages in first batch, got %d", len(decoded))
	}
	if string(decoded[0].Key) != "k1" {
		t.Fatalf("expected key=k1, got %s", decoded[0].Key)
	}

	// Read from offset 2 — should get only the second batch.
	data2, hw2, err := pm.ReadRawBatches(context.Background(), "topic", 0, 2, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches(offset=2) error = %v", err)
	}
	if hw2 != 3 {
		t.Fatalf("expected hw=3, got %d", hw2)
	}
	if len(data2) == 0 {
		t.Fatal("expected non-empty data for offset=2")
	}

	decoded2, err := log.DecodeRecordBatch(data2)
	if err != nil {
		t.Fatalf("DecodeRecordBatch(batch2) error = %v", err)
	}
	if len(decoded2) != 1 {
		t.Fatalf("expected 1 message, got %d", len(decoded2))
	}
	if string(decoded2[0].Key) != "k3" {
		t.Fatalf("expected key=k3, got %s", decoded2[0].Key)
	}

	// Read beyond HW should return empty.
	data3, hw3, err := pm.ReadRawBatches(context.Background(), "topic", 0, 3, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches(offset=3) error = %v", err)
	}
	if hw3 != 3 {
		t.Fatalf("expected hw=3, got %d", hw3)
	}
	if len(data3) != 0 {
		t.Fatalf("expected empty data for offset >= hw, got %d bytes", len(data3))
	}
}

func TestReadReplicaBatchRange_ReadsPastHighWatermark(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}

	now := time.Now().UnixMilli()
	batch0 := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k0"), Value: []byte("v0"), Timestamp: now}})
	batch1 := log.EncodeRecordBatch(1, []log.Message{{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now + 1}})
	if err := as.Append(batch0); err != nil {
		t.Fatalf("Append(batch0) error = %v", err)
	}
	if err := as.Append(batch1); err != nil {
		t.Fatalf("Append(batch1) error = %v", err)
	}

	ps.mu.Lock()
	ps.activeSegment = as
	ps.nextOffset = 2
	ps.replicaState = replication.NewReplicaState("leader", 1, 1, 1000)
	ps.mu.Unlock()

	br, err := pm.ReadReplicaBatchRange("topic", 0, 1, 1<<20)
	if err != nil {
		t.Fatalf("ReadReplicaBatchRange() error = %v", err)
	}
	if br.UpperBound != 2 {
		t.Fatalf("ReadReplicaBatchRange() upper bound = %d, want 2", br.UpperBound)
	}
	if br.Length == 0 {
		t.Fatal("ReadReplicaBatchRange() returned no data, want uncommitted tail batch")
	}
	replicaData := make([]byte, br.Length)
	n, err := br.File.ReadAt(replicaData, br.FileOffset)
	if err != nil && n < int(br.Length) {
		t.Fatalf("ReadAt() error = %v, n = %d", err, n)
	}
	replicaData = replicaData[:n]
	decoded, err := log.DecodeRecordBatch(replicaData)
	if err != nil {
		t.Fatalf("DecodeRecordBatch() error = %v", err)
	}
	if len(decoded) != 1 || string(decoded[0].Key) != "k1" {
		t.Fatalf("decoded replica batch = %+v, want one k1 record", decoded)
	}
}

func TestReadReplicaBatchRangeDoesNotServeSealedPrefix(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 10)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	batch := log.EncodeRecordBatch(10, []log.Message{{Key: []byte("tail"), Value: []byte("v")}})
	if err := as.Append(batch); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.nextOffset = 11
	ps.mu.Unlock()

	br, err := pm.ReadReplicaBatchRange("topic", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadReplicaBatchRange() error = %v", err)
	}
	if br.UpperBound != 11 {
		t.Fatalf("upper bound = %d, want 11", br.UpperBound)
	}
	if br.Length != 0 {
		t.Fatalf("sealed-prefix read returned %d bytes, want none", br.Length)
	}

	br, err = pm.ReadReplicaBatchRange("topic", 0, 10, 1<<20)
	if err != nil {
		t.Fatalf("ReadReplicaBatchRange(active tail) error = %v", err)
	}
	if br.Length == 0 {
		t.Fatal("expected active tail data, got none")
	}
	data := make([]byte, br.Length)
	n, err := br.File.ReadAt(data, br.FileOffset)
	if err != nil && n < int(br.Length) {
		t.Fatalf("ReadAt() error = %v, n = %d", err, n)
	}
	data = data[:n]
	if !bytes.Equal(data, batch) {
		t.Fatal("active tail bytes differ from appended batch")
	}
}

func TestReadRawBatchesDoesNotJumpFromSealedPrefixToActiveTail(t *testing.T) {
	ctx := context.Background()
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(ctx, newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatal(err)
	}

	sealed := log.EncodeRecordBatch(0, []log.Message{{Offset: 0, Key: []byte("sealed"), Value: []byte("zero")}})
	ref := log.SegmentRef{BaseOffset: 0, EndOffset: 0, Key: "topic/0/0-0.segment"}
	if err := pm.s3Client.Put(ctx, ref.Key, sealed, storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	var sidecar bytes.Buffer
	if err := log.WriteSidecar(&sidecar, []log.IndexEntry{{BaseOffset: 0, LastOffset: 0, BatchSize: int32(len(sealed))}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := pm.s3Client.Put(ctx, ref.OffsetIndexObjectKey(), sidecar.Bytes(), storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	active, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "active"), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	tail := log.EncodeRecordBatch(100, []log.Message{{Offset: 100, Key: []byte("tail"), Value: []byte("hundred")}})
	if err := active.Append(tail); err != nil {
		t.Fatal(err)
	}
	ps := pm.GetPartitionState("topic", 0)
	ps.mu.Lock()
	ps.activeSegment = active
	ps.nextOffset = 101
	ps.index.Add(ref)
	ps.index.SetHighWatermark(101)
	ps.mu.Unlock()

	raw, _, err := pm.ReadRawBatches(ctx, "topic", 0, 0, len(sealed)+len(tail))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, sealed) {
		t.Fatalf("read crossed offset gap: got %d bytes, want only sealed %d bytes", len(raw), len(sealed))
	}
}

// A hole in the in-memory index (e.g. a torn read of the live index during a
// concurrent flush) must never let the read path serve the next sealed segment
// across the gap: that would silently relabel a distant offset range as the
// requested one. Reads must stop at the first missing segment and return only
// the contiguous prefix.
func TestReadRawBatchesDoesNotJumpSealedGapToNewestSegment(t *testing.T) {
	ctx := context.Background()
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(ctx, newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatal(err)
	}

	seg0 := log.EncodeRecordBatch(0, []log.Message{
		{Offset: 0, Value: []byte("zero")},
		{Offset: 1, Value: []byte("one")},
		{Offset: 2, Value: []byte("two")},
	})
	ref0 := log.SegmentRef{BaseOffset: 0, EndOffset: 2, Key: "topic/0/0-2.segment"}
	if err := pm.s3Client.Put(ctx, ref0.Key, seg0, storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	var sidecar0 bytes.Buffer
	if err := log.WriteSidecar(&sidecar0, []log.IndexEntry{{BaseOffset: 0, LastOffset: 2, BatchSize: int32(len(seg0))}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := pm.s3Client.Put(ctx, ref0.OffsetIndexObjectKey(), sidecar0.Bytes(), storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	// The newest sealed segment starts far above the first, leaving a hole.
	seg1 := log.EncodeRecordBatch(100, []log.Message{
		{Offset: 100, Value: []byte("hundred")},
		{Offset: 101, Value: []byte("one-oh-one")},
		{Offset: 102, Value: []byte("one-oh-two")},
	})
	ref1 := log.SegmentRef{BaseOffset: 100, EndOffset: 102, Key: "topic/0/100-102.segment"}
	if err := pm.s3Client.Put(ctx, ref1.Key, seg1, storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	var sidecar1 bytes.Buffer
	if err := log.WriteSidecar(&sidecar1, []log.IndexEntry{{BaseOffset: 100, LastOffset: 102, BatchSize: int32(len(seg1))}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := pm.s3Client.Put(ctx, ref1.OffsetIndexObjectKey(), sidecar1.Bytes(), storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	active, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "active"), 200)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	tail := log.EncodeRecordBatch(200, []log.Message{{Offset: 200, Key: []byte("tail"), Value: []byte("two-hundred")}})
	if err := active.Append(tail); err != nil {
		t.Fatal(err)
	}

	ps := pm.GetPartitionState("topic", 0)
	ps.mu.Lock()
	ps.activeSegment = active
	ps.nextOffset = 201
	ps.index.Add(ref0)
	ps.index.Add(ref1)
	ps.index.SetHighWatermark(201)
	ps.mu.Unlock()

	raw, _, err := pm.ReadRawBatches(ctx, "topic", 0, 0, len(seg0)+len(seg1)+len(tail))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, seg0) {
		t.Fatalf("read crossed sealed gap: got %d bytes, want only %d bytes from the first segment", len(raw), len(seg0))
	}
}

func TestSyncFollowerSealedPrefixAdvancesLocalOffsetFromIndex(t *testing.T) {
	pm := newTestPartitionManager(t)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}
	ps := pm.GetPartitionState("topic", 0)
	ps.mu.Lock()
	ps.index.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 9, Key: "topic/0/0-9.seg"})
	ps.index.SetHighWatermark(10)
	ps.nextOffset = 0
	ps.mu.Unlock()

	if got := pm.SyncFollowerSealedPrefix(context.Background(), "topic", 0, 10); got != 10 {
		t.Fatalf("SyncFollowerSealedPrefix() = %d, want 10", got)
	}
}

func TestInitPartitionPreservesRecoveredFollowerEpoch(t *testing.T) {
	pm := newTestPartitionManager(t)
	partitionDir := pm.localPartitionDir("topic", 0)
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(partitionDir, "epoch"), []byte("4"), 0o644); err != nil {
		t.Fatalf("write epoch sidecar: %v", err)
	}

	ps, err := pm.initPartition(context.Background(), "topic", 0, 0)
	if err != nil {
		t.Fatalf("initPartition: %v", err)
	}
	if ps.epoch != 4 {
		t.Fatalf("epoch = %d, want recovered follower epoch 4", ps.epoch)
	}
	epochData, err := os.ReadFile(filepath.Join(partitionDir, "epoch"))
	if err != nil {
		t.Fatalf("read epoch sidecar: %v", err)
	}
	if string(epochData) != "4" {
		t.Fatalf("epoch sidecar = %q, want 4", epochData)
	}
}

// A restarted follower can find an obsolete, previously flushed tail in S3.
// When the current leader fences that tail at its active-segment boundary, the
// follower must not use its local S3 index to advance back into the fenced
// range. That would make it report the same divergent offset forever.
func TestFollowerRestartDoesNotRestoreFencedS3Tail(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = 1 << 20
	cfg.Segments.MaxAge = "1h"
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	const (
		topic         = "bench2"
		fenceOffset   = uint64(262144)
		staleLogEnd   = uint64(263144)
		followerEpoch = uint64(1)
	)
	// This is the shared state left by the old leader: a durable prefix plus an
	// epoch-1 segment that the current leader has subsequently fenced.
	for _, ref := range []log.SegmentRef{
		{BaseOffset: 0, EndOffset: fenceOffset - 1, Epoch: 0, Key: log.FormatSegmentKey(topic, 0, 0, fenceOffset-1, 0)},
		{BaseOffset: fenceOffset, EndOffset: staleLogEnd - 1, Epoch: followerEpoch, Key: log.FormatSegmentKey(topic, 0, fenceOffset, staleLogEnd-1, followerEpoch)},
	} {
		if err := s3Client.Put(ctx, ref.Key, []byte("stale segment"), storage.PutOpts{}); err != nil {
			t.Fatalf("put %s: %v", ref.Key, err)
		}
	}
	state, err := (&log.PartitionState{
		HighWatermark: fenceOffset,
		EpochHistory: []log.EpochEntry{
			{Epoch: 0, StartOffset: 0},
			{Epoch: followerEpoch, StartOffset: fenceOffset},
		},
	}).Marshal()
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if err := s3Client.Put(ctx, log.StateKey(topic, 0), state, storage.PutOpts{}); err != nil {
		t.Fatalf("put state: %v", err)
	}

	// Persisted local state from the follower before its process restart.
	localPartitionDir := filepath.Join(cfg.Cache.Directory, "local", topic, "0")
	if err := os.MkdirAll(localPartitionDir, 0o755); err != nil {
		t.Fatalf("mkdir local partition: %v", err)
	}
	if err := os.WriteFile(filepath.Join(localPartitionDir, "epoch"), []byte("1"), 0o644); err != nil {
		t.Fatalf("write epoch sidecar: %v", err)
	}

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager after restart: %v", err)
	}
	tc := meta.TopicConfig{Name: topic, Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 5, MinInsyncReplicas: 3}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic after restart: %v", err)
	}
	ps := pm.GetPartitionState(topic, 0)
	ps.mu.RLock()
	gotEpoch, gotNextOffset := ps.epoch, ps.nextOffset
	ps.mu.RUnlock()
	if gotEpoch != followerEpoch || gotNextOffset != staleLogEnd {
		t.Fatalf("restarted follower = epoch %d offset %d, want epoch %d offset %d", gotEpoch, gotNextOffset, followerEpoch, staleLogEnd)
	}

	if err := pm.TruncateLogFrom(topic, 0, fenceOffset); err != nil {
		t.Fatalf("TruncateLogFrom: %v", err)
	}
	if got := pm.SyncFollowerSealedPrefix(ctx, topic, 0, fenceOffset); got != fenceOffset {
		t.Fatalf("SyncFollowerSealedPrefix() = %d, want fence boundary %d", got, fenceOffset)
	}
}

// This is the exact partition-0 shape recovered from the live bench2 bucket:
// a segment is present through 263643, but state.json commits only through
// 263143. A follower may use the durable prefix but must not treat the 500
// uncommitted records as replicated data after a restart.
func TestFollowerRestartCapsS3PrefixAtPublishedHighWatermark(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = 1 << 20
	cfg.Segments.MaxAge = "1h"
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	const (
		topic           = "bench2"
		fenceOffset     = uint64(262144)
		publishedHW     = uint64(263144)
		publishedLogEnd = uint64(263644)
		followerEpoch   = uint64(1)
	)
	ref := log.SegmentRef{BaseOffset: 0, EndOffset: publishedLogEnd - 1, Epoch: followerEpoch, Key: log.FormatSegmentKey(topic, 0, 0, publishedLogEnd-1, followerEpoch)}
	if err := s3Client.Put(ctx, ref.Key, []byte("segment beyond high watermark"), storage.PutOpts{}); err != nil {
		t.Fatalf("put segment: %v", err)
	}
	state, err := (&log.PartitionState{
		HighWatermark: publishedHW,
		EpochHistory: []log.EpochEntry{
			{Epoch: 1, StartOffset: 0},
			{Epoch: 1, StartOffset: fenceOffset},
			{Epoch: 1, StartOffset: 262644},
			{Epoch: 1, StartOffset: 262644},
			{Epoch: 1, StartOffset: 262644},
		},
	}).Marshal()
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if err := s3Client.Put(ctx, log.StateKey(topic, 0), state, storage.PutOpts{}); err != nil {
		t.Fatalf("put state: %v", err)
	}

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager: %v", err)
	}
	tc := meta.TopicConfig{Name: topic, Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 5, MinInsyncReplicas: 3}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}
	if err := pm.TruncateLogFrom(topic, 0, fenceOffset); err != nil {
		t.Fatalf("TruncateLogFrom: %v", err)
	}
	if got := pm.SyncFollowerSealedPrefix(ctx, topic, 0, publishedLogEnd); got != publishedHW {
		t.Fatalf("SyncFollowerSealedPrefix() = %d, want published high watermark %d", got, publishedHW)
	}
}

func TestReadRawBatches_UnknownTopicReturnsError(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)

	_, _, err := pm.ReadRawBatches(context.Background(), "nonexistent", 0, 0, 1<<20)
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
}

func TestReadRawBatches_MaxBytesLimit(t *testing.T) {
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
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.mu.Unlock()

	// Append 3 batches.
	now := time.Now().UnixMilli()
	for i := 0; i < 3; i++ {
		msgs := []log.Message{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("val"), Timestamp: now + int64(i)}}
		raw := log.EncodeRecordBatch(0, msgs)
		if _, err := pm.AppendRawBatch(context.Background(), "topic", 0, raw); err != nil {
			t.Fatalf("AppendRawBatch(%d) error = %v", i, err)
		}
	}

	// Use a tiny maxBytes — should still return at least one batch.
	data, _, err := pm.ReadRawBatches(context.Background(), "topic", 0, 0, 1)
	if err != nil {
		t.Fatalf("ReadRawBatches(maxBytes=1) error = %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected at least one batch even with maxBytes=1")
	}

	// Verify we got exactly one batch (the first one).
	decoded, err := log.DecodeRecordBatch(data)
	if err != nil {
		t.Fatalf("DecodeRecordBatch error = %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 message (one batch), got %d", len(decoded))
	}
	if string(decoded[0].Key) != "k0" {
		t.Fatalf("expected key=k0, got %s", decoded[0].Key)
	}
}

func newTestTopicConfig(name string) meta.TopicConfig {
	return meta.TopicConfig{
		Name:              name,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
}

func TestAppendReplicatedRawBatches(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	// Wire up an active segment (followers receive replicated data into the active segment).
	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = false // follower
	ps.mu.Unlock()

	now := time.Now().UnixMilli()

	// Build two raw RecordBatch blobs with leader-assigned offsets.
	// batch1: base=10, 2 records → offsets 10,11
	// batch2: base=12, 1 record  → offset  12
	batch1 := log.EncodeRecordBatch(10, []log.Message{
		{Key: []byte("a"), Value: []byte("1"), Timestamp: now},
		{Key: []byte("b"), Value: []byte("2"), Timestamp: now + 1},
	})
	batch2 := log.EncodeRecordBatch(12, []log.Message{
		{Key: []byte("c"), Value: []byte("3"), Timestamp: now + 2},
	})

	if err := pm.AppendReplicatedRawBatches(context.Background(), "topic", 0, [][]byte{batch1, batch2}); err != nil {
		t.Fatalf("AppendReplicatedRawBatches() error = %v", err)
	}

	// nextOffset should be 13 (last offset 12 + 1).
	ps.mu.RLock()
	gotOffset := ps.nextOffset
	ps.mu.RUnlock()
	if gotOffset != 13 {
		t.Fatalf("nextOffset = %d, want 13", gotOffset)
	}

	// Verify both batches were written via the offset index.
	entries := as.OffsetIndex()
	if len(entries) != 2 {
		t.Fatalf("expected 2 index entries, got %d", len(entries))
	}
	if entries[0].BaseOffset != 10 {
		t.Errorf("entries[0].BaseOffset = %d, want 10", entries[0].BaseOffset)
	}
	if entries[1].BaseOffset != 12 {
		t.Errorf("entries[1].BaseOffset = %d, want 12", entries[1].BaseOffset)
	}
}

func TestAppendReplicatedRawBatches_NoActiveSegment(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	// activeSegment is nil by default — should return an error.
	batch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})
	err := pm.AppendReplicatedRawBatches(context.Background(), "topic", 0, [][]byte{batch})
	if err == nil {
		t.Fatal("expected error for nil active segment, got nil")
	}
}

func TestAppendReplicatedRawBatches_PartitionNotFound(t *testing.T) {
	pm := newTestPartitionManager(t)
	batch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})
	err := pm.AppendReplicatedRawBatches(context.Background(), "ghost", 0, [][]byte{batch})
	if err == nil {
		t.Fatal("expected error for unknown partition, got nil")
	}
}

func TestUpdateFollowerProgressDoesNotAdvertiseUnreplicatedOffsets(t *testing.T) {
	pm := newTestPartitionManager(t)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}
	ps := pm.GetPartitionState("topic", 0)
	ps.mu.Lock()
	ps.nextOffset = 100
	ps.mu.Unlock()

	pm.UpdateFollowerProgress("topic", 0, 1, 1_000, 99)

	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.followerHW != 100 {
		t.Fatalf("follower high watermark = %d, want local log end 100", ps.followerHW)
	}
}

func TestUpdateFollowerProgressCompactsOnlyDurableFollowerPrefix(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatal(err)
	}
	ps := pm.GetPartitionState("topic", 0)
	seg, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "follower-active"), 0)
	if err != nil {
		t.Fatal(err)
	}
	ps.mu.Lock()
	ps.isLeader = false
	ps.activeSegment = seg
	ps.mu.Unlock()
	now := time.Now().UnixMilli()
	first := log.EncodeRecordBatch(0, []log.Message{{Offset: 0, Timestamp: now, Value: []byte("zero")}, {Offset: 1, Timestamp: now, Value: []byte("one")}})
	second := log.EncodeRecordBatch(2, []log.Message{{Offset: 2, Timestamp: now, Value: []byte("two")}})
	if err := pm.AppendReplicatedRawBatches(context.Background(), "topic", 0, [][]byte{first, second}); err != nil {
		t.Fatal(err)
	}
	ps.mu.Lock()
	ps.index.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 1, Key: "topic/0/0-1.seg"})
	ps.mu.Unlock()

	pm.UpdateFollowerProgress("topic", 0, 1, 3, 1)
	ps.mu.RLock()
	compacted := ps.activeSegment
	flushed := ps.flushedOffset
	ps.mu.RUnlock()
	if compacted == nil {
		t.Fatal("follower active segment is nil after compaction")
	}
	if flushed != 1 || compacted.BaseOffset() != 2 || compacted.Size() != int64(len(second)) {
		t.Fatalf("follower segment after compaction = flushed=%d base=%d size=%d", flushed, compacted.BaseOffset(), compacted.Size())
	}
}

func TestUpdateFollowerProgressKeepsPrefixUntilIndexIsRefreshed(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)
	if err := pm.InitTopic(context.Background(), newTestTopicConfig("topic"), map[int]uint64{}); err != nil {
		t.Fatal(err)
	}
	ps := pm.GetPartitionState("topic", 0)
	seg, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "follower-active"), 0)
	if err != nil {
		t.Fatal(err)
	}
	ps.mu.Lock()
	ps.isLeader = false
	ps.activeSegment = seg
	ps.mu.Unlock()
	batch := log.EncodeRecordBatch(0, []log.Message{{Offset: 0, Value: []byte("zero")}, {Offset: 1, Value: []byte("one")}})
	if err := pm.AppendReplicatedRawBatches(context.Background(), "topic", 0, [][]byte{batch}); err != nil {
		t.Fatal(err)
	}

	pm.UpdateFollowerProgress("topic", 0, 1, 2, 1)
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.activeSegment.BaseOffset() != 0 || ps.activeSegment.Size() == 0 {
		t.Fatal("follower discarded a prefix before its sealed index was available")
	}
}

func TestNewestActiveSegmentBaseOffsetPrefersCompactedTail(t *testing.T) {
	dir := t.TempDir()
	for _, offset := range []int64{0, 20} {
		if err := os.WriteFile(filepath.Join(dir, log.SegmentFilename(offset)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := newestActiveSegmentBaseOffset(dir, 0); got != 20 {
		t.Fatalf("active segment base = %d, want compacted tail 20", got)
	}
}
