package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = maxSize
	cfg.Segments.MaxAge = "1h"

	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewPartitionManager() error = %v", err)
	}
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

func TestReadReplicaRawBatches_ReadsPastHighWatermark(t *testing.T) {
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
	ps.replicaState = replication.NewReplicaState("leader", 1, 1, 1000) // readable HW=1, log end=2
	ps.mu.Unlock()

	data, hw, err := pm.ReadRawBatches(context.Background(), "topic", 0, 1, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches() error = %v", err)
	}
	if hw != 1 {
		t.Fatalf("ReadRawBatches() hw = %d, want 1", hw)
	}
	if len(data) != 0 {
		t.Fatalf("ReadRawBatches() returned %d bytes, want 0 beyond readable HW", len(data))
	}

	replicaData, leo, err := pm.ReadReplicaRawBatches(context.Background(), "topic", 0, 1, 1<<20)
	if err != nil {
		t.Fatalf("ReadReplicaRawBatches() error = %v", err)
	}
	if leo != 2 {
		t.Fatalf("ReadReplicaRawBatches() upper bound = %d, want 2", leo)
	}
	if len(replicaData) == 0 {
		t.Fatal("ReadReplicaRawBatches() returned no data, want uncommitted tail batch")
	}
	decoded, err := log.DecodeRecordBatch(replicaData)
	if err != nil {
		t.Fatalf("DecodeRecordBatch() error = %v", err)
	}
	if len(decoded) != 1 || string(decoded[0].Key) != "k1" {
		t.Fatalf("decoded replica batch = %+v, want one k1 record", decoded)
	}
}

func TestReadReplicaRawBatchesDoesNotServeSealedPrefix(t *testing.T) {
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

	data, logEnd, err := pm.ReadReplicaRawBatches(context.Background(), "topic", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadReplicaRawBatches() error = %v", err)
	}
	if logEnd != 11 {
		t.Fatalf("log end = %d, want 11", logEnd)
	}
	if len(data) != 0 {
		t.Fatalf("sealed-prefix read returned %d bytes, want none", len(data))
	}

	data, _, err = pm.ReadReplicaRawBatches(context.Background(), "topic", 0, 10, 1<<20)
	if err != nil {
		t.Fatalf("ReadReplicaRawBatches(active tail) error = %v", err)
	}
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
