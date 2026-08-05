package diskless

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

func setupFlushForRead(t *testing.T) (*storage.S3Client, MetaStore) {
	t.Helper()
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch := makeTestBatch(t, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	})

	done := make(chan FlushResult, 1)
	if err := w.Flush(ctx, []BufferEntry{{
		Topic:     "t1",
		Partition: 0,
		Batch:     batch,
		Done:      done,
	}}); err != nil {
		t.Fatalf("setup flush: %v", err)
	}

	result := <-done
	if result.Err != nil {
		t.Fatalf("setup flush result: %v", result.Err)
	}

	return s3, meta
}

func TestReader_FetchFromStart(t *testing.T) {
	s3, meta := setupFlushForRead(t)
	r := NewReader(s3, meta)
	ctx := context.Background()

	data, hw, err := r.Fetch(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 3 {
		t.Fatalf("expected hw=3, got %d", hw)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}

	// Decode header to verify patched offset and record count.
	hdr, err := log.ReadRecordBatchHeader(data)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	if hdr.FirstOffset != 0 {
		t.Fatalf("expected FirstOffset=0, got %d", hdr.FirstOffset)
	}
	if hdr.NumRecords != 3 {
		t.Fatalf("expected NumRecords=3, got %d", hdr.NumRecords)
	}
}

func TestReader_FetchEmptyPartition(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	r := NewReader(s3, meta)
	ctx := context.Background()

	data, hw, err := r.Fetch(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %d bytes", len(data))
	}
	if hw != 0 {
		t.Fatalf("expected hw=0, got %d", hw)
	}
}

func TestReader_FetchPastEnd(t *testing.T) {
	s3, meta := setupFlushForRead(t)
	r := NewReader(s3, meta)
	ctx := context.Background()

	data, hw, err := r.Fetch(ctx, "t1", 0, 3, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %d bytes", len(data))
	}
	if hw != 3 {
		t.Fatalf("expected hw=3, got %d", hw)
	}
}

// TestReader_HighWatermarkIsCommittedNotAllocated verifies that the readable
// high watermark reflects only durably materialized segments, never offsets
// that were allocated but not yet registered (in-flight flushes or gaps).
func TestReader_HighWatermarkIsCommittedNotAllocated(t *testing.T) {
	ctx := context.Background()

	for name, meta := range map[string]MetaStore{
		"memory": NewMemoryMetaStore(),
		"s3":     NewS3MetaStore(testS3Client(t)),
	} {
		t.Run(name, func(t *testing.T) {
			r := NewReader(testS3Client(t), meta)

			// Allocate 5 offsets without materializing them.
			if _, err := meta.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 5}}); err != nil {
				t.Fatalf("allocate: %v", err)
			}
			allocHead, err := meta.GetPartitionHead(ctx, "t", 0)
			if err != nil {
				t.Fatalf("get partition head: %v", err)
			}
			if allocHead != 5 {
				t.Fatalf("allocation head = %d, want 5", allocHead)
			}

			// Nothing committed yet: the readable HW must be 0, not the
			// allocated 5.
			data, hw, err := r.Fetch(ctx, "t", 0, 0, 1<<20)
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}
			if hw != 0 {
				t.Fatalf("hw = %d, want 0 before registration", hw)
			}
			if data != nil {
				t.Fatalf("expected nil data, got %d bytes", len(data))
			}

			// After registration the HW reflects the materialized end. Fetch at
			// the committed head so no backing data file is required.
			if err := meta.RegisterSegment(ctx, SegmentRecord{
				FileKey:   "f.data",
				Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: 0, EndOffset: 5, ByteLength: 10}},
				CreatedAt: time.Now(),
			}); err != nil {
				t.Fatalf("register: %v", err)
			}
			committed, err := meta.GetCommittedHead(ctx, "t", 0)
			if err != nil {
				t.Fatalf("get committed head: %v", err)
			}
			if committed != 5 {
				t.Fatalf("committed head = %d, want 5", committed)
			}
			data, hw, err = r.Fetch(ctx, "t", 0, 5, 1<<20)
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}
			if hw != 5 {
				t.Fatalf("hw = %d, want 5 after registration", hw)
			}
			if data != nil {
				t.Fatalf("expected nil data at committed head, got %d bytes", len(data))
			}
		})
	}
}

// TestReader_HidesPartiallyRegisteredRefs verifies that a segment ref created
// before its committed head advances (a partially-registered flush) is not
// exposed to readers, preventing ghost reads of un-acked data.
func TestReader_HidesPartiallyRegisteredRefs(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewS3MetaStore(s3)
	ctx := context.Background()

	batchA := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("a"), Value: []byte("v")}})
	if err := log.PatchRecordBatchFirstOffset(batchA, 0); err != nil {
		t.Fatalf("patch A: %v", err)
	}
	batchB := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("b1"), Value: []byte("v")},
		{Key: []byte("b2"), Value: []byte("v")},
	})
	if err := log.PatchRecordBatchFirstOffset(batchB, 1); err != nil {
		t.Fatalf("patch B: %v", err)
	}
	if err := s3.Put(ctx, "dataA", batchA, storage.PutOpts{}); err != nil {
		t.Fatalf("put A: %v", err)
	}
	if err := s3.Put(ctx, "dataB", batchB, storage.PutOpts{}); err != nil {
		t.Fatalf("put B: %v", err)
	}
	if _, err := meta.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 3}}); err != nil {
		t.Fatalf("allocate: %v", err)
	}

	// Register A fully -> committed head = 1.
	if err := meta.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "dataA",
		Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: 0, EndOffset: 1, ByteOffset: 0, ByteLength: int64(len(batchA))}},
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("register A: %v", err)
	}

	// Simulate a partial register: B's ref object exists but the committed head
	// has not advanced past it yet.
	refB, err := json.Marshal(s3SegmentRef{
		FileKey: "dataB", ByteOffset: 0, ByteLength: int64(len(batchB)),
		BaseOffset: 1, EndOffset: 3, CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal ref B: %v", err)
	}
	if _, err := s3.ConditionalPut(ctx, s3SegKey("t", 0, 1, 3), refB, ""); err != nil {
		t.Fatalf("create partial ref B: %v", err)
	}

	r := NewReader(s3, meta)
	data, hw, err := r.Fetch(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 1 {
		t.Fatalf("hw = %d, want 1 (committed)", hw)
	}
	if len(data) != len(batchA) {
		t.Fatalf("data = %d bytes, want %d (only committed A, un-acked B hidden)", len(data), len(batchA))
	}

	// Complete the register -> committed = 3; both batches are now visible.
	if err := meta.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "dataB",
		Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: 1, EndOffset: 3, ByteOffset: 0, ByteLength: int64(len(batchB))}},
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("register B: %v", err)
	}
	data, hw, err = r.Fetch(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 3 {
		t.Fatalf("hw = %d, want 3", hw)
	}
	if len(data) != len(batchA)+len(batchB) {
		t.Fatalf("data = %d bytes, want %d (A+B)", len(data), len(batchA)+len(batchB))
	}
}
