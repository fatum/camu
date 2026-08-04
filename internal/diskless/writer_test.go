package diskless

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

func testS3Client(t *testing.T) *storage.S3Client {
	t.Helper()
	s3, err := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("create s3 client: %v", err)
	}
	return s3
}

func makeTestBatch(t *testing.T, msgs []log.Message) []byte {
	t.Helper()
	return log.EncodeRecordBatch(0, msgs)
}

func TestWriter_FlushSinglePartition(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch := makeTestBatch(t, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	})

	done := make(chan FlushResult, 1)
	entries := []BufferEntry{{
		Topic:     "t1",
		Partition: 0,
		Batch:     batch,
		Done:      done,
	}}

	if err := w.Flush(ctx, entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Verify head advanced by 2.
	head, err := meta.GetPartitionHead(ctx, "t1", 0)
	if err != nil {
		t.Fatalf("get head: %v", err)
	}
	if head != 2 {
		t.Fatalf("expected head=2, got %d", head)
	}

	// Verify segment registered with 1 ref.
	refs, err := meta.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].BaseOffset != 0 || refs[0].EndOffset != 2 {
		t.Fatalf("unexpected ref offsets: base=%d end=%d", refs[0].BaseOffset, refs[0].EndOffset)
	}

	// Verify S3 file exists.
	fileData, err := s3.Get(ctx, refs[0].FileKey)
	if err != nil {
		t.Fatalf("s3 get: %v", err)
	}
	if len(fileData) == 0 {
		t.Fatal("s3 file is empty")
	}

	// Verify Done channel result.
	result := <-done
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if result.BaseOffset != 0 {
		t.Fatalf("expected BaseOffset=0, got %d", result.BaseOffset)
	}
}

func TestWriter_FlushMultiPartition(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch1 := makeTestBatch(t, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
	})
	batch2 := makeTestBatch(t, []log.Message{
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	})

	done1 := make(chan FlushResult, 1)
	done2 := make(chan FlushResult, 1)
	entries := []BufferEntry{
		{Topic: "t1", Partition: 0, Batch: batch1, Done: done1},
		{Topic: "t1", Partition: 1, Batch: batch2, Done: done2},
	}

	if err := w.Flush(ctx, entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Verify heads.
	head0, _ := meta.GetPartitionHead(ctx, "t1", 0)
	head1, _ := meta.GetPartitionHead(ctx, "t1", 1)
	if head0 != 1 {
		t.Fatalf("expected head0=1, got %d", head0)
	}
	if head1 != 2 {
		t.Fatalf("expected head1=2, got %d", head1)
	}

	// Verify both refs point to same file key with different byte offsets.
	refs0, _ := meta.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	refs1, _ := meta.QuerySegments(ctx, "t1", 1, 0, 1<<20)
	if len(refs0) != 1 || len(refs1) != 1 {
		t.Fatalf("expected 1 ref each, got %d and %d", len(refs0), len(refs1))
	}
	if refs0[0].FileKey != refs1[0].FileKey {
		t.Fatal("expected same file key for both partitions")
	}
	if refs0[0].ByteOffset == refs1[0].ByteOffset {
		t.Fatal("expected different byte offsets")
	}
}

// flakyMetaStore wraps a MetaStore and fails a configurable number of
// RegisterSegment calls, modeling a transient segment-catalog write failure.
type flakyMetaStore struct {
	MetaStore
	registerFails atomic.Int32
}

func (m *flakyMetaStore) RegisterSegment(ctx context.Context, seg SegmentRecord) error {
	if m.registerFails.Add(1) == 1 {
		return errors.New("transient register error")
	}
	return m.MetaStore.RegisterSegment(ctx, seg)
}

// TestWriter_Flush_RetriesTransientPutFailure verifies that a transient S3 PUT
// failure does not strand the allocated offsets as a gap: the flush retries
// idempotently with the same file key and succeeds.
func TestWriter_Flush_RetriesTransientPutFailure(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	var puts atomic.Int32
	s3.SetFaultInjector(func(op string) error {
		if op == "put" && puts.Add(1) == 1 {
			return errors.New("transient s3 error")
		}
		return nil
	})
	defer s3.SetFaultInjector(nil)

	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch := makeTestBatch(t, []log.Message{{Key: []byte("k1"), Value: []byte("v1")}})
	done := make(chan FlushResult, 1)
	entries := []BufferEntry{{Topic: "t1", Partition: 0, Batch: batch, Done: done}}

	if err := w.Flush(ctx, entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// No gap: the head advanced and the segment is registered with its data.
	head, err := meta.GetPartitionHead(ctx, "t1", 0)
	if err != nil {
		t.Fatalf("get head: %v", err)
	}
	if head != 1 {
		t.Fatalf("head = %d, want 1 (no gap from the failed put)", head)
	}
	refs, err := meta.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %d, want 1", len(refs))
	}
	if _, err := s3.Get(ctx, refs[0].FileKey); err != nil {
		t.Fatalf("retried put did not persist the file: %v", err)
	}
}

// TestWriter_Flush_RetriesTransientRegisterFailure verifies that a transient
// segment-registration failure after a successful PUT is retried rather than
// orphaning the materialized offsets.
func TestWriter_Flush_RetriesTransientRegisterFailure(t *testing.T) {
	s3 := testS3Client(t)
	meta := &flakyMetaStore{MetaStore: NewMemoryMetaStore()}
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch := makeTestBatch(t, []log.Message{{Key: []byte("k1"), Value: []byte("v1")}})
	done := make(chan FlushResult, 1)
	entries := []BufferEntry{{Topic: "t1", Partition: 0, Batch: batch, Done: done}}

	if err := w.Flush(ctx, entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	head, err := meta.GetPartitionHead(ctx, "t1", 0)
	if err != nil {
		t.Fatalf("get head: %v", err)
	}
	if head != 1 {
		t.Fatalf("head = %d, want 1", head)
	}
	refs, err := meta.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %d, want 1 (register retried)", len(refs))
	}
}

// TestWriter_Flush_FailsAfterPersistentPutError verifies that a persistent
// object-store failure surfaces to the caller once the retry budget (the
// context) is exhausted, instead of blocking forever.
func TestWriter_Flush_FailsAfterPersistentPutError(t *testing.T) {
	s3 := testS3Client(t)
	s3.SetFaultInjector(func(op string) error {
		if op == "put" {
			return errors.New("s3 down")
		}
		return nil
	})
	defer s3.SetFaultInjector(nil)

	w := NewWriter(s3, NewMemoryMetaStore(), "node1")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	batch := makeTestBatch(t, []log.Message{{Key: []byte("k1"), Value: []byte("v1")}})
	done := make(chan FlushResult, 1)
	entries := []BufferEntry{{Topic: "t1", Partition: 0, Batch: batch, Done: done}}

	if err := w.Flush(ctx, entries); err == nil {
		t.Fatal("expected flush to fail after persistent put errors")
	}
	result := <-done
	if result.Err == nil {
		t.Fatal("expected the produce result to carry the flush error")
	}
}
