package diskless

import (
	"context"
	"testing"

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
