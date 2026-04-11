package diskless

import (
	"context"
	"testing"

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
