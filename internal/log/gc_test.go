package log

import (
	"context"
	"testing"

	"github.com/maksim/camu/internal/storage"
)

func TestGC_FindsOrphanedSegmentAssets(t *testing.T) {
	gc := newTestGC(t)
	ctx := context.Background()

	// Set up: index references "0-1.segment" but S3 also has "100-1.segment" (orphaned)
	orphans, err := gc.FindOrphans(ctx, "topic1", 0)
	if err != nil {
		t.Fatalf("FindOrphans() error: %v", err)
	}
	if len(orphans) != 3 {
		t.Fatalf("expected 3 orphan assets, got %d", len(orphans))
	}
}

func TestGC_CleansOrphans(t *testing.T) {
	gc := newTestGC(t)
	ctx := context.Background()

	err := gc.CleanOrphans(ctx, "topic1", 0)
	if err != nil {
		t.Fatalf("CleanOrphans() error: %v", err)
	}

	// Verify orphan is gone
	orphans, _ := gc.FindOrphans(ctx, "topic1", 0)
	if len(orphans) != 0 {
		t.Errorf("expected 0 orphans after cleanup, got %d", len(orphans))
	}
}

// newTestGC creates a GC with in-memory S3 that has:
// - An index at topic1/0/index.json referencing segment "topic1/0/0-1.segment"
// - Segment sidecars for the indexed segment
// - A full orphaned segment asset set for "topic1/0/100-1"
func newTestGC(t *testing.T) *GarbageCollector {
	t.Helper()
	s3Client, _ := storage.NewS3Client(storage.S3Config{
		Bucket: "test", Region: "us-east-1", Endpoint: "memory://",
	})
	ctx := context.Background()

	// Create index with one segment
	idx := NewIndex()
	idx.Add(SegmentRef{
		BaseOffset:     0,
		EndOffset:      99,
		Epoch:          1,
		Key:            "topic1/0/0-1.segment",
		OffsetIndexKey: "topic1/0/0-1.offset.idx",
		MetaKey:        "topic1/0/0-1.meta.json",
	})
	data, _ := idx.MarshalJSON()
	s3Client.Put(ctx, "topic1/0/index.json", data, storage.PutOpts{})

	// Create indexed segment assets in S3.
	s3Client.Put(ctx, "topic1/0/0-1.segment", []byte("seg0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.offset.idx", []byte("idx0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.meta.json", []byte("meta0"), storage.PutOpts{})

	// Create orphaned segment assets in S3.
	s3Client.Put(ctx, "topic1/0/100-1.segment", []byte("seg100"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/100-1.offset.idx", []byte("idx100"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/100-1.meta.json", []byte("meta100"), storage.PutOpts{})

	return NewGarbageCollector(s3Client)
}
