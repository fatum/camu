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
	// 2 sidecar orphans (100-1.offset.idx, 100-1.meta.json)
	// + 2 reverse orphans (200-1.segment, 200-1.offset.idx)
	// + 1 legacy index.json
	if len(orphans) != 5 {
		t.Fatalf("expected 5 orphans, got %d: %v", len(orphans), orphans)
	}
}

func TestGC_FindsReverseOrphans(t *testing.T) {
	s3Client, _ := storage.NewS3Client(storage.S3Config{
		Bucket: "test", Region: "us-east-1", Endpoint: "memory://",
	})
	ctx := context.Background()

	// Complete segment — not an orphan.
	s3Client.Put(ctx, "topic1/0/0-1.segment", []byte("seg0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.offset.idx", []byte("idx0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.meta.json", []byte("meta0"), storage.PutOpts{})

	// Bare .segment without .meta.json — reverse orphan.
	s3Client.Put(ctx, "topic1/0/50-1.segment", []byte("seg50"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/50-1.offset.idx", []byte("idx50"), storage.PutOpts{})

	gc := NewGarbageCollector(s3Client)
	orphans, err := gc.FindOrphans(ctx, "topic1", 0)
	if err != nil {
		t.Fatalf("FindOrphans() error: %v", err)
	}
	if len(orphans) != 2 {
		t.Fatalf("expected 2 reverse orphans, got %d: %v", len(orphans), orphans)
	}

	has := make(map[string]bool)
	for _, o := range orphans {
		has[o] = true
	}
	if !has["topic1/0/50-1.segment"] {
		t.Error("expected 50-1.segment to be flagged as orphan")
	}
	if !has["topic1/0/50-1.offset.idx"] {
		t.Error("expected 50-1.offset.idx to be flagged as orphan")
	}
}

func TestGC_FindsLegacyIndexJSON(t *testing.T) {
	s3Client, _ := storage.NewS3Client(storage.S3Config{
		Bucket: "test", Region: "us-east-1", Endpoint: "memory://",
	})
	ctx := context.Background()

	// Complete segment — not an orphan.
	s3Client.Put(ctx, "topic1/0/0-1.segment", []byte("seg0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.offset.idx", []byte("idx0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.meta.json", []byte("meta0"), storage.PutOpts{})

	// Legacy index.json — orphan.
	s3Client.Put(ctx, "topic1/0/index.json", []byte("[]"), storage.PutOpts{})

	// state.json must NOT be flagged.
	s3Client.Put(ctx, "topic1/0/state.json", []byte("{}"), storage.PutOpts{})

	gc := NewGarbageCollector(s3Client)
	orphans, err := gc.FindOrphans(ctx, "topic1", 0)
	if err != nil {
		t.Fatalf("FindOrphans() error: %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("expected 1 orphan (index.json), got %d: %v", len(orphans), orphans)
	}
	if orphans[0] != "topic1/0/index.json" {
		t.Errorf("expected topic1/0/index.json, got %s", orphans[0])
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
// - A complete segment with sidecars: "topic1/0/0-1.{segment,offset.idx,meta.json}"
// - Orphaned sidecars with no matching segment: "topic1/0/100-1.{offset.idx,meta.json}"
// - Non-sidecar files that should be ignored: state.json, producers.checkpoint
func newTestGC(t *testing.T) *GarbageCollector {
	t.Helper()
	s3Client, _ := storage.NewS3Client(storage.S3Config{
		Bucket: "test", Region: "us-east-1", Endpoint: "memory://",
	})
	ctx := context.Background()

	// Complete segment with sidecars — not orphans.
	s3Client.Put(ctx, "topic1/0/0-1.segment", []byte("seg0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.offset.idx", []byte("idx0"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/0-1.meta.json", []byte("meta0"), storage.PutOpts{})

	// Orphaned sidecars (segment missing, e.g. crash during flush).
	s3Client.Put(ctx, "topic1/0/100-1.offset.idx", []byte("idx100"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/100-1.meta.json", []byte("meta100"), storage.PutOpts{})

	// Reverse orphan: bare .segment without .meta.json (crash after segment upload).
	s3Client.Put(ctx, "topic1/0/200-1.segment", []byte("seg200"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/200-1.offset.idx", []byte("idx200"), storage.PutOpts{})

	// Legacy index.json that should be cleaned up.
	s3Client.Put(ctx, "topic1/0/index.json", []byte("[]"), storage.PutOpts{})

	// Non-sidecar files that must be ignored.
	s3Client.Put(ctx, "topic1/0/state.json", []byte("{}"), storage.PutOpts{})
	s3Client.Put(ctx, "topic1/0/producers.checkpoint", []byte("{}"), storage.PutOpts{})

	return NewGarbageCollector(s3Client)
}
