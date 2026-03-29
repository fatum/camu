package log

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func TestRetention_RemovesExpiredSegments(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket: "test", Region: "us-east-1", Endpoint: "memory://",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	now := time.Now()

	// Expired segment (48h old, retention is 24h).
	expiredSegKey := "topic/0/00000000000000000000-99-1.segment"
	expiredIdxKey := "topic/0/00000000000000000000-99-1.offset.idx"
	expiredMetaKey := "topic/0/00000000000000000000-99-1.meta.json"
	expiredMeta := SegmentMetadata{
		BaseOffset:     0,
		EndOffset:      99,
		Epoch:          1,
		SegmentKey:     expiredSegKey,
		OffsetIndexKey: expiredIdxKey,
		CreatedAt:      now.Add(-48 * time.Hour),
	}
	expiredMetaData, _ := json.Marshal(expiredMeta)
	s3Client.Put(ctx, expiredSegKey, []byte("seg-data"), storage.PutOpts{})
	s3Client.Put(ctx, expiredIdxKey, []byte("idx-data"), storage.PutOpts{})
	s3Client.Put(ctx, expiredMetaKey, expiredMetaData, storage.PutOpts{})

	// Fresh segment (1h old, should survive).
	freshSegKey := "topic/0/00000000000000000100-199-1.segment"
	freshIdxKey := "topic/0/00000000000000000100-199-1.offset.idx"
	freshMetaKey := "topic/0/00000000000000000100-199-1.meta.json"
	freshMeta := SegmentMetadata{
		BaseOffset:     100,
		EndOffset:      199,
		Epoch:          1,
		SegmentKey:     freshSegKey,
		OffsetIndexKey: freshIdxKey,
		CreatedAt:      now.Add(-1 * time.Hour),
	}
	freshMetaData, _ := json.Marshal(freshMeta)
	s3Client.Put(ctx, freshSegKey, []byte("seg-data"), storage.PutOpts{})
	s3Client.Put(ctx, freshIdxKey, []byte("idx-data"), storage.PutOpts{})
	s3Client.Put(ctx, freshMetaKey, freshMetaData, storage.PutOpts{})

	rc := NewRetentionCleaner(s3Client, time.Hour, 24*time.Hour)
	p := PartitionInfo{Topic: "topic", PartitionID: 0}
	if err := rc.cleanPartition(ctx, p); err != nil {
		t.Fatalf("cleanPartition: %v", err)
	}

	// Expired segment files should be gone.
	if _, err := s3Client.Get(ctx, expiredSegKey); err == nil {
		t.Error("expired segment still exists")
	}
	if _, err := s3Client.Get(ctx, expiredIdxKey); err == nil {
		t.Error("expired offset index still exists")
	}
	if _, err := s3Client.Get(ctx, expiredMetaKey); err == nil {
		t.Error("expired metadata still exists")
	}

	// Fresh segment files should still be present.
	if _, err := s3Client.Get(ctx, freshSegKey); err != nil {
		t.Error("fresh segment was deleted")
	}
	if _, err := s3Client.Get(ctx, freshIdxKey); err != nil {
		t.Error("fresh offset index was deleted")
	}
	if _, err := s3Client.Get(ctx, freshMetaKey); err != nil {
		t.Error("fresh metadata was deleted")
	}
}
