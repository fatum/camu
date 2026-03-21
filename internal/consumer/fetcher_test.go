package consumer

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

func makeTestMessages(count int) []log.Message {
	msgs := make([]log.Message, count)
	for i := range msgs {
		msgs[i] = log.Message{
			Offset:    uint64(i),
			Timestamp: time.Now().UnixNano(),
			Key:       []byte("key"),
			Value:     []byte("value-" + string(rune('0'+i))),
		}
	}
	return msgs
}

func writeTestSegment(t *testing.T, msgs []log.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := log.WriteSegment(&buf, msgs, log.CompressionNone); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	return buf.Bytes()
}

func TestFetcher_ReadFromCache(t *testing.T) {
	// Set up S3 client (in-memory, but we won't put segment there).
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	// Set up disk cache.
	cacheDir := t.TempDir()
	diskCache, err := log.NewDiskCache(cacheDir, 100*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	// Create test messages and segment.
	msgs := makeTestMessages(3)
	segData := writeTestSegment(t, msgs)

	// Build index.
	segKey := "test-topic/0/0-0.segment"
	idx := log.NewIndex()
	idx.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  2,
		Epoch:      0,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	// Put segment in disk cache (not in S3).
	if err := diskCache.Put(segKey, segData); err != nil {
		t.Fatalf("diskCache.Put: %v", err)
	}

	fetcher := NewFetcher(s3Client, diskCache)

	// Fetch from offset 0 — should read from cache.
	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, nil, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(result))
	}
	if nextOffset != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOffset)
	}
	if string(result[0].Value) != string(msgs[0].Value) {
		t.Errorf("first message value = %q, want %q", result[0].Value, msgs[0].Value)
	}
}

func TestFetcher_ReadFromS3(t *testing.T) {
	// Set up S3 client (in-memory).
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	// Set up disk cache (empty).
	cacheDir := t.TempDir()
	diskCache, err := log.NewDiskCache(cacheDir, 100*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	// Create test messages and segment.
	msgs := makeTestMessages(3)
	segData := writeTestSegment(t, msgs)

	// Put segment in S3 (not in cache).
	segKey := "test-topic/0/0-0.segment"
	if err := s3Client.Put(context.Background(), segKey, segData, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put: %v", err)
	}

	// Build index.
	idx := log.NewIndex()
	idx.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  2,
		Epoch:      0,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	fetcher := NewFetcher(s3Client, diskCache)

	// First fetch — should come from S3 and be cached.
	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, nil, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch (first): %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(result))
	}
	if nextOffset != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOffset)
	}

	// Verify it was cached.
	if !diskCache.Has(segKey) {
		t.Error("expected segment to be cached after S3 fetch")
	}

	// Second fetch — should come from cache (we can verify by deleting from S3).
	if err := s3Client.Delete(context.Background(), segKey); err != nil {
		t.Fatalf("s3Client.Delete: %v", err)
	}

	result2, nextOffset2, err := fetcher.Fetch(context.Background(), idx, nil, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch (second, from cache): %v", err)
	}
	if len(result2) != 3 {
		t.Fatalf("expected 3 messages from cache, got %d", len(result2))
	}
	if nextOffset2 != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOffset2)
	}
}

func TestFetcher_ReadFromBuffer(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	cacheDir := t.TempDir()
	diskCache, err := log.NewDiskCache(cacheDir, 100*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	fetcher := NewFetcher(s3Client, diskCache)

	// Create buffer messages (unflushed).
	buffer := makeTestMessages(3)

	// Fetch from offset 0 with buffer — should read from buffer.
	result, nextOffset, err := fetcher.Fetch(context.Background(), log.NewIndex(), buffer, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 messages from buffer, got %d", len(result))
	}
	if nextOffset != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOffset)
	}

	// Fetch from offset 1 — should get 2 messages.
	result2, nextOffset2, err := fetcher.Fetch(context.Background(), log.NewIndex(), buffer, "test-topic", 0, 1, 10)
	if err != nil {
		t.Fatalf("Fetch from offset 1: %v", err)
	}
	if len(result2) != 2 {
		t.Fatalf("expected 2 messages from buffer, got %d", len(result2))
	}
	if nextOffset2 != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOffset2)
	}
}

func TestFetcher_EmptyTopic(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	cacheDir := t.TempDir()
	diskCache, err := log.NewDiskCache(cacheDir, 100*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	fetcher := NewFetcher(s3Client, diskCache)

	// Fetch from empty topic — no index, no buffer.
	result, nextOffset, err := fetcher.Fetch(context.Background(), log.NewIndex(), nil, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(result))
	}
	if nextOffset != 0 {
		t.Errorf("nextOffset = %d, want 0", nextOffset)
	}
}
