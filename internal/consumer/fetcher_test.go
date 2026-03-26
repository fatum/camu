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
	if err := log.WriteSegment(&buf, msgs, log.CompressionNone, 16*1024); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	return buf.Bytes()
}

func writeTestSegmentIndex(t *testing.T, segData []byte, baseOffset uint64) []byte {
	t.Helper()
	idx, err := log.BuildSegmentOffsetIndex(segData, baseOffset, 1)
	if err != nil {
		t.Fatalf("BuildSegmentOffsetIndex: %v", err)
	}
	return idx
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
	if err := diskCache.Put(log.SegmentOffsetIndexKey(segKey), writeTestSegmentIndex(t, segData, 0)); err != nil {
		t.Fatalf("diskCache.Put(offset index): %v", err)
	}

	fetcher := NewFetcher(s3Client, diskCache)

	// Fetch from offset 0 — should read from cache.
	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, "test-topic", 0, 0, 10)
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
	if err := s3Client.Put(context.Background(), log.SegmentOffsetIndexKey(segKey), writeTestSegmentIndex(t, segData, 0), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(offset index): %v", err)
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
	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, "test-topic", 0, 0, 10)
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
	if !diskCache.Has(log.SegmentOffsetIndexKey(segKey)) {
		t.Error("expected segment offset index to be cached after S3 fetch")
	}

	// Second fetch — should come from cache (we can verify by deleting from S3).
	if err := s3Client.Delete(context.Background(), segKey); err != nil {
		t.Fatalf("s3Client.Delete: %v", err)
	}

	result2, nextOffset2, err := fetcher.Fetch(context.Background(), idx, "test-topic", 0, 0, 10)
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

func TestFetcher_UsesExplicitOffsetIndexKey(t *testing.T) {
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

	msgs := []log.Message{
		{Offset: 10, Timestamp: time.Now().UnixNano(), Key: []byte("k10"), Value: []byte("v10")},
		{Offset: 11, Timestamp: time.Now().UnixNano(), Key: []byte("k11"), Value: []byte("v11")},
		{Offset: 12, Timestamp: time.Now().UnixNano(), Key: []byte("k12"), Value: []byte("v12")},
	}
	segKey := "test-topic/0/10-0.segment"
	offsetIdxKey := "test-topic/0/assets/10-0.offset.idx"
	segData := writeTestSegment(t, msgs)
	if err := s3Client.Put(context.Background(), segKey, segData, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(segment): %v", err)
	}
	if err := s3Client.Put(context.Background(), offsetIdxKey, writeTestSegmentIndex(t, segData, 10), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(offset index): %v", err)
	}

	idx := log.NewIndex()
	idx.Add(log.SegmentRef{
		BaseOffset:     10,
		EndOffset:      12,
		Key:            segKey,
		OffsetIndexKey: offsetIdxKey,
		CreatedAt:      time.Now(),
	})

	fetcher := NewFetcher(s3Client, diskCache)
	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, "test-topic", 0, 11, 1)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(result) != 1 || result[0].Offset != 11 {
		t.Fatalf("unexpected fetch result: %+v", result)
	}
	if nextOffset != 12 {
		t.Fatalf("nextOffset = %d, want 12", nextOffset)
	}
	if !diskCache.Has(offsetIdxKey) {
		t.Fatal("expected explicit offset index sidecar to be cached")
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

	// Fetch from empty topic — no indexed segments.
	result, nextOffset, err := fetcher.Fetch(context.Background(), log.NewIndex(), "test-topic", 0, 0, 10)
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

func TestFetcher_ReadAcrossMultipleSegments(t *testing.T) {
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

	seg0Msgs := []log.Message{
		{Offset: 0, Timestamp: time.Now().UnixNano(), Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: time.Now().UnixNano(), Key: []byte("k1"), Value: []byte("v1")},
	}
	seg1Msgs := []log.Message{
		{Offset: 2, Timestamp: time.Now().UnixNano(), Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 3, Timestamp: time.Now().UnixNano(), Key: []byte("k3"), Value: []byte("v3")},
	}

	seg0Key := "test-topic/0/0-0.segment"
	seg1Key := "test-topic/0/2-0.segment"
	if err := s3Client.Put(context.Background(), seg0Key, writeTestSegment(t, seg0Msgs), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(seg0): %v", err)
	}
	if err := s3Client.Put(context.Background(), seg1Key, writeTestSegment(t, seg1Msgs), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(seg1): %v", err)
	}

	idx := log.NewIndex()
	idx.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  1,
		Epoch:      0,
		Key:        seg0Key,
		CreatedAt:  time.Now(),
	})
	idx.Add(log.SegmentRef{
		BaseOffset: 2,
		EndOffset:  3,
		Epoch:      0,
		Key:        seg1Key,
		CreatedAt:  time.Now(),
	})

	fetcher := NewFetcher(s3Client, diskCache)

	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, "test-topic", 0, 1, 3)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 messages across segments, got %d", len(result))
	}
	for i, want := range []uint64{1, 2, 3} {
		if result[i].Offset != want {
			t.Fatalf("result[%d].Offset = %d, want %d", i, result[i].Offset, want)
		}
	}
	if nextOffset != 4 {
		t.Fatalf("nextOffset = %d, want 4", nextOffset)
	}
}

func TestFetcher_ReadAcrossMixedCacheAndS3Segments(t *testing.T) {
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

	seg0Key := "test-topic/0/0-0.segment"
	seg1Key := "test-topic/0/2-0.segment"
	seg0Data := writeTestSegment(t, []log.Message{
		{Offset: 0, Timestamp: time.Now().UnixNano(), Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: time.Now().UnixNano(), Key: []byte("k1"), Value: []byte("v1")},
	})
	seg1Data := writeTestSegment(t, []log.Message{
		{Offset: 2, Timestamp: time.Now().UnixNano(), Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 3, Timestamp: time.Now().UnixNano(), Key: []byte("k3"), Value: []byte("v3")},
	})

	if err := diskCache.Put(seg0Key, seg0Data); err != nil {
		t.Fatalf("diskCache.Put(seg0): %v", err)
	}
	if err := s3Client.Put(context.Background(), seg1Key, seg1Data, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(seg1): %v", err)
	}

	idx := log.NewIndex()
	idx.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 1, Key: seg0Key, CreatedAt: time.Now()})
	idx.Add(log.SegmentRef{BaseOffset: 2, EndOffset: 3, Key: seg1Key, CreatedAt: time.Now()})

	fetcher := NewFetcher(s3Client, diskCache)
	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, "test-topic", 0, 0, 4)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(result) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(result))
	}
	if nextOffset != 4 {
		t.Fatalf("nextOffset = %d, want 4", nextOffset)
	}
	if !diskCache.Has(seg1Key) {
		t.Fatal("expected S3-fetched second segment to be cached")
	}
}

func TestFetcher_ReturnsPartialResultsWhenLaterSegmentFetchFails(t *testing.T) {
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

	seg0Key := "test-topic/0/0-0.segment"
	seg1Key := "test-topic/0/2-0.segment"
	if err := s3Client.Put(context.Background(), seg0Key, writeTestSegment(t, []log.Message{
		{Offset: 0, Timestamp: time.Now().UnixNano(), Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: time.Now().UnixNano(), Key: []byte("k1"), Value: []byte("v1")},
	}), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(seg0): %v", err)
	}

	idx := log.NewIndex()
	idx.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 1, Key: seg0Key, CreatedAt: time.Now()})
	idx.Add(log.SegmentRef{BaseOffset: 2, EndOffset: 3, Key: seg1Key, CreatedAt: time.Now()})

	fetcher := NewFetcher(s3Client, diskCache)
	result, nextOffset, err := fetcher.Fetch(context.Background(), idx, "test-topic", 0, 0, 4)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected partial 2 messages, got %d", len(result))
	}
	if nextOffset != 2 {
		t.Fatalf("nextOffset = %d, want 2", nextOffset)
	}
}

func TestFetcher_ReturnsErrorWhenFirstSegmentFetchFails(t *testing.T) {
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

	idx := log.NewIndex()
	idx.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 1, Key: "missing.segment", CreatedAt: time.Now()})

	fetcher := NewFetcher(s3Client, diskCache)
	_, _, err = fetcher.Fetch(context.Background(), idx, "test-topic", 0, 0, 2)
	if err == nil {
		t.Fatal("expected error when first segment fetch fails")
	}
}
