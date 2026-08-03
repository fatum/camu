package consumer

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/metrics"
	"github.com/maksim/camu/internal/storage"
)

func makeTestMessages(count int) []log.Message {
	msgs := make([]log.Message, count)
	for i := range msgs {
		msgs[i] = log.Message{
			Offset:    uint64(i),
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("key"),
			Value:     []byte("value-" + string(rune('0'+i))),
		}
	}
	return msgs
}

func writeTestSegment(t *testing.T, msgs []log.Message) []byte {
	t.Helper()
	var segData []byte
	for _, msg := range msgs {
		segData = append(segData, log.EncodeRecordBatch(int64(msg.Offset), []log.Message{msg})...)
	}
	return segData
}

func putTestSealedSegment(t *testing.T, s3Client *storage.S3Client, key string, data []byte) {
	t.Helper()
	if err := s3Client.Put(context.Background(), key, data, storage.PutOpts{}); err != nil {
		t.Fatalf("put segment: %v", err)
	}
	var entries []log.IndexEntry
	for position := 0; position < len(data); {
		header, err := log.ReadRecordBatchHeader(data[position:])
		if err != nil {
			t.Fatalf("read batch header: %v", err)
		}
		batchSize := int(header.RecordBatchSize())
		entries = append(entries, log.IndexEntry{BaseOffset: header.FirstOffset, LastOffset: header.LastOffset(), Position: int64(position), BatchSize: int32(batchSize)})
		position += batchSize
	}
	var sidecar bytes.Buffer
	if err := log.WriteSidecar(&sidecar, entries, nil); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	if err := s3Client.Put(context.Background(), log.SegmentOffsetIndexKey(key), sidecar.Bytes(), storage.PutOpts{}); err != nil {
		t.Fatalf("put sidecar: %v", err)
	}
}

func TestFetcher_WalkRangeReadsOnlyNeededBatch(t *testing.T) {
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	diskCache, err := log.NewDiskCache(t.TempDir(), 100*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	first := log.EncodeRecordBatch(0, makeTestMessages(2))
	secondMessages := makeTestMessages(2)
	for i := range secondMessages {
		secondMessages[i].Offset += 2
	}
	second := log.EncodeRecordBatch(2, secondMessages)
	segment := append(append([]byte(nil), first...), second...)
	key := "test-topic/0/0-3.segment"
	if err := s3Client.Put(context.Background(), key, segment, storage.PutOpts{}); err != nil {
		t.Fatalf("put segment: %v", err)
	}
	var sidecar bytes.Buffer
	if err := log.WriteSidecar(&sidecar, []log.IndexEntry{
		{BaseOffset: 0, LastOffset: 1, Position: 0, BatchSize: int32(len(first))},
		{BaseOffset: 2, LastOffset: 3, Position: int64(len(first)), BatchSize: int32(len(second))},
	}, nil); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	if err := s3Client.Put(context.Background(), log.SegmentOffsetIndexKey(key), sidecar.Bytes(), storage.PutOpts{}); err != nil {
		t.Fatalf("put sidecar: %v", err)
	}

	registry := metrics.NewRegistry()
	s3Client.SetMetrics(registry)
	idx := log.NewIndex()
	idx.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 3, Key: key, CreatedAt: time.Now()})

	var got []uint64
	next, err := NewFetcher(s3Client, diskCache).Walk(context.Background(), idx, "test-topic", 0, 2, 1, func(m log.Message) bool {
		got = append(got, m.Offset)
		return true
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(got) != 1 || got[0] != 2 || next != 3 {
		t.Fatalf("Walk = messages=%v next=%d, want [2], 3", got, next)
	}

	metricsText := registry.Handler()
	wantRange := "camu_s3_bytes_total{direction=\"read\",operation=\"get_range\"} " + strconv.Itoa(len(second))
	if !strings.Contains(metricsText, wantRange) {
		t.Fatalf("range read metrics missing %q:\n%s", wantRange, metricsText)
	}
	if strings.Contains(metricsText, "operation=\"get\"} "+strconv.Itoa(len(segment))) {
		t.Fatalf("Walk loaded the full segment:\n%s", metricsText)
	}
}

func TestFetcher_ReadsBoundedRangeFromS3(t *testing.T) {
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

	putTestSealedSegment(t, s3Client, segKey, segData)

	fetcher := NewFetcher(s3Client, diskCache)

	// Fetch from offset 0 through the sidecar-guided range path.
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
	putTestSealedSegment(t, s3Client, segKey, segData)

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

	// First fetch reads a bounded S3 range; only the sidecar is cached.
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

	if diskCache.Has(segKey) {
		t.Error("segment payload must not be cached")
	}
}

func TestFetcher_FetchFromMiddleOfSegment(t *testing.T) {
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
		{Offset: 10, Timestamp: time.Now().UnixMilli(), Key: []byte("k10"), Value: []byte("v10")},
		{Offset: 11, Timestamp: time.Now().UnixMilli(), Key: []byte("k11"), Value: []byte("v11")},
		{Offset: 12, Timestamp: time.Now().UnixMilli(), Key: []byte("k12"), Value: []byte("v12")},
	}
	segKey := "test-topic/0/10-0.segment"
	segData := writeTestSegment(t, msgs)
	putTestSealedSegment(t, s3Client, segKey, segData)

	idx := log.NewIndex()
	idx.Add(log.SegmentRef{
		BaseOffset: 10,
		EndOffset:  12,
		Key:        segKey,
		CreatedAt:  time.Now(),
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
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1")},
	}
	seg1Msgs := []log.Message{
		{Offset: 2, Timestamp: time.Now().UnixMilli(), Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 3, Timestamp: time.Now().UnixMilli(), Key: []byte("k3"), Value: []byte("v3")},
	}

	seg0Key := "test-topic/0/0-0.segment"
	seg1Key := "test-topic/0/2-0.segment"
	putTestSealedSegment(t, s3Client, seg0Key, writeTestSegment(t, seg0Msgs))
	putTestSealedSegment(t, s3Client, seg1Key, writeTestSegment(t, seg1Msgs))

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
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1")},
	})
	seg1Data := writeTestSegment(t, []log.Message{
		{Offset: 2, Timestamp: time.Now().UnixMilli(), Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 3, Timestamp: time.Now().UnixMilli(), Key: []byte("k3"), Value: []byte("v3")},
	})

	putTestSealedSegment(t, s3Client, seg0Key, seg0Data)
	putTestSealedSegment(t, s3Client, seg1Key, seg1Data)

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
	if diskCache.Has(seg0Key) || diskCache.Has(seg1Key) {
		t.Fatal("segment payloads must not be cached")
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
	putTestSealedSegment(t, s3Client, seg0Key, writeTestSegment(t, []log.Message{
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1")},
	}))

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
