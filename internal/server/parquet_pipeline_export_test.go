package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/parquet"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

func TestParquetPipelineObjectKeyIsDeterministic(t *testing.T) {
	ts := time.UnixMilli(1710000000000).UTC()
	keys := make([]string, 0, 3)
	for _, epoch := range []uint64{7, 8, 9} {
		_ = epoch // epoch is intentionally excluded from the object identity.
		keys = append(keys, parquetPipelineObjectKey("events", 2, ts, 10, 19))
	}
	if keys[0] == "" || keys[0] != keys[1] || keys[1] != keys[2] {
		t.Fatalf("object keys across epochs = %v, want identical non-empty keys", keys)
	}
}

func TestParquetExportIngestTimeUsesStableSegmentMetadata(t *testing.T) {
	created := time.Date(2026, time.July, 31, 12, 34, 0, 0, time.UTC)
	index := log.NewIndex()
	index.Add(log.SegmentRef{BaseOffset: 10, EndOffset: 19, CreatedAt: created})
	got := parquetExportIngestTime(index, 10, 0)
	if !got.Equal(created) {
		t.Fatalf("ingest time = %v, want segment creation time %v", got, created)
	}
	first := parquetPipelineObjectKey("events", 2, got, 10, 19)
	second := parquetPipelineObjectKey("events", 2, parquetExportIngestTime(index, 10, 0), 10, 19)
	if first != second {
		t.Fatalf("retry object key changed: %q != %q", first, second)
	}
}

func TestParquetExportIngestTimeZeroTimestampIsDeterministicWithoutMetadata(t *testing.T) {
	index := log.NewIndex()
	first := parquetExportIngestTime(index, 0, 0)
	second := parquetExportIngestTime(index, 0, 0)
	if !first.Equal(time.Unix(0, 0).UTC()) || !first.Equal(second) {
		t.Fatalf("zero timestamp fallback = %v, %v; want stable Unix epoch", first, second)
	}
}

func TestParquetExportIngestTimeUsesIndexSnapshotAfterSegmentReplacement(t *testing.T) {
	index := log.NewIndex()
	first := time.Date(2026, time.July, 31, 12, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	index.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 9, CreatedAt: first})
	snapshot := index.Clone()
	index.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 9, CreatedAt: second})
	if got := parquetExportIngestTime(snapshot, 0, 0); !got.Equal(first) {
		t.Fatalf("snapshot ingest time = %v, want %v", got, first)
	}
	if got := parquetExportIngestTime(index, 0, 0); !got.Equal(second) {
		t.Fatalf("live ingest time = %v, want %v", got, second)
	}
}

func TestParquetPendingExportPreservesBucketAcrossRetries(t *testing.T) {
	client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{s3Client: client}
	ctx := context.Background()
	key := parquetPendingExportKey("events", 0, 0, 9)
	first := time.Date(2026, time.July, 31, 12, 0, 0, 0, time.UTC)
	if got, err := s.loadOrCreateParquetPendingExport(ctx, key, first); err != nil || !got.Equal(first) {
		t.Fatalf("create pending metadata = %v, %v", got, err)
	}
	second := first.Add(time.Hour)
	got, err := s.loadOrCreateParquetPendingExport(ctx, key, second)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(first) {
		t.Fatalf("retry bucket = %v, want original %v", got, first)
	}
}

func TestParquetPipelineCommittedRangeBound(t *testing.T) {
	if err := pipeline.ValidateCommittedRange(10, 19, 20); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.ValidateCommittedRange(10, 20, 20); err == nil {
		t.Fatal("range at high watermark was accepted")
	}
}

func TestParquetPipelineCleanupPreservesReferencedObject(t *testing.T) {
	ctx := context.Background()
	client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	store := parquet.NewStore(parquetObjectAdapter{client: client}, parquet.NoFencer{})
	key := "data/events/part-0.parquet"
	if err := client.Put(ctx, key, []byte("data"), storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	ts := time.UnixMilli(1710000000000).UTC()
	date, hour := parquet.BucketDateHour(ts)
	if _, err := store.ReplaceOverlappingEntries(ctx, "events", 0, date, hour, []parquet.Entry{{ObjectKey: key, BaseOffset: 0, EndOffset: 1, SourceKey: "pipeline", SourceEpoch: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := cleanupUnreferencedParquetUpload(ctx, store, client, "events", 0, ts, key, errors.New("manifest error")); err == nil || err.Error() != "manifest error" {
		t.Fatalf("cleanup result = %v, want original error", err)
	}
	if _, err := client.Get(ctx, key); err != nil {
		t.Fatalf("referenced object was deleted: %v", err)
	}
}
