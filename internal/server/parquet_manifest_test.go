package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

func TestParquetPathHelpersDeterministic(t *testing.T) {
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)

	gotObject := parquetExportObjectKey("events", 7, ts, 12000, 12999, 1, "events/7/12000-12999-1.segment|epoch=1")
	if !strings.HasPrefix(gotObject, "parquet/dt=2026-04-11/topic=events/hour=13/") || !strings.HasSuffix(gotObject, ".parquet") {
		t.Fatalf("parquetExportObjectKey() = %q, want analytics path layout", gotObject)
	}
	if again := parquetExportObjectKey("events", 7, ts, 12000, 12999, 1, "events/7/12000-12999-1.segment|epoch=1"); gotObject != again {
		t.Fatalf("parquetExportObjectKey() not deterministic: %q != %q", gotObject, again)
	}
	if got := parquetExportObjectKey("events", 7, ts, 12000, 12999, 1, "events/7/00000000000000012000-12999-1.segment"); got == parquetExportObjectKey("events", 7, ts, 12000, 12999, 1, "events/7/00000000000000012000-12999-2.segment") {
		t.Fatal("parquetExportObjectKey() must differ for distinct immutable source segments")
	}

	gotManifest := parquetManifestKey("events", 7, ts)
	wantManifest := "_meta/parquet_manifests/events/dt=2026-04-11/hour=13/part-7.json"
	if gotManifest != wantManifest {
		t.Fatalf("parquetManifestKey() = %q, want %q", gotManifest, wantManifest)
	}

}

func TestParquetManifestRoundTrip(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)

	manifest := ParquetManifest{
		Topic:         "events",
		Partition:     3,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
		Entries: []ParquetManifestEntry{
			{ObjectKey: parquetExportObjectKey("events", 3, ts, 10, 19, 1, "events/3/10-19-1.segment|epoch=1"), BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "events/3/10-19-1.segment", SourceEpoch: 1},
		},
	}
	if _, err := s.publishParquetManifest(ctx, manifest); err != nil {
		t.Fatalf("publishParquetManifest() error = %v", err)
	}

	got, err := s.getParquetManifest(ctx, "events", 3, ts)
	if err != nil {
		t.Fatalf("getParquetManifest() error = %v", err)
	}
	if got.Generation != 1 || got.Topic != manifest.Topic || got.Partition != manifest.Partition || got.Date != manifest.Date || got.Hour != manifest.Hour || got.SchemaVersion != manifest.SchemaVersion {
		t.Fatalf("manifest round-trip mismatch: got %+v want %+v", got, manifest)
	}
	if len(got.Entries) != 1 || got.Entries[0] != manifest.Entries[0] {
		t.Fatalf("manifest entries = %+v, want %+v", got.Entries, manifest.Entries)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("manifest UpdatedAt is zero, want populated")
	}
}

func TestPublishParquetManifestRequiresBucket(t *testing.T) {
	s := newTestServer(t)
	_, err := s.publishParquetManifest(context.Background(), ParquetManifest{
		Topic:     "events",
		Partition: 1,
	})
	if err == nil {
		t.Fatal("publishParquetManifest() error = nil, want bucket validation error")
	}
}

func TestPublishParquetManifestIncrementsGeneration(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)

	manifest := ParquetManifest{
		Topic:         "events",
		Partition:     0,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
		Entries: []ParquetManifestEntry{
			{ObjectKey: parquetExportObjectKey("events", 0, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1},
		},
	}

	published, err := s.publishParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("publishParquetManifest(first) error = %v", err)
	}
	if published.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", published.Generation)
	}

	manifest.Entries = append(manifest.Entries, ParquetManifestEntry{ObjectKey: parquetExportObjectKey("events", 0, ts, 10, 19, 1, "events/0/10-19-1.segment|epoch=1"), BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "events/0/10-19-1.segment", SourceEpoch: 1})
	published, err = s.publishParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("publishParquetManifest(second) error = %v", err)
	}
	if published.Generation != 2 {
		t.Fatalf("second generation = %d, want 2", published.Generation)
	}
}

func TestPublishParquetManifestFencedByTopicDeletion(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{Name: "events", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now()}
	if err := s.putTopicDeletion(ctx, topicDeletionRecord{Topic: tc, StartedAt: time.Now()}); err != nil {
		t.Fatalf("putTopicDeletion() error = %v", err)
	}

	_, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic:         "events",
		Partition:     0,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
	})
	if !errors.Is(err, errParquetPublishFenced) {
		t.Fatalf("publishParquetManifest() error = %v, want %v", err, errParquetPublishFenced)
	}
}

func TestResolveParquetEntriesFromManifest(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)

	manifest := ParquetManifest{
		Topic:         "events",
		Partition:     2,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
		Entries: []ParquetManifestEntry{
			{ObjectKey: parquetExportObjectKey("events", 2, ts, 20, 29, 1, "events/2/20-29-1.segment|epoch=1"), BaseOffset: 20, EndOffset: 29, SchemaVersion: 1, SourceKey: "events/2/20-29-1.segment", SourceEpoch: 1},
		},
	}
	if _, err := s.publishParquetManifest(ctx, manifest); err != nil {
		t.Fatalf("publishParquetManifest() error = %v", err)
	}

	entries, gotManifest, err := s.resolveParquetEntriesFromManifest(ctx, "events", 2, ts)
	if err != nil {
		t.Fatalf("resolveParquetEntriesFromManifest() error = %v", err)
	}
	if len(entries) != 1 || entries[0] != manifest.Entries[0] {
		t.Fatalf("entries = %+v, want %+v", entries, manifest.Entries)
	}
	if gotManifest.Generation != 1 {
		t.Fatalf("manifest generation = %d, want 1", gotManifest.Generation)
	}
}

func TestPublishParquetManifestIdempotentOnRetry(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)

	manifest := ParquetManifest{
		Topic:         "events",
		Partition:     0,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
		Entries: []ParquetManifestEntry{
			{ObjectKey: parquetExportObjectKey("events", 0, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1},
			{ObjectKey: parquetExportObjectKey("events", 0, ts, 10, 19, 1, "events/0/10-19-1.segment|epoch=1"), BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "events/0/10-19-1.segment", SourceEpoch: 1},
		},
	}

	first, err := s.publishParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("publishParquetManifest(first) error = %v", err)
	}
	if first.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", first.Generation)
	}

	// Simulate uploader retry after a crash between upload and checkpoint
	// advance: the same entries are republished. Generation must not bump.
	retry, err := s.publishParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("publishParquetManifest(retry) error = %v", err)
	}
	if retry.Generation != 1 {
		t.Fatalf("retry generation = %d, want 1 (idempotent)", retry.Generation)
	}

	// A subset retry (one entry missing from the incoming manifest, but
	// already published) should also no-op.
	subset := manifest
	subset.Entries = manifest.Entries[:1]
	sub, err := s.publishParquetManifest(ctx, subset)
	if err != nil {
		t.Fatalf("publishParquetManifest(subset) error = %v", err)
	}
	if sub.Generation != 1 {
		t.Fatalf("subset generation = %d, want 1 (idempotent)", sub.Generation)
	}

	// A genuinely new entry must still bump.
	extended := manifest
	extended.Entries = append(extended.Entries, ParquetManifestEntry{
		ObjectKey: parquetExportObjectKey("events", 0, ts, 20, 29, 1, "events/0/20-29-1.segment|epoch=1"), BaseOffset: 20, EndOffset: 29, SchemaVersion: 1, SourceKey: "events/0/20-29-1.segment", SourceEpoch: 1,
	})
	bumped, err := s.publishParquetManifest(ctx, extended)
	if err != nil {
		t.Fatalf("publishParquetManifest(extended) error = %v", err)
	}
	if bumped.Generation != 2 {
		t.Fatalf("extended generation = %d, want 2", bumped.Generation)
	}
}

func TestListParquetManifestsForTopicBoundaryInclusive(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	if _, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "23", SchemaVersion: 1,
	}); err != nil {
		t.Fatalf("publishParquetManifest error = %v", err)
	}

	// A query ending at T23:00:00Z should include the hour=23 bucket
	// because that bucket covers [23:00, 24:00).
	from := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 4, 11, 23, 0, 0, 0, time.UTC)
	manifests, err := s.listParquetManifestsForTopic(ctx, "events", from, to)
	if err != nil {
		t.Fatalf("listParquetManifestsForTopic error = %v", err)
	}
	if len(manifests) != 1 || manifests[0].Hour != "23" {
		t.Fatalf("manifests = %+v, want hour=23 bucket", manifests)
	}
}

func TestDeleteParquetTopicMetadataRemovesDataObjects(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Plant a Parquet data object under parquet/{topic}/...
	dataKey := parquetExportObjectKey("events", 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC), 0, 9, 1, "events/0/0-9-1.segment|epoch=1")
	if err := s.s3Client.Put(ctx, dataKey, []byte("stub"), storage.PutOpts{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("plant parquet data: %v", err)
	}
	if _, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []ParquetManifestEntry{{ObjectKey: dataKey, BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1}},
	}); err != nil {
		t.Fatalf("publishParquetManifest: %v", err)
	}

	if err := s.deleteParquetTopicMetadata(ctx, "events"); err != nil {
		t.Fatalf("deleteParquetTopicMetadata: %v", err)
	}

	if _, err := s.s3Client.Get(ctx, dataKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("parquet data object survived deletion: err = %v", err)
	}
	leftover, err := s.s3Client.List(ctx, parquetDataPrefix)
	if err != nil {
		t.Fatalf("list parquet data prefix: %v", err)
	}
	for _, key := range leftover {
		if strings.Contains(key, "/topic=events/") {
			t.Fatalf("parquet data objects leaked: %v", leftover)
		}
	}
}

func TestCompactParquetBucketIdempotent(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)

	// Start with three small files in one bucket.
	small := []ParquetManifestEntry{
		{ObjectKey: parquetExportObjectKey("events", 0, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1},
		{ObjectKey: parquetExportObjectKey("events", 0, ts, 10, 19, 1, "events/0/10-19-1.segment|epoch=1"), BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "events/0/10-19-1.segment", SourceEpoch: 1},
		{ObjectKey: parquetExportObjectKey("events", 0, ts, 20, 29, 1, "events/0/20-29-1.segment|epoch=1"), BaseOffset: 20, EndOffset: 29, SchemaVersion: 1, SourceKey: "events/0/20-29-1.segment", SourceEpoch: 1},
	}
	published, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1, Entries: small,
	})
	if err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	if published.Generation != 1 || len(published.Entries) != 3 {
		t.Fatalf("initial publish state = gen %d / %d entries, want 1/3", published.Generation, len(published.Entries))
	}

	// Compaction step: replace the three smalls with one big file.
	big := ParquetManifestEntry{
		ObjectKey: parquetExportObjectKey("events", 0, ts, 0, 29, 1, "events/0/0-29-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 29, SchemaVersion: 1, SourceKey: "compaction/events/0/0-29", SourceEpoch: 0,
	}
	removeKeys := []string{small[0].ObjectKey, small[1].ObjectKey, small[2].ObjectKey}
	compacted, err := s.compactParquetBucket(ctx, "events", 0, "2026-04-11", "13", removeKeys, []ParquetManifestEntry{big})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if compacted.Generation != 2 {
		t.Fatalf("compacted gen = %d, want 2", compacted.Generation)
	}
	if len(compacted.Entries) != 1 || compacted.Entries[0].ObjectKey != big.ObjectKey {
		t.Fatalf("compacted entries = %+v, want single big entry", compacted.Entries)
	}

	// Retry the same compaction after a simulated crash. Must be a no-op
	// — generation must NOT bump, state must not drift.
	retry, err := s.compactParquetBucket(ctx, "events", 0, "2026-04-11", "13", removeKeys, []ParquetManifestEntry{big})
	if err != nil {
		t.Fatalf("compact retry: %v", err)
	}
	if retry.Generation != 2 {
		t.Fatalf("retry gen = %d, want 2 (idempotent)", retry.Generation)
	}
	if len(retry.Entries) != 1 || retry.Entries[0].ObjectKey != big.ObjectKey {
		t.Fatalf("retry entries = %+v, want single big entry", retry.Entries)
	}

	// A second, genuinely new compaction still works normally.
	big2 := ParquetManifestEntry{
		ObjectKey: parquetExportObjectKey("events", 0, ts, 0, 29, 2, "events/0/0-29-2.segment|epoch=2"), BaseOffset: 0, EndOffset: 29, SchemaVersion: 2, SourceKey: "compaction/events/0/0-29-v2", SourceEpoch: 0,
	}
	advanced, err := s.compactParquetBucket(ctx, "events", 0, "2026-04-11", "13", []string{big.ObjectKey}, []ParquetManifestEntry{big2})
	if err != nil {
		t.Fatalf("second compact: %v", err)
	}
	if advanced.Generation != 3 {
		t.Fatalf("second compact gen = %d, want 3", advanced.Generation)
	}
	if advanced.SchemaVersion != 2 {
		t.Fatalf("schema version = %d, want 2", advanced.SchemaVersion)
	}
}

func TestCompactParquetBucketPartialCrashRecovery(t *testing.T) {
	// Simulates a crash mid-compaction: the new entry was published, one
	// of the three old entries was already removed from the manifest, but
	// the other two are still there. A retry must converge to "only big
	// remains" without error.
	s := newTestServer(t)
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)

	small := []ParquetManifestEntry{
		{ObjectKey: parquetExportObjectKey("events", 0, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1},
		{ObjectKey: parquetExportObjectKey("events", 0, ts, 10, 19, 1, "events/0/10-19-1.segment|epoch=1"), BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "events/0/10-19-1.segment", SourceEpoch: 1},
	}
	big := ParquetManifestEntry{
		ObjectKey: parquetExportObjectKey("events", 0, ts, 0, 19, 1, "events/0/0-19-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 19, SchemaVersion: 1, SourceKey: "compaction/events/0/0-19", SourceEpoch: 0,
	}
	// Planted post-crash state: small[0] is gone, small[1] still there, big present.
	if _, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []ParquetManifestEntry{small[1], big},
	}); err != nil {
		t.Fatalf("plant crash state: %v", err)
	}

	// Retry compaction with ORIGINAL removeKeys (caller doesn't know which
	// were already removed).
	removeKeys := []string{small[0].ObjectKey, small[1].ObjectKey}
	final, err := s.compactParquetBucket(ctx, "events", 0, "2026-04-11", "13", removeKeys, []ParquetManifestEntry{big})
	if err != nil {
		t.Fatalf("compact retry: %v", err)
	}
	if len(final.Entries) != 1 || final.Entries[0].ObjectKey != big.ObjectKey {
		t.Fatalf("final entries = %+v, want [big]", final.Entries)
	}
}

func TestListParquetManifestsForTopicFiltersByTime(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	m1 := ParquetManifest{Topic: "events", Partition: 0, Date: "2026-04-10", Hour: "12", SchemaVersion: 1}
	if _, err := s.publishParquetManifest(ctx, m1); err != nil {
		t.Fatalf("publishParquetManifest(m1) error = %v", err)
	}
	m2 := ParquetManifest{Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1}
	if _, err := s.publishParquetManifest(ctx, m2); err != nil {
		t.Fatalf("publishParquetManifest(m2) error = %v", err)
	}
	m3 := ParquetManifest{Topic: "other", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1}
	if _, err := s.publishParquetManifest(ctx, m3); err != nil {
		t.Fatalf("publishParquetManifest(m3) error = %v", err)
	}

	from := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 4, 11, 23, 0, 0, 0, time.UTC)
	manifests, err := s.listParquetManifestsForTopic(ctx, "events", from, to)
	if err != nil {
		t.Fatalf("listParquetManifestsForTopic() error = %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("len(manifests) = %d, want 1", len(manifests))
	}
	if manifests[0].Date != "2026-04-11" || manifests[0].Hour != "13" {
		t.Fatalf("manifest bucket = %s/%s, want 2026-04-11/13", manifests[0].Date, manifests[0].Hour)
	}
}
