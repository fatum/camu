package server

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/parquet"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

func TestWriteParquetChunkIsReadableByDuckDB(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
		{Name: "optional_at", Type: "timestamp", Path: "$.optional_at", Nullable: true},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
	chunk, err := encodeParquetChunk([]log.Message{
		{Offset: 7, Timestamp: time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC).UnixMilli(), Key: []byte("key-7"), Value: []byte(`{"name":"alpha","count":7,"ratio":1.5,"enabled":true,"occurred_at":"2026-08-03T14:30:00+02:30","optional_at":"2026-08-03T12:30:00Z"}`)},
		{Offset: 8, Timestamp: time.Date(2026, time.August, 3, 12, 1, 0, 0, time.UTC).UnixMilli(), Key: []byte("key-8"), Value: []byte(`{"name":"beta","count":8,"ratio":2.5,"enabled":false,"occurred_at":"2026-08-03T14:31:00+02:30"}`)},
	}, schema)
	if err != nil {
		t.Fatalf("encodeParquetChunk() error = %v", err)
	}
	defer chunk.cleanup()
	path := chunk.file.Name()
	if chunk.size == 0 {
		t.Fatal("temporary Parquet file is empty")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open(duckdb) error = %v", err)
	}
	defer db.Close()

	var records, offset, count, occurredAtNanos, optionalAtNanos int64
	var name string
	var ratio float64
	var enabled, missingOptionalAt bool
	var note *string
	err = db.QueryRow(`SELECT count(*), min(record_offset), min(name), min("count"), min(ratio), bool_and(enabled), max(note), min(epoch_ns(occurred_at)), min(epoch_ns(optional_at)), bool_or(optional_at IS NULL) FROM read_parquet(?)`, path).Scan(&records, &offset, &name, &count, &ratio, &enabled, &note, &occurredAtNanos, &optionalAtNanos, &missingOptionalAt)
	if err != nil {
		t.Fatalf("read native parquet with DuckDB: %v", err)
	}
	if records != 2 || offset != 7 || name != "alpha" || count != 7 || ratio != 1.5 || enabled || note != nil {
		t.Fatalf("read parquet values = records=%d offset=%d name=%q count=%d ratio=%v enabled=%v note=%v", records, offset, name, count, ratio, enabled, note)
	}
	wantOccurredAt, err := time.Parse(time.RFC3339Nano, "2026-08-03T14:30:00+02:30")
	if err != nil {
		t.Fatal(err)
	}
	wantOptionalAt, err := time.Parse(time.RFC3339Nano, "2026-08-03T12:30:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if occurredAtNanos != wantOccurredAt.UTC().UnixNano() || optionalAtNanos != wantOptionalAt.UTC().UnixNano() || !missingOptionalAt {
		t.Fatalf("timestamp values = occurred_at=%d optional_at=%d missing_optional=%v", occurredAtNanos, optionalAtNanos, missingOptionalAt)
	}
}

func TestEncodeParquetChunkSeparatesSchemaFailuresFromValidRange(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	chunk, err := encodeParquetChunk([]log.Message{
		{Offset: 10, Timestamp: 10, Value: []byte(`{"id":10}`)},
		{Offset: 11, Timestamp: 11, Value: []byte(`{"id":"invalid"}`)},
		{Offset: 12, Timestamp: 12, Value: []byte(`{"id":12}`)},
	}, schema)
	if err != nil {
		t.Fatalf("encodeParquetChunk() error = %v", err)
	}
	defer chunk.cleanup()
	if chunk.records != 2 || chunk.start != 10 || chunk.end != 12 || chunk.startTS != 10 {
		t.Fatalf("encoded range = records=%d start=%d end=%d startTS=%d", chunk.records, chunk.start, chunk.end, chunk.startTS)
	}
	if len(chunk.failures) != 1 || chunk.failures[0].message.Offset != 11 {
		t.Fatalf("schema failures = %+v", chunk.failures)
	}
	if chunk.size == 0 {
		t.Fatal("encoded Parquet data is empty")
	}
}

func TestParquetChunkCleanupRemovesTemporaryFile(t *testing.T) {
	chunk, err := encodeParquetChunk([]log.Message{{Offset: 0, Value: []byte(`{"value":1}`)}}, nil)
	if err != nil {
		t.Fatalf("encodeParquetChunk() error = %v", err)
	}
	path := chunk.file.Name()
	chunk.cleanup()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary Parquet file remains after cleanup: %v", err)
	}
}

func TestPutImmutableParquetFileAcceptsEqualConflictAndRejectsDifferentFile(t *testing.T) {
	client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := encodeParquetChunk([]log.Message{{Offset: 0, Value: []byte(`{"value":"first"}`)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.cleanup()
	if err := putImmutableParquetFile(context.Background(), client, "parquet/events/file.parquet", first.file, first.size); err != nil {
		t.Fatalf("put immutable first file: %v", err)
	}
	if err := putImmutableParquetFile(context.Background(), client, "parquet/events/file.parquet", first.file, first.size); err != nil {
		t.Fatalf("put immutable equal retry: %v", err)
	}
	different, err := encodeParquetChunk([]log.Message{{Offset: 0, Value: []byte(`{"value":"different"}`)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer different.cleanup()
	if err := putImmutableParquetFile(context.Background(), client, "parquet/events/file.parquet", different.file, different.size); err == nil {
		t.Fatal("put immutable different retry succeeded")
	}
}

func TestParquetExportDLQFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	s, tc, identity, cp, _ := setupParquetExportPass(t)
	tc.Schema = &meta.TopicSchema{
		Encoding:        "json",
		DeadLetterTopic: "missing-dlq",
		Fields:          []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}},
	}

	// The source record has no id, so conversion creates a schema failure. Its
	// DLQ cannot be loaded; the Parquet checkpoint must remain untouched.
	s.runParquetExportPass(context.Background(), tc, identity, cp)
	if cp.NextOffset != 0 || cp.Generation != 0 {
		t.Fatalf("checkpoint advanced after DLQ failure: %+v", *cp)
	}
	if _, err := s.s3Client.Get(context.Background(), pipeline.CheckpointKey(parquetPipelineName, tc.Name, identity.Partition)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("checkpoint was persisted after DLQ failure: %v", err)
	}
}

type manifestConflictObjectStore struct {
	parquet.ObjectStore
	mu      sync.Mutex
	puts    int
	onFirst func()
}

func (s *manifestConflictObjectStore) ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error) {
	s.mu.Lock()
	s.puts++
	first := s.puts == 1
	s.mu.Unlock()
	if first && s.onFirst != nil {
		s.onFirst()
	}
	return "", parquet.ErrConflict
}

func (s *manifestConflictObjectStore) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

func setupParquetExportPass(t *testing.T) (*Server, meta.TopicConfig, PartitionIdentity, *pipeline.Checkpoint, time.Time) {
	t.Helper()
	s := newTestServer(t)
	tc := meta.TopicConfig{Name: "events", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, ExportEnabled: true}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}
	created := time.Date(2026, time.August, 2, 15, 0, 0, 0, time.UTC)
	key := "events/0/0-0.segment"
	batch := log.EncodeRecordBatch(0, []log.Message{{Offset: 0, Value: []byte(`{"value":1}`), Timestamp: created.UnixMilli()}})
	if err := s.partitionManager.GetDiskCache().Put(key, batch); err != nil {
		t.Fatalf("cache segment: %v", err)
	}
	if err := s.s3Client.Put(context.Background(), key, batch, storage.PutOpts{}); err != nil {
		t.Fatalf("store segment: %v", err)
	}
	var sidecar bytes.Buffer
	entry := log.IndexEntry{BaseOffset: 0, LastOffset: 0, Position: 0, BatchSize: int32(len(batch)), FirstTimestamp: created.UnixMilli(), MaxTimestamp: created.UnixMilli()}
	if err := log.WriteSidecar(&sidecar, []log.IndexEntry{entry}, nil); err != nil {
		t.Fatalf("write segment sidecar: %v", err)
	}
	if err := s.partitionManager.GetDiskCache().Put(log.SegmentOffsetIndexKey(key), sidecar.Bytes()); err != nil {
		t.Fatalf("cache segment sidecar: %v", err)
	}
	ps := s.partitionManager.GetPartitionState(tc.Name, 0)
	ps.mu.Lock()
	ps.index.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 0, Key: key, CreatedAt: created})
	ps.index.SetHighWatermark(1)
	ps.mu.Unlock()
	s.assignmentsMu.Lock()
	s.myPartitions = map[string]map[int]localPartitionAssignment{tc.Name: {0: {Owned: true, LeaderEpoch: 1}}}
	s.assignmentsMu.Unlock()
	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, LeaderEpoch: 1}
	cp := &pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: parquetPipelineName, SinkVersion: parquetPipelineVersion}
	return s, tc, identity, cp, created
}

func TestParquetExportPersistentManifestConflictsDoNotAdvanceCheckpoint(t *testing.T) {
	s, tc, identity, cp, _ := setupParquetExportPass(t)
	objects := &manifestConflictObjectStore{ObjectStore: parquetObjectAdapter{client: s.s3Client}}
	s.parquetStoreFactory = func() *parquet.Store { return parquet.NewStore(objects, parquet.NoFencer{}) }

	s.runParquetExportPass(context.Background(), tc, identity, cp)
	if cp.NextOffset != 0 || cp.Generation != 0 {
		t.Fatalf("checkpoint advanced after persistent manifest conflicts: %+v", *cp)
	}
	if objects.putCount() != 6 {
		t.Fatalf("manifest conditional puts = %d, want 6 bounded attempts", objects.putCount())
	}
	if _, err := s.getParquetManifest(context.Background(), tc.Name, 0, time.Date(2026, time.August, 2, 15, 0, 0, 0, time.UTC)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("manifest was published after persistent conflicts: %v", err)
	}
	if _, err := s.s3Client.Get(context.Background(), pipeline.CheckpointKey(parquetPipelineName, tc.Name, 0)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("checkpoint was published after manifest conflicts: %v", err)
	}
}

func TestParquetExportLeadershipLossDuringManifestRetryFencesOldEpoch(t *testing.T) {
	s, tc, identity, cp, created := setupParquetExportPass(t)
	objects := &manifestConflictObjectStore{ObjectStore: parquetObjectAdapter{client: s.s3Client}}
	objects.onFirst = func() {
		s.assignmentsMu.Lock()
		s.myPartitions[tc.Name][0] = localPartitionAssignment{Owned: true, LeaderEpoch: identity.LeaderEpoch + 1}
		s.assignmentsMu.Unlock()
	}
	s.parquetStoreFactory = func() *parquet.Store { return parquet.NewStore(objects, parquet.NoFencer{}) }

	s.runParquetExportPass(context.Background(), tc, identity, cp)
	if cp.NextOffset != 0 || cp.Generation != 0 {
		t.Fatalf("old epoch advanced checkpoint after fencing: %+v", *cp)
	}
	if objects.putCount() != 1 {
		t.Fatalf("old epoch attempted %d manifest writes after first conflict, want 1", objects.putCount())
	}
	if _, err := s.getParquetManifest(context.Background(), tc.Name, 0, created); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old epoch published a manifest after fencing: %v", err)
	}
	if _, err := s.s3Client.Get(context.Background(), pipeline.CheckpointKey(parquetPipelineName, tc.Name, 0)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old epoch published checkpoint after fencing: %v", err)
	}
}

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

func TestParquetSinkFailureStage(t *testing.T) {
	cause := errors.New("object store unavailable")
	err := parquetSinkError(parquetSinkStageObjectUpload, cause)
	if got := parquetSinkFailureStage(err); got != parquetSinkStageObjectUpload {
		t.Fatalf("sink failure stage = %q, want %q", got, parquetSinkStageObjectUpload)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("sink failure does not unwrap cause: %v", err)
	}
	if got := parquetSinkFailureStage(errors.New("unclassified")); got != "unknown" {
		t.Fatalf("unclassified sink failure stage = %q, want unknown", got)
	}
}

func TestParquetManifestErrorDetails(t *testing.T) {
	cause := &parquet.ManifestCASConflictError{Key: "_meta/parquet_manifests/events/dt=2026-08-02/hour=13/part-0.json", Attempts: 6}
	category, key, attempts := parquetManifestErrorDetails(parquetSinkError(parquetSinkStageManifestPublish, cause))
	if category != "cas_conflict_exhausted" || key != cause.Key || attempts != cause.Attempts {
		t.Fatalf("manifest details = %q, %q, %d", category, key, attempts)
	}
	category, key, attempts = parquetManifestErrorDetails(errors.New("backend unavailable"))
	if category != "manifest_write_error" || key != "" || attempts != 0 {
		t.Fatalf("generic manifest details = %q, %q, %d", category, key, attempts)
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

func TestParquetExportManifestConflictRetainsObjectForSuccessor(t *testing.T) {
	s, tc, identity, cp, ingestTime := setupParquetExportPass(t)
	conflicts := &manifestConflictObjectStore{ObjectStore: parquetObjectAdapter{client: s.s3Client}}
	s.parquetStoreFactory = func() *parquet.Store { return parquet.NewStore(conflicts, parquet.NoFencer{}) }

	s.runParquetExportPass(context.Background(), tc, identity, cp)
	objectKey := parquetPipelineObjectKey(tc.Name, identity.Partition, ingestTime, 0, 0)
	if _, err := s.s3Client.Get(context.Background(), objectKey); err != nil {
		t.Fatalf("manifest-conflicted upload was deleted: %v", err)
	}

	// A successor must be able to reuse the retained deterministic object and
	// publish it without racing a cleanup from the old epoch.
	successor := parquet.NewStore(parquetObjectAdapter{client: s.s3Client}, parquet.NoFencer{})
	date, hour := parquet.BucketDateHour(ingestTime)
	if _, err := successor.ReplaceOverlappingEntries(context.Background(), tc.Name, identity.Partition, date, hour, []parquet.Entry{{ObjectKey: objectKey, BaseOffset: 0, EndOffset: 0, SchemaVersion: 1, SourceKey: "pipeline", SourceEpoch: identity.LeaderEpoch + 1}}); err != nil {
		t.Fatalf("successor publish retained object: %v", err)
	}
	if _, err := s.s3Client.Get(context.Background(), objectKey); err != nil {
		t.Fatalf("successor manifest references deleted object: %v", err)
	}
}
