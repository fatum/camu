package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

// TestReadDisklessCommittedBatchBoundedToCapturedWatermark verifies that a
// concurrent commit between the caller's GetCommittedHead and the engine's
// Fetch never exports offsets at or above the captured watermark, which would
// duplicate records on the next pass.
func TestReadDisklessCommittedBatchBoundedToCapturedWatermark(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
	defer s.disklessEngine.Close()

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true}
	raw := log.EncodeRecordBatch(0, []log.Message{
		{Value: []byte(`{"id":0}`)},
		{Value: []byte(`{"id":1}`)},
		{Value: []byte(`{"id":2}`)},
	})
	if _, err := s.disklessEngine.Produce(ctx, tc.Name, 0, raw); err != nil {
		t.Fatalf("diskless produce: %v", err)
	}
	committed, err := s.disklessMeta.GetCommittedHead(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("committed head: %v", err)
	}
	if committed != 3 {
		t.Fatalf("committed head = %d, want 3", committed)
	}

	// Call with a captured watermark of 2, simulating a record that committed
	// after the exporter captured its watermark.
	msgs, next, err := s.readDisklessCommittedBatch(ctx, tc, 0, 0, 2, 100)
	if err != nil {
		t.Fatalf("readDisklessCommittedBatch: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages = %d, want 2 (offset 2 must be deferred to the next pass)", len(msgs))
	}
	if msgs[0].Offset != 0 || msgs[1].Offset != 1 {
		t.Fatalf("message offsets = [%d %d], want [0 1]", msgs[0].Offset, msgs[1].Offset)
	}
	if next != 2 {
		t.Fatalf("next = %d, want 2 (bounded to the captured watermark)", next)
	}
}

func TestDisklessExportPass(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
	defer s.disklessEngine.Close()

	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	tc := meta.TopicConfig{Name: "disk-orders", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true, Schema: schema}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, tc.Name, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	// Produce two committed records through the diskless engine.
	raw := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k1"), Value: []byte(`{"id":7}`)},
		{Key: []byte("k2"), Value: []byte(`{"id":8}`)},
	})
	if _, err := s.disklessEngine.Produce(ctx, tc.Name, 0, raw); err != nil {
		t.Fatalf("diskless produce: %v", err)
	}
	committed, err := s.disklessMeta.GetCommittedHead(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("committed head: %v", err)
	}
	if committed != 2 {
		t.Fatalf("committed head = %d, want 2", committed)
	}

	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	cp := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: parquetPipelineName, SinkVersion: parquetPipelineVersion}
	s.runParquetExportPass(ctx, tc, identity, &cp)
	if cp.NextOffset != 2 {
		t.Fatalf("checkpoint next offset = %d, want 2", cp.NextOffset)
	}

	// The checkpoint must be durable and the exported object must exist.
	store := pipeline.NewCheckpointStore(s.s3Client, serverPipelineFence{server: s})
	durable, err := store.Load(ctx, parquetPipelineName, tc.Name, 0)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if durable.NextOffset != 2 {
		t.Fatalf("durable checkpoint next offset = %d, want 2", durable.NextOffset)
	}
	ingestTime := time.Unix(0, 0).UTC() // diskless records carry no ingest segment; fallback epoch
	objectKey := iceberg.ExportObjectKey(tc.Name, 0, ingestTime, 0, 1, 1, "pipeline")
	if _, err := s.s3Client.Get(ctx, objectKey); err != nil {
		t.Fatalf("expected exported parquet object %s: %v", objectKey, err)
	}
}

func TestEncodeParquetChunkSeparatesSchemaFailuresFromValidRange(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	chunk, err := iceberg.EncodeChunk("", []log.Message{
		{Offset: 10, Timestamp: 10, Value: []byte(`{"id":10}`)},
		{Offset: 11, Timestamp: 11, Value: []byte(`{"id":"invalid"}`)},
		{Offset: 12, Timestamp: 12, Value: []byte(`{"id":12}`)},
	}, schema)
	if err != nil {
		t.Fatalf("iceberg.EncodeChunk() error = %v", err)
	}
	defer chunk.Cleanup()
	if chunk.Records != 2 || chunk.Start != 10 || chunk.End != 12 || chunk.StartTS != 10 {
		t.Fatalf("encoded range = records=%d start=%d end=%d startTS=%d", chunk.Records, chunk.Start, chunk.End, chunk.StartTS)
	}
	if len(chunk.Failures) != 1 || chunk.Failures[0].Message.Offset != 11 {
		t.Fatalf("schema failures = %+v", chunk.Failures)
	}
	if chunk.Size == 0 {
		t.Fatal("encoded Parquet data is empty")
	}
}

func TestParquetChunkCleanupRemovesTemporaryFile(t *testing.T) {
	chunk, err := iceberg.EncodeChunk("", []log.Message{{Offset: 0, Value: []byte(`{"value":1}`)}}, nil)
	if err != nil {
		t.Fatalf("iceberg.EncodeChunk() error = %v", err)
	}
	path := chunk.File.Name()
	chunk.Cleanup()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary Parquet file remains after cleanup: %v", err)
	}
}

func TestServerEncodeParquetChunkUsesConfiguredTempDirectory(t *testing.T) {
	s := newTestServer(t)
	chunk, err := s.encodeParquetChunk([]log.Message{{Offset: 0, Value: []byte(`{"value":1}`)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer chunk.Cleanup()
	if filepath.Dir(chunk.File.Name()) != s.cfg.Maintenance.ParquetExport.TempDirectoryValue() {
		t.Fatalf("chunk directory = %s, want %s", filepath.Dir(chunk.File.Name()), s.cfg.Maintenance.ParquetExport.TempDirectoryValue())
	}
}

func TestPutImmutableParquetFileAcceptsEqualConflictAndRejectsDifferentFile(t *testing.T) {
	client, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := iceberg.EncodeChunk("", []log.Message{{Offset: 0, Value: []byte(`{"value":"first"}`)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Cleanup()
	if err := putImmutableParquetFile(context.Background(), client, "parquet/events/file.parquet", first.File, first.Size); err != nil {
		t.Fatalf("put immutable first file: %v", err)
	}
	if err := putImmutableParquetFile(context.Background(), client, "parquet/events/file.parquet", first.File, first.Size); err != nil {
		t.Fatalf("put immutable equal retry: %v", err)
	}
	different, err := iceberg.EncodeChunk("", []log.Message{{Offset: 0, Value: []byte(`{"value":"different"}`)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer different.Cleanup()
	if err := putImmutableParquetFile(context.Background(), client, "parquet/events/file.parquet", different.File, different.Size); err == nil {
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
	iceberg.ObjectStore
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
	return "", iceberg.ErrConflict
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
	s.parquetStoreFactory = func() *iceberg.Store { return iceberg.NewStore(objects, iceberg.NoFencer{}) }

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
	s.parquetStoreFactory = func() *iceberg.Store { return iceberg.NewStore(objects, iceberg.NoFencer{}) }

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
	cause := &iceberg.ManifestCASConflictError{Key: "_meta/parquet_manifests/events/dt=2026-08-02/hour=13/part-0.json", Attempts: 6}
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
	s.parquetStoreFactory = func() *iceberg.Store { return iceberg.NewStore(conflicts, iceberg.NoFencer{}) }

	s.runParquetExportPass(context.Background(), tc, identity, cp)
	objectKey := parquetPipelineObjectKey(tc.Name, identity.Partition, ingestTime, 0, 0)
	if _, err := s.s3Client.Get(context.Background(), objectKey); err != nil {
		t.Fatalf("manifest-conflicted upload was deleted: %v", err)
	}

	// A successor must be able to reuse the retained deterministic object and
	// publish it without racing a cleanup from the old epoch.
	successor := iceberg.NewStore(parquetObjectAdapter{client: s.s3Client}, iceberg.NoFencer{})
	date, hour := iceberg.BucketDateHour(ingestTime)
	if _, err := successor.ReplaceOverlappingEntries(context.Background(), tc.Name, identity.Partition, date, hour, []iceberg.Entry{{ObjectKey: objectKey, BaseOffset: 0, EndOffset: 0, SchemaVersion: 1, SourceKey: "pipeline", SourceEpoch: identity.LeaderEpoch + 1}}); err != nil {
		t.Fatalf("successor publish retained object: %v", err)
	}
	if _, err := s.s3Client.Get(context.Background(), objectKey); err != nil {
		t.Fatalf("successor manifest references deleted object: %v", err)
	}
}
