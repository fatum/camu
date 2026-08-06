package server

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
)

// TestIcebergExportPass verifies the Iceberg sink end to end for a diskless
// topic: the pass commits a standard Iceberg snapshot whose manifest references
// the exported data file, and advances the iceberg-export checkpoint last.
func TestIcebergExportPass(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
	defer s.disklessEngine.Close()
	s.cfg.Maintenance.ParquetExport.Warehouse = "warehouse/"

	tc := meta.TopicConfig{Name: "orders", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true}
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
	cp := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion}
	s.runIcebergExportPass(ctx, tc, identity, &cp)
	if cp.NextOffset != 2 {
		t.Fatalf("checkpoint next offset = %d, want 2", cp.NextOffset)
	}

	// The iceberg-export checkpoint must be durable.
	store := pipeline.NewCheckpointStore(s.s3Client, serverPipelineFence{server: s})
	durable, err := store.Load(ctx, icebergPipelineName, tc.Name, 0)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if durable.NextOffset != 2 {
		t.Fatalf("durable checkpoint next offset = %d, want 2", durable.NextOffset)
	}

	// The Iceberg table must expose the exported data file through its current
	// snapshot's manifest.
	table := s.icebergTableStoreFor()
	loaded, err := table.Load(ctx, tc.Name)
	if err != nil {
		t.Fatalf("iceberg table load: %v", err)
	}
	if loaded.CurrentSnapshotID == nil {
		t.Fatal("iceberg table has no current snapshot")
	}
	files, err := table.CurrentDataFiles(ctx, tc.Name)
	if err != nil {
		t.Fatalf("CurrentDataFiles() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("data files = %d, want 1", len(files))
	}
	if _, err := s.s3Client.Get(ctx, files[0].FilePath); err != nil {
		t.Fatalf("exported data file %s missing: %v", files[0].FilePath, err)
	}
	if files[0].RecordCount != 2 || files[0].FileFormat != "PARQUET" || files[0].Content != 0 {
		t.Fatalf("data file = %+v, want 2 PARQUET DATA records", files[0])
	}
}

// TestIcebergExportPassIsIdempotentAcrossRetry verifies a re-run of the pass
// over the same committed range converges on the same snapshot instead of
// duplicating it (the checkpoint is reset to simulate a crash after the
// snapshot commit but before the checkpoint publish).
func TestIcebergExportPassIsIdempotentAcrossRetry(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
	defer s.disklessEngine.Close()

	tc := meta.TopicConfig{Name: "orders", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true}
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

	raw := log.EncodeRecordBatch(0, []log.Message{{Value: []byte(`{"id":1}`)}})
	if _, err := s.disklessEngine.Produce(ctx, tc.Name, 0, raw); err != nil {
		t.Fatalf("diskless produce: %v", err)
	}

	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	cp := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion}
	s.runIcebergExportPass(ctx, tc, identity, &cp)
	if cp.NextOffset != 1 {
		t.Fatalf("checkpoint next offset = %d, want 1", cp.NextOffset)
	}
	table := s.icebergTableStoreFor()
	first, err := table.Load(ctx, tc.Name)
	if err != nil {
		t.Fatalf("iceberg table load: %v", err)
	}

	// Simulate a crash before the checkpoint: re-run from offset 0.
	cp = pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion}
	s.runIcebergExportPass(ctx, tc, identity, &cp)
	if cp.NextOffset != 1 {
		t.Fatalf("retry checkpoint next offset = %d, want 1", cp.NextOffset)
	}
	second, err := table.Load(ctx, tc.Name)
	if err != nil {
		t.Fatalf("iceberg table reload: %v", err)
	}
	if len(second.Snapshots) != len(first.Snapshots) {
		t.Fatalf("snapshots after retry = %d, want %d (idempotent)", len(second.Snapshots), len(first.Snapshots))
	}
}

// TestIcebergExportPassBatchesRangesIntoOneSnapshot verifies that one pass
// buffers multiple source ranges (bounded per-read by max_records) into a
// single snapshot instead of committing one snapshot per range.
func TestIcebergExportPassBatchesRangesIntoOneSnapshot(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
	defer s.disklessEngine.Close()
	s.cfg.Maintenance.ParquetExport.MaxRecords = 1

	tc := meta.TopicConfig{Name: "orders", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true}
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

	for _, id := range []int{1, 2, 3} {
		raw := log.EncodeRecordBatch(0, []log.Message{{Value: []byte(`{"id":` + string(rune(0+id)) + `}`)}})
		if _, err := s.disklessEngine.Produce(ctx, tc.Name, 0, raw); err != nil {
			t.Fatalf("diskless produce: %v", err)
		}
	}
	committed, err := s.disklessMeta.GetCommittedHead(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("committed head: %v", err)
	}
	if committed != 3 {
		t.Fatalf("committed head = %d, want 3", committed)
	}

	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	cp := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion}
	s.runIcebergExportPass(ctx, tc, identity, &cp)
	if cp.NextOffset != 3 {
		t.Fatalf("checkpoint next offset = %d, want 3", cp.NextOffset)
	}

	table := s.icebergTableStoreFor()
	loaded, err := table.Load(ctx, tc.Name)
	if err != nil {
		t.Fatalf("iceberg table load: %v", err)
	}
	if len(loaded.Snapshots) != 1 {
		t.Fatalf("snapshots = %d, want 1 (one snapshot covering all ranges)", len(loaded.Snapshots))
	}
	files, err := table.CurrentDataFiles(ctx, tc.Name)
	if err != nil {
		t.Fatalf("CurrentDataFiles() error = %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("data files = %d, want 3 (one per read range)", len(files))
	}
}

// TestIcebergExportPassAvroValue verifies an avro-encoded topic value flows
// through the export pipeline into the Iceberg table: the committed raw Avro
// bytes are decoded into typed columns and exported.
func TestIcebergExportPassAvroValue(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
	defer s.disklessEngine.Close()
	s.cfg.Maintenance.ParquetExport.Warehouse = "warehouse/"

	avroSchema := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	tc := meta.TopicConfig{Name: "orders", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true, Schema: avroSchema}
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

	value, err := iceberg.EncodeAvroValue(avroSchema, map[string]any{"id": int64(7)})
	if err != nil {
		t.Fatalf("EncodeAvroValue: %v", err)
	}
	raw := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k1"), Value: value}})
	if _, err := s.disklessEngine.Produce(ctx, tc.Name, 0, raw); err != nil {
		t.Fatalf("diskless produce: %v", err)
	}

	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	cp := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion}
	s.runIcebergExportPass(ctx, tc, identity, &cp)
	if cp.NextOffset != 1 {
		t.Fatalf("checkpoint next offset = %d, want 1", cp.NextOffset)
	}
	table := s.icebergTableStoreFor()
	files, err := table.CurrentDataFiles(ctx, tc.Name)
	if err != nil {
		t.Fatalf("CurrentDataFiles() error = %v", err)
	}
	if len(files) != 1 || files[0].RecordCount != 1 {
		t.Fatalf("data files = %+v, want 1 file with 1 avro record", files)
	}
	if err := table.ValidateTable(ctx, tc.Name); err != nil {
		t.Fatalf("ValidateTable() error = %v", err)
	}
}

// TestIcebergExportPassAvroSchemaEvolution verifies read-side evolution end to
// end: a value written under schema version 0 is exported correctly after the
// topic schema evolves to version 1 (a nullable field added), because the
// value's schema-id envelope resolves its writer schema.
func TestIcebergExportPassAvroSchemaEvolution(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
	defer s.disklessEngine.Close()
	s.cfg.Maintenance.ParquetExport.Warehouse = "warehouse/"

	v0 := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	tc := meta.TopicConfig{Name: "orders", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true, Schema: v0}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if _, err := s.schemaRegistry.RegisterTopicSchema(ctx, tc.Name, v0); err != nil {
		t.Fatalf("RegisterTopicSchema() error = %v", err)
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

	payload, err := iceberg.EncodeAvroValue(v0, map[string]any{"id": int64(7)})
	if err != nil {
		t.Fatalf("EncodeAvroValue: %v", err)
	}
	wrapped := iceberg.AvroWrap(0, payload)
	raw := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k1"), Value: wrapped}})
	if _, err := s.disklessEngine.Produce(ctx, tc.Name, 0, raw); err != nil {
		t.Fatalf("diskless produce: %v", err)
	}

	// Evolve the projection: add a nullable field.
	v1 := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
	if _, err := s.schemaRegistry.RegisterSchemaVersion(ctx, tc.Name, v1); err != nil {
		t.Fatalf("RegisterSchemaVersion() error = %v", err)
	}
	tc.Schema = v1

	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	cp := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion}
	s.runIcebergExportPass(ctx, tc, identity, &cp)
	if cp.NextOffset != 1 {
		t.Fatalf("checkpoint next offset = %d, want 1", cp.NextOffset)
	}
	table := s.icebergTableStoreFor()
	files, err := table.CurrentDataFiles(ctx, tc.Name)
	if err != nil {
		t.Fatalf("CurrentDataFiles() error = %v", err)
	}
	if len(files) != 1 || files[0].RecordCount != 1 {
		t.Fatalf("data files = %+v, want 1 file with 1 record", files)
	}
}
