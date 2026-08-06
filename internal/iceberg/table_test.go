package iceberg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/maksim/camu/internal/meta"
)

func newTestTableStore() *TableStore {
	return NewTableStore(newFakeObjectStore(), nil, "warehouse/")
}

func TestTableCreateLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
	created, err := ts.Create(ctx, "events", schema)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.version != 0 {
		t.Fatalf("created version = %d, want 0", created.version)
	}
	if created.FormatVersion != tableFormatVersion {
		t.Fatalf("format-version = %d, want %d", created.FormatVersion, tableFormatVersion)
	}

	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.version != 0 {
		t.Fatalf("loaded version = %d, want 0", loaded.version)
	}
	if loaded.TableUUID != created.TableUUID {
		t.Fatalf("table-uuid changed across round trip: %s != %s", loaded.TableUUID, created.TableUUID)
	}
	icebergSchema := loaded.currentSchema()
	if icebergSchema == nil || len(icebergSchema.Fields) != 9 {
		t.Fatalf("schema fields = %d, want 9 (7 base + 2 typed)", len(icebergSchema.Fields))
	}
	if icebergSchema.Fields[0].Name != "record_offset" || icebergSchema.Fields[0].Type != "long" || !icebergSchema.Fields[0].Required {
		t.Fatalf("base column 0 = %+v, want required record_offset long", icebergSchema.Fields[0])
	}
	if icebergSchema.Fields[5].Name != "dt" || icebergSchema.Fields[5].Type != "string" || !icebergSchema.Fields[5].Required {
		t.Fatalf("partition column dt = %+v, want required string", icebergSchema.Fields[5])
	}
	if icebergSchema.Fields[6].Name != "hour" || icebergSchema.Fields[6].Type != "int" || !icebergSchema.Fields[6].Required {
		t.Fatalf("partition column hour = %+v, want required int", icebergSchema.Fields[6])
	}
	if icebergSchema.Fields[7].Name != "id" || icebergSchema.Fields[7].Type != "long" || !icebergSchema.Fields[7].Required {
		t.Fatalf("typed column id = %+v, want required long", icebergSchema.Fields[7])
	}
	if icebergSchema.Fields[8].Name != "note" || icebergSchema.Fields[8].Type != "string" || icebergSchema.Fields[8].Required {
		t.Fatalf("typed column note = %+v, want nullable string", icebergSchema.Fields[8])
	}
	if loaded.CurrentSnapshotID != nil {
		t.Fatalf("new table has current-snapshot-id = %v, want nil", *loaded.CurrentSnapshotID)
	}
	spec := loaded.currentPartitionSpec()
	if len(loaded.PartitionSpecs) != 1 || spec == nil || len(spec.Fields) != 2 {
		t.Fatalf("partition specs = %+v, want one dt/hour spec", loaded.PartitionSpecs)
	}
	if spec.Fields[0].Name != "dt" || spec.Fields[0].Transform != "identity" || spec.Fields[0].SourceID != 6 || spec.Fields[0].FieldID != 1000 {
		t.Fatalf("partition field 0 = %+v, want identity(dt) on column 6 field-id 1000", spec.Fields[0])
	}
	if spec.Fields[1].Name != "hour" || spec.Fields[1].Transform != "identity" || spec.Fields[1].SourceID != 7 || spec.Fields[1].FieldID != 1001 {
		t.Fatalf("partition field 1 = %+v, want identity(hour) on column 7 field-id 1001", spec.Fields[1])
	}
}

func TestTableLoadMissingReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	_, err := ts.Load(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load(missing) error = %v, want ErrNotFound", err)
	}
}

func TestTableCreateConflictsWhenExists(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	_, err := ts.Create(ctx, "events", nil)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second Create() error = %v, want ErrConflict", err)
	}
}

func TestTableAppendSnapshotAndReload(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	manifestList := "warehouse/events/metadata/snap-test.avro"
	snap, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor(manifestList), manifestList, SnapshotSummary{"added-data-files": "2"})
	if err != nil {
		t.Fatalf("AppendSnapshot() error = %v", err)
	}
	if snap.ManifestList != manifestList {
		t.Fatalf("snapshot manifest list = %q, want %q", snap.ManifestList, manifestList)
	}

	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.version != 1 {
		t.Fatalf("version after commit = %d, want 1", loaded.version)
	}
	if loaded.CurrentSnapshotID == nil || *loaded.CurrentSnapshotID != snap.SnapshotID {
		t.Fatalf("current-snapshot-id = %v, want %d", loaded.CurrentSnapshotID, snap.SnapshotID)
	}
	if len(loaded.Snapshots) != 1 || loaded.Snapshots[0].SnapshotID != snap.SnapshotID {
		t.Fatalf("snapshots = %+v, want [%d]", loaded.Snapshots, snap.SnapshotID)
	}
	if ref, ok := loaded.Refs["main"]; !ok || ref.SnapshotID != snap.SnapshotID {
		t.Fatalf("main ref = %+v, want snapshot %d", loaded.Refs["main"], snap.SnapshotID)
	}
}

func TestTableAppendSnapshotIsIdempotent(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	manifestList := "warehouse/events/metadata/snap-test.avro"
	first, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor(manifestList), manifestList, nil)
	if err != nil {
		t.Fatalf("first AppendSnapshot() error = %v", err)
	}
	second, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor(manifestList), manifestList, nil)
	if err != nil {
		t.Fatalf("retry AppendSnapshot() error = %v", err)
	}
	if second.SnapshotID != first.SnapshotID {
		t.Fatalf("retry snapshot id = %d, want %d", second.SnapshotID, first.SnapshotID)
	}
	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Snapshots) != 1 {
		t.Fatalf("snapshots after idempotent retry = %d, want 1", len(loaded.Snapshots))
	}
}

func TestTableAppendSnapshotRetriesCASConflict(t *testing.T) {
	ctx := context.Background()
	objects := newFakeObjectStore()
	ts := NewTableStore(objects, nil, "warehouse/")
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// The first version-hint CAS after the metadata write fails once; the
	// commit must reload the winner's metadata and retry at the next version.
	objects.injectConditionalPutConflict("warehouse/events/metadata/version-hint.text", 1)
	manifestList := "warehouse/events/metadata/snap-test.avro"
	snap, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor(manifestList), manifestList, nil)
	if err != nil {
		t.Fatalf("AppendSnapshot() after conflict error = %v", err)
	}
	if snap.SnapshotID != snapshotIDFor(manifestList) {
		t.Fatalf("snapshot id = %d, want %d", snap.SnapshotID, snapshotIDFor(manifestList))
	}
	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.version != 1 {
		t.Fatalf("version after retried commit = %d, want 1", loaded.version)
	}
	if len(loaded.Snapshots) != 1 {
		t.Fatalf("snapshots after retried commit = %d, want 1", len(loaded.Snapshots))
	}
}

func TestTableDeleteRemovesEverything(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor("warehouse/events/metadata/snap-test.avro"), "warehouse/events/metadata/snap-test.avro", nil); err != nil {
		t.Fatalf("AppendSnapshot() error = %v", err)
	}
	if err := ts.DeleteTable(ctx, "events"); err != nil {
		t.Fatalf("DeleteTable() error = %v", err)
	}
	if _, err := ts.Load(ctx, "events"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load after delete error = %v, want ErrNotFound", err)
	}
}

func TestSchemaFromTopicTypes(t *testing.T) {
	schema := SchemaFromTopic(&meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "s", Type: "string", Path: "$.s"},
		{Name: "i", Type: "int64", Path: "$.i"},
		{Name: "f", Type: "float64", Path: "$.f"},
		{Name: "b", Type: "bool", Path: "$.b"},
		{Name: "t", Type: "timestamp", Path: "$.t"},
	}})
	want := []string{"long", "long", "binary", "binary", "string", "string", "int", "string", "long", "double", "boolean", "timestamp_ns"}
	if len(schema.Fields) != len(want) {
		t.Fatalf("fields = %d, want %d", len(schema.Fields), len(want))
	}
	for i, f := range schema.Fields {
		if f.Type != want[i] {
			t.Fatalf("field %d (%s) type = %q, want %q", i, f.Name, f.Type, want[i])
		}
		if f.ID != i+1 {
			t.Fatalf("field %s id = %d, want %d", f.Name, f.ID, i+1)
		}
	}
}

func TestTableRejectsInvalidMetadataFile(t *testing.T) {
	ctx := context.Background()
	objects := newFakeObjectStore()
	ts := NewTableStore(objects, nil, "warehouse/")
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// Corrupt the current metadata file.
	for key := range objects.objects {
		if strings.HasSuffix(key, ".metadata.json") {
			objects.mu.Lock()
			objects.objects[key] = fakeObject{data: []byte("{not json"), etag: "x"}
			objects.mu.Unlock()
		}
	}
	if _, err := ts.Load(ctx, "events"); err == nil || !strings.Contains(err.Error(), "parse iceberg table metadata") {
		t.Fatalf("Load(corrupt) error = %v, want parse error", err)
	}
}

func TestTableCommitSnapshotWritesManifestAndList(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	files := []DataFile{
		{Content: DataFileContentData, FilePath: "warehouse/events/data/a.parquet", FileFormat: DataFileFormatParquet, DT: "2026-08-06", Hour: 13, RecordCount: 4, FileSizeBytes: 1000},
		{Content: DataFileContentData, FilePath: "warehouse/events/data/b.parquet", FileFormat: DataFileFormatParquet, DT: "2026-08-06", Hour: 14, RecordCount: 6, FileSizeBytes: 2000},
	}
	snap, err := ts.CommitSnapshot(ctx, "events", files)
	if err != nil {
		t.Fatalf("CommitSnapshot() error = %v", err)
	}
	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	head := loaded.currentSnapshot()
	if head == nil || head.SnapshotID != snap.SnapshotID || head.ManifestList == "" {
		t.Fatalf("head snapshot = %+v, want committed snapshot", head)
	}
	if head.SequenceNumber != 1 {
		t.Fatalf("head sequence number = %d, want 1", head.SequenceNumber)
	}

	// The manifest list references one manifest; the manifest references the
	// two data files.
	listData, err := ts.objects.Get(ctx, head.ManifestList)
	if err != nil {
		t.Fatalf("get manifest list: %v", err)
	}
	manifests, err := readManifestList(listData)
	if err != nil {
		t.Fatalf("readManifestList() error = %v", err)
	}
	if len(manifests) != 1 || manifests[0].ManifestPath == "" || manifests[0].AddedFilesCount != 2 || manifests[0].AddedRowsCount != 10 {
		t.Fatalf("manifest list = %+v, want 1 manifest with 2 files / 10 rows", manifests)
	}
	if len(manifests[0].Partitions) != 2 {
		t.Fatalf("partition summaries = %d, want 2 (dt, hour)", len(manifests[0].Partitions))
	}
	if string(manifests[0].Partitions[0].LowerBound) != "2026-08-06" || string(manifests[0].Partitions[0].UpperBound) != "2026-08-06" {
		t.Fatalf("dt summary bounds = %q..%q, want 2026-08-06..2026-08-06", manifests[0].Partitions[0].LowerBound, manifests[0].Partitions[0].UpperBound)
	}
	if !bytes.Equal(manifests[0].Partitions[1].LowerBound, []byte{26}) || !bytes.Equal(manifests[0].Partitions[1].UpperBound, []byte{28}) {
		t.Fatalf("hour summary bounds = %v..%v, want zigzag(13)..zigzag(14)", manifests[0].Partitions[1].LowerBound, manifests[0].Partitions[1].UpperBound)
	}
	manifestData, err := ts.objects.Get(ctx, manifests[0].ManifestPath)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	entries, err := readManifestEntries(manifestData)
	if err != nil {
		t.Fatalf("readManifestEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("manifest entries = %d, want 2", len(entries))
	}
	for i, e := range entries {
		if e.Status != manifestEntryAdded || e.SnapshotID != snap.SnapshotID || e.DataFile.FilePath != files[i].FilePath || e.DataFile.DT != "2026-08-06" {
			t.Fatalf("entry %d = %+v, want ADDED snapshot %d file %s dt=2026-08-06", i, e, snap.SnapshotID, files[i].FilePath)
		}
	}
}

func TestTableCommitSnapshotCarriesParentManifests(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	first, err := ts.CommitSnapshot(ctx, "events", []DataFile{{Content: DataFileContentData, FilePath: "warehouse/events/data/a.parquet", FileFormat: DataFileFormatParquet, RecordCount: 1, FileSizeBytes: 100}})
	if err != nil {
		t.Fatalf("first CommitSnapshot() error = %v", err)
	}
	second, err := ts.CommitSnapshot(ctx, "events", []DataFile{{Content: DataFileContentData, FilePath: "warehouse/events/data/b.parquet", FileFormat: DataFileFormatParquet, RecordCount: 1, FileSizeBytes: 200}})
	if err != nil {
		t.Fatalf("second CommitSnapshot() error = %v", err)
	}
	if first.SnapshotID == second.SnapshotID {
		t.Fatalf("snapshots share id %d", first.SnapshotID)
	}
	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	head := loaded.currentSnapshot()
	listData, err := ts.objects.Get(ctx, head.ManifestList)
	if err != nil {
		t.Fatalf("get manifest list: %v", err)
	}
	manifests, err := readManifestList(listData)
	if err != nil {
		t.Fatalf("readManifestList() error = %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("manifest list after two commits = %d manifests, want 2 (parent + new)", len(manifests))
	}
	if len(loaded.Snapshots) != 2 {
		t.Fatalf("snapshots = %d, want 2", len(loaded.Snapshots))
	}
}

func TestTableCommitSnapshotIsIdempotent(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	files := []DataFile{{Content: DataFileContentData, FilePath: "warehouse/events/data/a.parquet", FileFormat: DataFileFormatParquet, RecordCount: 1, FileSizeBytes: 100}}
	first, err := ts.CommitSnapshot(ctx, "events", files)
	if err != nil {
		t.Fatalf("first CommitSnapshot() error = %v", err)
	}
	second, err := ts.CommitSnapshot(ctx, "events", files)
	if err != nil {
		t.Fatalf("retry CommitSnapshot() error = %v", err)
	}
	if second.SnapshotID != first.SnapshotID {
		t.Fatalf("retry snapshot id = %d, want %d", second.SnapshotID, first.SnapshotID)
	}
	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Snapshots) != 1 {
		t.Fatalf("snapshots after idempotent retry = %d, want 1", len(loaded.Snapshots))
	}
}

func TestTableCommitSnapshotMergesManifests(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	const commits = maxManifestsPerSnapshot + 1
	for i := 0; i < commits; i++ {
		files := []DataFile{{Content: DataFileContentData, FilePath: fmt.Sprintf("warehouse/events/data/f%d.parquet", i), FileFormat: DataFileFormatParquet, RecordCount: 1, FileSizeBytes: 100}}
		if _, err := ts.CommitSnapshot(ctx, "events", files); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	head := loaded.currentSnapshot()
	listData, err := ts.objects.Get(ctx, head.ManifestList)
	if err != nil {
		t.Fatalf("get manifest list: %v", err)
	}
	manifests, err := readManifestList(listData)
	if err != nil {
		t.Fatalf("readManifestList() error = %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("manifest list after %d commits = %d manifests, want 1 (merged)", commits, len(manifests))
	}
	files, err := ts.CurrentDataFiles(ctx, "events")
	if err != nil {
		t.Fatalf("CurrentDataFiles() error = %v", err)
	}
	if len(files) != commits {
		t.Fatalf("data files = %d, want %d (merged manifest must keep every file)", len(files), commits)
	}
}
