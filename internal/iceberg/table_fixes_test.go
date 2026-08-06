package iceberg

import (
	"context"
	"testing"

	"github.com/maksim/camu/internal/meta"
)

func TestMaxSchemaFieldID(t *testing.T) {
	s := &Schema{Fields: []SchemaField{{ID: 1}, {ID: 42}, {ID: 7}}}
	if got := maxSchemaFieldID(s.Fields); got != 42 {
		t.Fatalf("maxSchemaFieldID = %d, want 42", got)
	}
}

func TestEnsureSchemaLastColumnIDIsMax(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	v1 := &meta.TopicSchema{Encoding: "json", Version: 1, Fields: []meta.SchemaField{
		{Name: "a", Type: "string", Path: "$.a"},
		{Name: "b", Type: "int64", Path: "$.b"},
		{Name: "c", Type: "bool", Path: "$.c"},
	}}
	if _, err := ts.EnsureSchema(ctx, "events", v1, 1); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}
	loaded, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := maxSchemaFieldID(loaded.currentSchema().Fields); loaded.LastColumnID != want {
		t.Fatalf("LastColumnID = %d, want max field id %d", loaded.LastColumnID, want)
	}
}

func TestTableMetadataLogChain(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	v0, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(v0.MetadataLog) != 0 {
		t.Fatalf("initial metadata-log = %d entries, want 0", len(v0.MetadataLog))
	}
	if _, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor("l1"), "l1", nil); err != nil {
		t.Fatalf("AppendSnapshot() error = %v", err)
	}
	v1, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(v1.MetadataLog) != 1 || v1.MetadataLog[0].MetadataFile != v0.metadataKey {
		t.Fatalf("metadata-log after first commit = %+v, want entry for %q", v1.MetadataLog, v0.metadataKey)
	}
	if _, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor("l2"), "l2", nil); err != nil {
		t.Fatalf("AppendSnapshot() error = %v", err)
	}
	v2, err := ts.Load(ctx, "events")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(v2.MetadataLog) != 2 || v2.MetadataLog[1].MetadataFile != v1.metadataKey {
		t.Fatalf("metadata-log after second commit = %+v, want entries for %q and %q", v2.MetadataLog, v0.metadataKey, v1.metadataKey)
	}
	if clone := v2.clone(); len(clone.MetadataLog) != 2 || clone.MetadataLog[1].MetadataFile != v1.metadataKey {
		t.Fatalf("cloned metadata-log = %+v, want 2 entries", clone.MetadataLog)
	}
}

func TestCommitRemovesOrphanMetadataAtSameVersion(t *testing.T) {
	ctx := context.Background()
	ts := newTestTableStore()
	if _, err := ts.Create(ctx, "events", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// Simulate a concurrent loser that wrote a metadata file at version 1 but
	// lost the version-hint CAS, and whose cleanup failed: two files at the
	// same version would let a reader listing by prefix pick the wrong one.
	orphan := "warehouse/events/metadata/00001-00000000-0000-0000-0000-000000000000.metadata.json"
	if _, err := ts.objects.ConditionalPut(ctx, orphan, []byte("{}"), ""); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	if _, err := ts.AppendSnapshot(ctx, "events", snapshotIDFor("l1"), "l1", nil); err != nil {
		t.Fatalf("AppendSnapshot() error = %v", err)
	}
	keys, err := ts.objects.List(ctx, "warehouse/events/metadata/00001-")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("files at version 1 = %v, want only the winner's file", keys)
	}
	if keys[0] == orphan {
		t.Fatalf("winner kept the orphaned metadata file %q", orphan)
	}
	if _, err := ts.Load(ctx, "events"); err != nil {
		t.Fatalf("Load() after cleanup error = %v", err)
	}
}
