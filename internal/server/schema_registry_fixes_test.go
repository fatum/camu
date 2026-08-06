package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/maksim/camu/internal/meta"
)

// TestRegisterSchemaVersionHealsCrashOrphan verifies that a schema object left
// unreferenced by a crash between the object write and the index commit does
// not block the next registration: the orphan is replaced atomically.
func TestRegisterSchemaVersionHealsCrashOrphan(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	reg := srv.schemaRegistry
	topic := "orders"
	v0 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	if _, err := reg.RegisterTopicSchema(ctx, topic, v0); err != nil {
		t.Fatalf("RegisterTopicSchema() error = %v", err)
	}

	// Simulate a crashed registration: a stale schema object exists at version
	// 1 but the index still ends at 0.
	orphan := &meta.TopicSchema{Encoding: "json", Version: 1, Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "stale", Type: "string", Path: "$.stale"},
	}}
	enc, err := json.Marshal(orphan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.s3Client.ConditionalPut(ctx, reg.schemaKey(topic, 1), enc, ""); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	v1 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
	id, err := reg.RegisterSchemaVersion(ctx, topic, v1)
	if err != nil {
		t.Fatalf("RegisterSchemaVersion after orphan = %v", err)
	}
	if id != 1 {
		t.Fatalf("version id = %d, want 1", id)
	}
	got, err := reg.SchemaForID(ctx, topic, 1)
	if err != nil {
		t.Fatalf("SchemaForID(1) error = %v", err)
	}
	if len(got.Fields) != 2 || got.Fields[1].Name != "note" {
		t.Fatalf("SchemaForID(1) = %+v, want the new schema (orphan replaced)", got)
	}
}

// TestRegisterSchemaVersionIdenticalContentIsIdempotent verifies that a
// registration racing another at the same version, whose object write loses
// the create-if-not-exists and finds identical content already present, does
// not fail terminally: it completes and the index lists the version once.
func TestRegisterSchemaVersionIdenticalContentIsIdempotent(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	reg := srv.schemaRegistry
	topic := "orders"
	v0 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	if _, err := reg.RegisterTopicSchema(ctx, topic, v0); err != nil {
		t.Fatalf("RegisterTopicSchema() error = %v", err)
	}
	v1 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
	// A concurrent registration already wrote the identical version-1 object
	// (its index commit has not landed yet).
	seeded := meta.CloneSchema(v1)
	seeded.Version = 1
	enc, err := json.Marshal(seeded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.s3Client.ConditionalPut(ctx, reg.schemaKey(topic, 1), enc, ""); err != nil {
		t.Fatalf("seed object: %v", err)
	}

	id, err := reg.RegisterSchemaVersion(ctx, topic, v1)
	if err != nil {
		t.Fatalf("RegisterSchemaVersion() error = %v", err)
	}
	if id != 1 {
		t.Fatalf("version id = %d, want 1 (concurrent identical registration)", id)
	}
	data, err := srv.s3Client.Get(ctx, reg.indexKey(topic))
	if err != nil {
		t.Fatal(err)
	}
	var index schemaIndex
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, indexID := range index.IDs {
		if indexID == id {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("index lists version %d %d times, want once: %+v", id, count, index)
	}
}

// TestDeleteTopicSchemas verifies the registry state for a topic is removed
// when the topic is deleted.
func TestDeleteTopicSchemas(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	reg := srv.schemaRegistry
	topic := "orders"
	v0 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	if _, err := reg.RegisterTopicSchema(ctx, topic, v0); err != nil {
		t.Fatalf("RegisterTopicSchema() error = %v", err)
	}
	if _, err := reg.RegisterSchemaVersion(ctx, topic, &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}); err != nil {
		t.Fatalf("RegisterSchemaVersion() error = %v", err)
	}
	if err := reg.DeleteTopicSchemas(ctx, topic); err != nil {
		t.Fatalf("DeleteTopicSchemas() error = %v", err)
	}
	keys, err := srv.s3Client.List(ctx, "_meta/schemas/orders/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("schema objects after delete = %v, want none", keys)
	}
}

// TestGCUnreferencedSchemas verifies the periodic reconcile removes only
// objects not referenced by any index.
func TestGCUnreferencedSchemas(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	reg := srv.schemaRegistry
	v0 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	if _, err := reg.RegisterTopicSchema(ctx, "orders", v0); err != nil {
		t.Fatalf("RegisterTopicSchema() error = %v", err)
	}
	if _, err := reg.RegisterSchemaVersion(ctx, "orders", &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}); err != nil {
		t.Fatalf("RegisterSchemaVersion() error = %v", err)
	}
	// Two crash-orphaned objects for a topic that never got an index.
	for _, id := range []int{0, 1} {
		if _, err := srv.s3Client.ConditionalPut(ctx, reg.schemaKey("ghost", id), []byte("{}"), ""); err != nil {
			t.Fatalf("seed ghost object: %v", err)
		}
	}

	reg.GCUnreferencedSchemas(ctx)

	ghost, err := srv.s3Client.List(ctx, "_meta/schemas/ghost/")
	if err != nil {
		t.Fatal(err)
	}
	if len(ghost) != 0 {
		t.Fatalf("ghost objects after GC = %v, want none", ghost)
	}
	orders, err := srv.s3Client.List(ctx, "_meta/schemas/orders/")
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 3 { // registry.json + 0.json + 1.json
		t.Fatalf("orders objects after GC = %v, want 3 kept", orders)
	}
}
