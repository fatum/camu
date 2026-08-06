package server

import (
	"context"
	"testing"

	"github.com/maksim/camu/internal/meta"
)

func TestSchemaRegistryVersioning(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	reg := srv.schemaRegistry
	topic := "orders"

	v0 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	id, err := reg.RegisterTopicSchema(ctx, topic, v0)
	if err != nil {
		t.Fatalf("RegisterTopicSchema() error = %v", err)
	}
	if id != 0 {
		t.Fatalf("initial schema id = %d, want 0", id)
	}

	// Idempotent re-registration returns 0 without error.
	if id, err := reg.RegisterTopicSchema(ctx, topic, v0); err != nil || id != 0 {
		t.Fatalf("re-register = %d, %v; want 0, nil", id, err)
	}

	// Additive change (new nullable field) is compatible.
	v1 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
	id, err = reg.RegisterSchemaVersion(ctx, topic, v1)
	if err != nil {
		t.Fatalf("RegisterSchemaVersion(v1) error = %v", err)
	}
	if id != 1 {
		t.Fatalf("v1 schema id = %d, want 1", id)
	}
	got, err := reg.SchemaForID(ctx, topic, 1)
	if err != nil {
		t.Fatalf("SchemaForID(1) error = %v", err)
	}
	if got.Version != 1 || len(got.Fields) != 2 {
		t.Fatalf("SchemaForID(1) = %+v, want version 1 with 2 fields", got)
	}
	got0, err := reg.SchemaForID(ctx, topic, 0)
	if err != nil {
		t.Fatalf("SchemaForID(0) error = %v", err)
	}
	if len(got0.Fields) != 1 {
		t.Fatalf("SchemaForID(0) fields = %d, want 1", len(got0.Fields))
	}
}

func TestSchemaRegistryRejectsIncompatibleChanges(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	reg := srv.schemaRegistry
	topic := "orders"

	v0 := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	if _, err := reg.RegisterTopicSchema(ctx, topic, v0); err != nil {
		t.Fatalf("RegisterTopicSchema() error = %v", err)
	}

	// Removing a field breaks backward compatibility.
	removed := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{}}
	if _, err := reg.RegisterSchemaVersion(ctx, topic, removed); err == nil {
		t.Fatal("RegisterSchemaVersion with removed field succeeded, want error")
	}

	// Changing a field type breaks compatibility.
	retyped := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "string", Path: "$.id"}}}
	if _, err := reg.RegisterSchemaVersion(ctx, topic, retyped); err == nil {
		t.Fatal("RegisterSchemaVersion with retyped field succeeded, want error")
	}

	// Changing the encoding breaks compatibility.
	reencoded := &meta.TopicSchema{Encoding: "avro", Fields: v0.Fields}
	if _, err := reg.RegisterSchemaVersion(ctx, topic, reencoded); err == nil {
		t.Fatal("RegisterSchemaVersion with different encoding succeeded, want error")
	}
}

func TestSchemaRegistryMissingTopic(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	reg := srv.schemaRegistry
	if _, err := reg.SchemaForID(ctx, "missing", 0); err == nil {
		t.Fatal("SchemaForID(missing) error = nil, want error")
	}
	if _, err := reg.RegisterSchemaVersion(ctx, "missing", &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}); err == nil {
		t.Fatal("RegisterSchemaVersion(missing) error = nil, want error")
	}
}
