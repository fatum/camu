package server

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/meta"
)

func topicCfgWithSchema(schema *meta.TopicSchema) meta.TopicConfig {
	return meta.TopicConfig{Name: "t", Schema: schema}
}

func TestValidateTypedValue(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	s := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}, {Name: "ok", Type: "bool", Path: "$.ok", Nullable: true}}}
	tc := topicCfgWithSchema(s)
	if err := srv.validateTypedValue(ctx, tc, `{"id": 4}`); err != nil {
		t.Fatal(err)
	}
	if err := srv.validateTypedValue(ctx, tc, `{"id":"4"}`); err == nil {
		t.Fatal("expected type error")
	}
	if err := srv.validateTypedValue(ctx, tc, `{"ok":true}`); err == nil {
		t.Fatal("expected missing required field")
	}
}

func TestValidateTypedValueRejectsTimestampOutsideUnixNanosecondRange(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"}}}
	tc := topicCfgWithSchema(schema)
	if err := srv.validateTypedValue(ctx, tc, `{"occurred_at":"2263-01-01T00:00:00Z"}`); err == nil {
		t.Fatal("accepted timestamp above Unix nanosecond range")
	}
	if err := srv.validateTypedValue(ctx, tc, `{"occurred_at":"1677-09-21T00:12:43.145224192Z"}`); err != nil {
		t.Fatalf("rejected lowest representable timestamp: %v", err)
	}
}

func TestValidateTypedValueAvro(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	schema := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}, {Name: "ok", Type: "bool", Path: "$.ok", Nullable: true}}}
	tc := topicCfgWithSchema(schema)
	encoded, err := iceberg.EncodeAvroValue(schema, map[string]any{"id": int64(4), "ok": true})
	if err != nil {
		t.Fatalf("EncodeAvroValue: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString(encoded)
	if err := srv.validateTypedValue(ctx, tc, b64); err != nil {
		t.Fatalf("validateTypedValue(avro) error = %v", err)
	}
	if err := srv.validateTypedValue(ctx, tc, "not-base64!!"); err == nil {
		t.Fatal("validateTypedValue accepted non-base64 avro value")
	}
}
