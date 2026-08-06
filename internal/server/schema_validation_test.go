package server

import (
	"encoding/base64"
	"testing"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/meta"
)

func TestValidateTypedValue(t *testing.T) {
	s := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}, {Name: "ok", Type: "bool", Path: "$.ok", Nullable: true}}}
	if err := validateTypedValue(s, `{"id": 4}`); err != nil {
		t.Fatal(err)
	}
	if err := validateTypedValue(s, `{"id":"4"}`); err == nil {
		t.Fatal("expected type error")
	}
	if err := validateTypedValue(s, `{"ok":true}`); err == nil {
		t.Fatal("expected missing required field")
	}
}

func TestValidateTypedValueRejectsTimestampOutsideUnixNanosecondRange(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"}}}
	if err := validateTypedValue(schema, `{"occurred_at":"2263-01-01T00:00:00Z"}`); err == nil {
		t.Fatal("accepted timestamp above Unix nanosecond range")
	}
	if err := validateTypedValue(schema, `{"occurred_at":"1677-09-21T00:12:43.145224192Z"}`); err != nil {
		t.Fatalf("rejected lowest representable timestamp: %v", err)
	}
}

func TestValidateTypedValueAvro(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}, {Name: "ok", Type: "bool", Path: "$.ok", Nullable: true}}}
	encoded, err := iceberg.EncodeAvroValue(schema, map[string]any{"id": int64(4), "ok": true})
	if err != nil {
		t.Fatalf("EncodeAvroValueForTest: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString(encoded)
	if err := validateTypedValue(schema, b64); err != nil {
		t.Fatalf("validateTypedValue(avro) error = %v", err)
	}
	if err := validateTypedValue(schema, "not-base64!!"); err == nil {
		t.Fatal("validateTypedValue accepted non-base64 avro value")
	}
}
