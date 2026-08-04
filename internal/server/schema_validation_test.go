package server

import (
	"strings"
	"testing"

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

func TestDecodeTypedFieldsSelectsNestedSchemaPaths(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.event.id"},
		{Name: "name", Type: "string", Path: "$.event.name"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "optional", Type: "string", Path: "$.optional", Nullable: true},
	}}
	values, err := decodeTypedFields(schema, []byte(`{"event":{"id":7,"name":"alpha","ignored":"payload"},"enabled":true,"unrelated":{"large":"ignored"}}`))
	if err != nil {
		t.Fatalf("decodeTypedFields() error = %v", err)
	}
	if len(values) != len(schema.Fields) || !values[0].present || values[0].value.Int64() != 7 || !values[1].present || values[1].value.String() != "alpha" || !values[2].present || !values[2].value.Boolean() || values[3].present {
		t.Fatalf("decoded values = %+v", values)
	}
}

func TestDecodeTypedFieldsRejectsInvalidSelectedFieldWithoutDecodingUnknownFields(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	_, err := decodeTypedFields(schema, []byte(`{"id":"not-an-int","unrelated":{"value":[1,2,3]}}`))
	if err == nil || !strings.Contains(err.Error(), `field "id" must be int64`) {
		t.Fatalf("decodeTypedFields() error = %v, want int64 validation error", err)
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
