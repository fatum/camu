package iceberg

import (
	"context"
	"strings"
	"testing"

	"github.com/maksim/camu/internal/meta"
)

func TestDecodeTypedFieldsSelectsNestedSchemaPaths(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.event.id"},
		{Name: "name", Type: "string", Path: "$.event.name"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "optional", Type: "string", Path: "$.optional", Nullable: true},
	}}
	values, err := DecodeTypedFields(context.Background(), "t", schema, nil, []byte(`{"event":{"id":7,"name":"alpha","ignored":"payload"},"enabled":true,"unrelated":{"large":"ignored"}}`))
	if err != nil {
		t.Fatalf("DecodeTypedFields() error = %v", err)
	}
	if len(values) != len(schema.Fields) || !values[0].Present || values[0].Value.Int64() != 7 || !values[1].Present || values[1].Value.String() != "alpha" || !values[2].Present || !values[2].Value.Boolean() || values[3].Present {
		t.Fatalf("decoded values = %+v", values)
	}
}

func TestDecodeTypedFieldsRejectsInvalidSelectedFieldWithoutDecodingUnknownFields(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	_, err := DecodeTypedFields(context.Background(), "t", schema, nil, []byte(`{"id":"not-an-int","unrelated":{"value":[1,2,3]}}`))
	if err == nil || !strings.Contains(err.Error(), `field "id" must be int64`) {
		t.Fatalf("DecodeTypedFields() error = %v, want int64 validation error", err)
	}
}
