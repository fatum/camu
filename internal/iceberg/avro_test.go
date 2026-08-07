package iceberg

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

func testAvroSchema(t *testing.T) *meta.TopicSchema {
	t.Helper()
	return &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
}

func TestDecodeAvroTypedFieldsRoundTrip(t *testing.T) {
	schema := testAvroSchema(t)
	// Encode a conforming value with hamba, then decode it through the topic
	// projection.
	schemaJSON, err := avroValueSchemaJSON(schema)
	if err != nil {
		t.Fatalf("avroValueSchemaJSON() error = %v", err)
	}
	avroSchema, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("avro.Parse() error = %v", err)
	}
	value, err := avro.Marshal(avroSchema, map[string]any{
		"name": "alpha", "count": int64(7), "ratio": 1.5, "enabled": true, "note": nil,
	})
	if err != nil {
		t.Fatalf("avro.Marshal() error = %v", err)
	}
	values, err := DecodeAvroTypedFields(context.Background(), "t", schema, nil, value)
	if err != nil {
		t.Fatalf("DecodeAvroTypedFields() error = %v", err)
	}
	if len(values) != len(schema.Fields) {
		t.Fatalf("decoded fields = %d, want %d", len(values), len(schema.Fields))
	}
	if !values[0].Present || values[0].Value.String() != "alpha" {
		t.Fatalf("name = %+v, want alpha", values[0])
	}
	if !values[1].Present || values[1].Value.Int64() != 7 {
		t.Fatalf("count = %+v, want 7", values[1])
	}
	if !values[2].Present || values[2].Value.Double() != 1.5 {
		t.Fatalf("ratio = %+v, want 1.5", values[2])
	}
	if !values[3].Present || !values[3].Value.Boolean() {
		t.Fatalf("enabled = %+v, want true", values[3])
	}
	if values[4].Present {
		t.Fatalf("note = %+v, want null/absent", values[4])
	}
}

func TestDecodeAvroTypedFieldsRejectsTypeMismatch(t *testing.T) {
	schema := testAvroSchema(t)
	// A value written under a drifted schema (count as string) must be
	// rejected rather than silently mis-typed.
	writerSchema, err := avro.Parse(`{"type":"record","name":"camu_value","fields":[{"name":"name","type":"string"},{"name":"count","type":"string"},{"name":"ratio","type":"double"},{"name":"enabled","type":"boolean"},{"name":"note","type":["null","string"],"default":null}]}`)
	if err != nil {
		t.Fatalf("avro.Parse() error = %v", err)
	}
	value, err := avro.Marshal(writerSchema, map[string]any{
		"name": "alpha", "count": "not-a-long", "ratio": 1.5, "enabled": true, "note": nil,
	})
	if err != nil {
		t.Fatalf("avro.Marshal() error = %v", err)
	}
	if _, err := DecodeAvroTypedFields(context.Background(), "t", schema, nil, value); err == nil {
		t.Fatal("DecodeAvroTypedFields() error = nil, want type mismatch rejection")
	}
}

func TestDecodeAvroTypedFieldsRejectsGarbage(t *testing.T) {
	schema := testAvroSchema(t)
	if _, err := DecodeAvroTypedFields(context.Background(), "t", schema, nil, []byte("not avro")); err == nil {
		t.Fatal("DecodeAvroTypedFields() error = nil, want decode error")
	}
}

func TestDecodeAvroTypedFieldsTimestampMillis(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}}
	schemaJSON, err := avroValueSchemaJSON(schema)
	if err != nil {
		t.Fatalf("avroValueSchemaJSON() error = %v", err)
	}
	if !strings.Contains(schemaJSON, "timestamp-millis") {
		t.Fatalf("avro schema = %s, want timestamp-millis logical type", schemaJSON)
	}
	avroSchema, _ := avro.Parse(schemaJSON)
	want := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	value, err := avro.Marshal(avroSchema, map[string]any{"occurred_at": want.UnixMilli()})
	if err != nil {
		t.Fatalf("avro.Marshal() error = %v", err)
	}
	values, err := DecodeAvroTypedFields(context.Background(), "t", schema, nil, value)
	if err != nil {
		t.Fatalf("DecodeAvroTypedFields() error = %v", err)
	}
	if !values[0].Present || values[0].Value.Int64() != want.UnixNano() {
		t.Fatalf("occurred_at = %+v, want %d", values[0], want.UnixNano())
	}
}

func TestDecodeTypedFieldsDispatchesOnEncoding(t *testing.T) {
	jsonSchema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	values, err := DecodeTypedFields(context.Background(), "t", jsonSchema, nil, []byte(`{"id":4}`))
	if err != nil || !values[0].Present || values[0].Value.Int64() != 4 {
		t.Fatalf("json dispatch: values=%+v err=%v", values, err)
	}

	avroSchema := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	schemaJSON, _ := avroValueSchemaJSON(avroSchema)
	as, _ := avro.Parse(schemaJSON)
	raw, _ := avro.Marshal(as, map[string]any{"id": int64(9)})
	values, err = DecodeTypedFields(context.Background(), "t", avroSchema, nil, raw)
	if err != nil || !values[0].Present || values[0].Value.Int64() != 9 {
		t.Fatalf("avro dispatch: values=%+v err=%v", values, err)
	}
}

type fakeResolver struct {
	schemas map[int]*meta.TopicSchema
}

func (f *fakeResolver) SchemaForID(_ context.Context, _ string, id int) (*meta.TopicSchema, error) {
	if s, ok := f.schemas[id]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("no schema %d", id)
}

// TestDecodeAvroTypedFieldsReadSideEvolution verifies that a value written
// under an old schema version still decodes onto the current projection when
// its schema-id envelope resolves the writer schema: fields added by newer
// versions project as absent (nullable), existing fields keep their values.
func TestDecodeAvroTypedFieldsReadSideEvolution(t *testing.T) {
	v0 := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	payload, err := EncodeAvroValue(v0, map[string]any{"id": int64(7)})
	if err != nil {
		t.Fatalf("EncodeAvroValue() error = %v", err)
	}
	wrapped := AvroWrap(0, payload)
	v1 := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
	resolver := &fakeResolver{schemas: map[int]*meta.TopicSchema{0: v0}}
	values, err := DecodeAvroTypedFields(context.Background(), "t", v1, resolver, wrapped)
	if err != nil {
		t.Fatalf("DecodeAvroTypedFields() error = %v", err)
	}
	if !values[0].Present || values[0].Value.Int64() != 7 {
		t.Fatalf("id = %+v, want 7 from the v0 writer schema", values[0])
	}
	if values[1].Present {
		t.Fatalf("note = %+v, want absent for a v0-written value", values[1])
	}

	// An unresolvable schema id is an error.
	if _, err := DecodeAvroTypedFields(context.Background(), "t", v1, resolver, AvroWrap(99, payload)); err == nil {
		t.Fatal("DecodeAvroTypedFields() with unknown schema id error = nil, want error")
	}
}

type notFoundResolver struct{}

func (notFoundResolver) SchemaForID(context.Context, string, int) (*meta.TopicSchema, error) {
	return nil, storage.ErrNotFound
}

// TestDecodeAvroUnwrapFalsePositive verifies that a raw (unwrapped) Avro value
// whose first byte happens to be the Confluent magic byte is not mis-decoded:
// the bogus schema id fails to resolve, and the full input is decoded against
// the topic schema instead of being truncated by 5 bytes.
func TestDecodeAvroUnwrapFalsePositive(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{
		{Name: "a", Type: "int64", Path: "$.a"},
		{Name: "s", Type: "string", Path: "$.s"},
	}}
	// a=0 encodes to a leading 0x00 (the magic byte) and the value is longer
	// than 5 bytes, so AvroUnwrap falsely reports a schema-id envelope.
	raw, err := EncodeAvroValue(schema, map[string]any{"a": int64(0), "s": "hello"})
	if err != nil {
		t.Fatalf("EncodeAvroValue() error = %v", err)
	}
	if _, _, wrapped := AvroUnwrap(raw); !wrapped {
		t.Fatalf("test premise broken: AvroUnwrap(%x) did not report wrapped", raw)
	}
	values, err := DecodeAvroTypedFields(context.Background(), "t", schema, notFoundResolver{}, raw)
	if err != nil {
		t.Fatalf("DecodeAvroTypedFields() on a false positive = %v, want fallback decode", err)
	}
	if !values[0].Present || values[0].Value.Int64() != 0 {
		t.Fatalf("a = %+v, want 0", values[0])
	}
	if !values[1].Present || values[1].Value.String() != "hello" {
		t.Fatalf("s = %+v, want hello", values[1])
	}
}

// TestAvroTimestampScale verifies the Unix-nanosecond scale honors the writer
// schema's timestamp logical type, so a value declared as micros is not
// misread as millis; a plain long defaults to millis (camu's writer).
func TestAvroTimestampScale(t *testing.T) {
	millis, err := avro.Parse(`{"type":"long","logicalType":"timestamp-millis"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := avroTimestampScale(millis); got != 1_000_000 {
		t.Fatalf("timestamp-millis scale = %d, want 1_000_000", got)
	}
	micros, err := avro.Parse(`{"type":"long","logicalType":"timestamp-micros"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := avroTimestampScale(micros); got != 1_000 {
		t.Fatalf("timestamp-micros scale = %d, want 1_000", got)
	}
	localMicros, err := avro.Parse(`{"type":"long","logicalType":"local-timestamp-micros"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := avroTimestampScale(localMicros); got != 1_000 {
		t.Fatalf("local-timestamp-micros scale = %d, want 1_000", got)
	}
	plain, err := avro.Parse(`"long"`)
	if err != nil {
		t.Fatal(err)
	}
	if got := avroTimestampScale(plain); got != 1_000_000 {
		t.Fatalf("plain long scale = %d, want 1_000_000 (default)", got)
	}
}
