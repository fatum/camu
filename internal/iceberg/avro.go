package iceberg

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/maksim/camu/internal/meta"
	"github.com/parquet-go/parquet-go"
)

// This file adds Avro as a topic value encoding alongside JSON. The topic
// schema's Fields remain the projected-column definition (name/type/path), and
// the Avro value is decoded against an Avro record schema derived from those
// fields — the same projection model as JSON, with Avro's binary wire format.
// Non-conforming values are reported as decode failures and routed to the DLQ.

// avroValueSchemaJSON builds the Avro record schema a topic's values are
// expected to match, from its typed Fields. Each field becomes a top-level
// record field typed to match its Iceberg column; nullable fields become
// ["null", type] unions with a null default.
func avroValueSchemaJSON(topicSchema *meta.TopicSchema) (string, error) {
	fields := make([]string, 0, len(topicSchema.Fields))
	for _, f := range topicSchema.Fields {
		name := avroFieldName(f.Path)
		if name == "" || strings.Contains(name, ".") {
			return "", fmt.Errorf("avro field %q: nested paths are not supported", f.Name)
		}
		typ, err := avroFieldType(f)
		if err != nil {
			return "", err
		}
		field := fmt.Sprintf(`{"name":%s,"type":%s}`, jsonQuote(name), typ)
		if f.Nullable {
			field = fmt.Sprintf(`{"name":%s,"type":["null",%s],"default":null}`, jsonQuote(name), typ)
		}
		fields = append(fields, field)
	}
	return fmt.Sprintf(`{"type":"record","name":"camu_value","fields":[%s]}`, strings.Join(fields, ",")), nil
}

// avroFieldName returns the top-level Avro record field name for a schema path
// (the path without the leading "$.").
func avroFieldName(path string) string {
	return strings.TrimPrefix(path, "$.")
}

func avroFieldType(f meta.SchemaField) (string, error) {
	switch f.Type {
	case "string":
		return `"string"`, nil
	case "int64":
		return `"long"`, nil
	case "float64":
		return `"double"`, nil
	case "bool":
		return `"boolean"`, nil
	case "timestamp":
		return `{"type":"long","logicalType":"timestamp-millis"}`, nil
	default:
		return "", fmt.Errorf("unsupported schema field type %q", f.Type)
	}
}

// DecodeAvroTypedFields decodes an Avro-encoded record value and materializes
// the topic schema's projected fields as parquet values, mirroring
// DecodeTypedFields for JSON. Required fields missing from the value are
// errors; nullable fields missing or null are not present.
// EncodeAvroValue encodes a record value against a topic's derived Avro
// schema. It is the inverse of DecodeAvroTypedFields and is used by tooling
// and tests to produce values a topic will accept.
func EncodeAvroValue(topicSchema *meta.TopicSchema, record map[string]any) ([]byte, error) {
	plan, err := decodePlanFor(topicSchema)
	if err != nil {
		return nil, err
	}
	return avro.Marshal(plan.avro, record)
}

// avroWireMagic is the Confluent Schema Registry wire-format magic byte that
// prefixes a value with its 4-byte big-endian schema id.
const avroWireMagic = 0x00

// AvroWrap prefixes an encoded Avro payload with the schema-id envelope
// ([magic][4-byte big-endian schema id]) so the export can resolve the value's
// writer schema for read-side evolution.
func AvroWrap(schemaID int, payload []byte) []byte {
	out := make([]byte, 0, 5+len(payload))
	out = append(out, avroWireMagic)
	out = binary.BigEndian.AppendUint32(out, uint32(schemaID))
	return append(out, payload...)
}

// AvroUnwrap parses the schema-id envelope from an Avro value. wrapped is true
// when the value carries the envelope; the remaining payload is the raw Avro
// record bytes.
func AvroUnwrap(input []byte) (schemaID int, payload []byte, wrapped bool) {
	if len(input) < 5 || input[0] != avroWireMagic {
		return 0, input, false
	}
	return int(binary.BigEndian.Uint32(input[1:5])), input[5:], true
}

// SchemaResolver resolves a registered topic schema version by its registry
// id. The server's embedded registry implements it; offline tooling can pass
// nil to decode against the topic's own schema only.
type SchemaResolver interface {
	SchemaForID(ctx context.Context, topic string, id int) (*meta.TopicSchema, error)
}

// DecodeAvroTypedFields decodes an Avro-encoded record value and materializes
// the topic schema's projected fields as parquet values, mirroring
// DecodeTypedFields for JSON. A value wrapped in the schema-id envelope is
// decoded against its writer schema (resolved via resolver, enabling
// read-side evolution); an unwrapped value is decoded against the topic's own
// schema. Required fields missing from the value are errors; nullable fields
// missing or null are not present.
func DecodeAvroTypedFields(ctx context.Context, topic string, topicSchema *meta.TopicSchema, resolver SchemaResolver, input []byte) ([]DecodedField, error) {
	plan, err := decodePlanFor(topicSchema)
	if err != nil {
		return nil, err
	}
	return plan.decode(ctx, topic, resolver, input)
}

func (p *decodePlan) decodeAvroInto(ctx context.Context, topic string, resolver SchemaResolver, input []byte, values []DecodedField) error {
	writer := p
	if schemaID, payload, wrapped := AvroUnwrap(input); wrapped {
		if resolver != nil {
			resolved, err := resolver.SchemaForID(ctx, topic, schemaID)
			if err != nil {
				return fmt.Errorf("resolve schema id %d: %w", schemaID, err)
			}
			writer, err = decodePlanFor(resolved)
			if err != nil {
				return err
			}
		}
		input = payload
	}
	var m map[string]any
	if err := avro.Unmarshal(writer.avro, input, &m); err != nil {
		return fmt.Errorf("decode avro value: %w", err)
	}
	for i, f := range p.fields {
		raw, present := m[avroFieldName(f.Path)]
		if !present || raw == nil {
			if !f.Nullable {
				return fmt.Errorf("required field %q is missing", f.Name)
			}
			continue
		}
		v, err := avroFieldValue(f, raw)
		if err != nil {
			return err
		}
		values[i] = DecodedField{Present: true, Value: v}
	}
	return nil
}

func avroFieldValue(f meta.SchemaField, raw any) (parquet.Value, error) {
	switch f.Type {
	case "string":
		s, ok := raw.(string)
		if !ok {
			return parquet.Value{}, fmt.Errorf("field %q must be string", f.Name)
		}
		return parquet.ValueOf(s), nil
	case "int64":
		n, ok := avroLong(raw)
		if !ok {
			return parquet.Value{}, fmt.Errorf("field %q must be int64", f.Name)
		}
		return parquet.Int64Value(n), nil
	case "float64":
		switch v := raw.(type) {
		case float64:
			return parquet.DoubleValue(v), nil
		case float32:
			return parquet.DoubleValue(float64(v)), nil
		}
		return parquet.Value{}, fmt.Errorf("field %q must be number", f.Name)
	case "bool":
		b, ok := raw.(bool)
		if !ok {
			return parquet.Value{}, fmt.Errorf("field %q must be bool", f.Name)
		}
		return parquet.BooleanValue(b), nil
	case "timestamp":
		nanos, ok := avroTimestampNanos(raw)
		if !ok {
			return parquet.Value{}, fmt.Errorf("field %q must be a timestamp", f.Name)
		}
		return parquet.Int64Value(nanos), nil
	default:
		return parquet.Value{}, fmt.Errorf("unsupported schema field type %q", f.Type)
	}
}

func avroLong(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case float64:
		if v == float64(int64(v)) {
			return int64(v), true
		}
	}
	return 0, false
}

// avroTimestampNanos normalizes a decoded timestamp to Unix nanoseconds. With
// the timestamp-millis logical type hamba returns a time.Time; a plain long is
// treated as epoch millis.
func avroTimestampNanos(raw any) (int64, bool) {
	if t, ok := raw.(time.Time); ok {
		return t.UnixNano(), true
	}
	if ms, ok := avroLong(raw); ok {
		return ms * int64(time.Millisecond), true
	}
	return 0, false
}

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
