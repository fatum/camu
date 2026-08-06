package iceberg

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hamba/avro/v2"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
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
				if errors.Is(err, storage.ErrNotFound) {
					// The schema id is not registered: the value is a raw Avro
					// payload that coincidentally starts with the Confluent
					// magic byte (a false positive from AvroUnwrap). Decode the
					// full input against the topic's own schema instead.
					return p.decodeAvroWire(input, p.avro, values)
				}
				return fmt.Errorf("resolve schema id %d: %w", schemaID, err)
			}
			writer, err = decodePlanFor(resolved)
			if err != nil {
				return err
			}
		}
		input = payload
	}
	return p.decodeAvroWire(input, writer.avro, values)
}

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
