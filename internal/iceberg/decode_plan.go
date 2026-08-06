package iceberg

import (
	"context"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/maksim/camu/internal/meta"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// decodePlan is the compiled, reusable state for decoding a topic schema's
// values. Building it is expensive — the JSON field tree, Avro schema
// compilation, protobuf descriptor construction — so plans are cached per
// schema and reused across produce validation and export rows.
type decodePlan struct {
	encoding string
	fields   []meta.SchemaField
	tree     *jsonFieldTree                 // json
	avro     avro.Schema                    // avro
	avroJSON string                         // avro schema JSON (for EncodeAvroValue)
	proto    protoreflect.MessageDescriptor // protobuf
}

func newDecodePlan(schema *meta.TopicSchema) (*decodePlan, error) {
	plan := &decodePlan{encoding: schema.Encoding, fields: schema.Fields}
	switch schema.Encoding {
	case "json":
		plan.tree = newJSONFieldTree(schema.Fields)
	case "avro":
		avroJSON, err := avroValueSchemaJSON(schema)
		if err != nil {
			return nil, err
		}
		plan.avroJSON = avroJSON
		plan.avro, err = avro.Parse(avroJSON)
		if err != nil {
			return nil, fmt.Errorf("parse avro value schema: %w", err)
		}
	case "protobuf":
		md, err := protobufDescriptor(schema)
		if err != nil {
			return nil, err
		}
		plan.proto = md
	default:
		plan.tree = newJSONFieldTree(schema.Fields)
	}
	return plan, nil
}

var decodePlanCache sync.Map // *meta.TopicSchema -> *decodePlan

// decodePlanFor returns the cached decode plan for a schema, building it on
// first use. Camu schemas are effectively immutable after registration (schema
// registry versions are immutable objects; topic schema updates replace the
// pointer wholesale), so the plan is keyed by the schema pointer itself.
func decodePlanFor(schema *meta.TopicSchema) (*decodePlan, error) {
	if schema == nil {
		return newDecodePlan(&meta.TopicSchema{Encoding: "json"})
	}
	if v, ok := decodePlanCache.Load(schema); ok {
		return v.(*decodePlan), nil
	}
	plan, err := newDecodePlan(schema)
	if err != nil {
		return nil, err
	}
	actual, _ := decodePlanCache.LoadOrStore(schema, plan)
	return actual.(*decodePlan), nil
}

func (p *decodePlan) decode(ctx context.Context, topic string, resolver SchemaResolver, input []byte) ([]DecodedField, error) {
	switch p.encoding {
	case "avro":
		return p.decodeAvro(ctx, topic, resolver, input)
	case "protobuf":
		return p.decodeProtobuf(ctx, topic, resolver, input)
	default:
		return p.decodeJSON(input)
	}
}

func (p *decodePlan) decodeJSON(input []byte) ([]DecodedField, error) {
	s := &jsonScanner{data: input}
	s.skipSpace()
	if s.eof() || s.peek() != '{' {
		return nil, fmt.Errorf("value must be a JSON object")
	}
	s.pos++
	values := make([]DecodedField, len(p.fields))
	if err := walkJSONObject(s, p.tree, p.fields, values); err != nil {
		return nil, err
	}
	s.skipSpace()
	if !s.eof() {
		return nil, fmt.Errorf("value is not valid JSON: multiple JSON values")
	}
	for index, field := range p.fields {
		if !values[index].Present && !field.Nullable {
			return nil, fmt.Errorf("required field %q is missing", field.Name)
		}
	}
	return values, nil
}
