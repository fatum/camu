package iceberg

import (
	"context"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/maksim/camu/internal/meta"
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
	// nameToIndex maps an avro/protobuf field name to its projection index.
	nameToIndex map[string]int
}

func newDecodePlan(schema *meta.TopicSchema) (*decodePlan, error) {
	plan := &decodePlan{encoding: schema.Encoding, fields: schema.Fields, nameToIndex: make(map[string]int, len(schema.Fields))}
	for i, f := range schema.Fields {
		plan.nameToIndex[avroFieldName(f.Path)] = i
	}
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
		plan, ok := v.(*decodePlan)
		if !ok {
			return nil, fmt.Errorf("decode plan cache: unexpected type %T", v)
		}
		return plan, nil
	}
	plan, err := newDecodePlan(schema)
	if err != nil {
		return nil, err
	}
	actual, _ := decodePlanCache.LoadOrStore(schema, plan)
	actualPlan, ok := actual.(*decodePlan)
	if !ok {
		return nil, fmt.Errorf("decode plan cache: unexpected type %T", actual)
	}
	return actualPlan, nil
}

func (p *decodePlan) decode(ctx context.Context, topic string, resolver SchemaResolver, input []byte) ([]DecodedField, error) {
	values := make([]DecodedField, len(p.fields))
	if err := p.decodeInto(ctx, topic, resolver, input, values); err != nil {
		return nil, err
	}
	return values, nil
}

// decodeInto decodes into a caller-provided buffer so hot loops (export rows)
// reuse it instead of allocating per value. The buffer must have length
// len(p.fields); its prior contents are cleared.
func (p *decodePlan) decodeInto(ctx context.Context, topic string, resolver SchemaResolver, input []byte, values []DecodedField) error {
	clear(values)
	switch p.encoding {
	case "avro":
		return p.decodeAvroInto(ctx, topic, resolver, input, values)
	case "protobuf":
		return p.decodeProtobufInto(ctx, topic, resolver, input, values)
	default:
		return p.decodeJSONInto(input, values)
	}
}

func (p *decodePlan) decodeJSONInto(input []byte, values []DecodedField) error {
	s := &jsonScanner{data: input}
	s.skipSpace()
	if s.eof() || s.peek() != '{' {
		return fmt.Errorf("value must be a JSON object")
	}
	s.pos++
	if err := walkJSONObject(s, p.tree, p.fields, values); err != nil {
		return err
	}
	s.skipSpace()
	if !s.eof() {
		return fmt.Errorf("value is not valid JSON: multiple JSON values")
	}
	for index, field := range p.fields {
		if !values[index].Present && !field.Nullable {
			return fmt.Errorf("required field %q is missing", field.Name)
		}
	}
	return nil
}
