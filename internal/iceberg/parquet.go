package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/parquet-go/parquet-go"
)

// SchemaFailure is one source record that could not be converted to typed
// Parquet columns. Failed records are kept separate from the valid rows so a
// caller can make its DLQ durable before it checkpoints the source range.
type SchemaFailure struct {
	Message log.Message
	Err     error
}

// Chunk is the result of converting one committed source range to a Parquet
// data file. File is a temporary object owned by the Chunk; call Cleanup when
// the caller is done with it.
type Chunk struct {
	File     *os.File
	Size     int64
	Records  int
	Start    uint64
	End      uint64
	StartTS  int64
	Failures []SchemaFailure
}

// Cleanup closes and removes the temporary data file.
func (c Chunk) Cleanup() {
	if c.File == nil {
		return
	}
	name := c.File.Name()
	_ = c.File.Close()
	_ = os.Remove(name)
}

// EncodeChunk validates and encodes one committed source range into a
// temporary Parquet file. dt and hour are the ingest-time partition values
// written on every row (they drive the Iceberg partition spec). resolver
// resolves Avro writer schemas for values wrapped in the schema-id envelope.
// It does not retain a second []log.Message containing every valid record: the
// source reader's batch stays the only full in-memory source range. The chunk
// file is removed by Cleanup.
func EncodeChunk(ctx context.Context, dir string, messages []log.Message, schema *meta.TopicSchema, dt string, hour int32, topic string, resolver SchemaResolver) (Chunk, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return Chunk{}, fmt.Errorf("create parquet temp directory: %w", err)
		}
	}
	file, err := os.CreateTemp(dir, "camu-parquet-*.parquet")
	if err != nil {
		return Chunk{}, fmt.Errorf("create parquet temp file: %w", err)
	}
	failed := true
	defer func() {
		if failed {
			name := file.Name()
			_ = file.Close()
			_ = os.Remove(name)
		}
	}()
	fileSchema := parquetSchema(schema)
	writer := parquet.NewWriter(file, fileSchema, parquet.Compression(&parquet.Snappy))
	encoder := newParquetRowEncoder(ctx, topic, schema, resolver, fileSchema)
	chunk := Chunk{}
	for _, m := range messages {
		row, err := encoder.row(m, dt, hour)
		if err != nil {
			if schema != nil {
				chunk.Failures = append(chunk.Failures, SchemaFailure{Message: m, Err: err})
				continue
			}
			return Chunk{}, fmt.Errorf("encode parquet row at offset %d: %w", m.Offset, err)
		}
		if chunk.Records == 0 {
			chunk.Start = m.Offset
			chunk.StartTS = m.Timestamp
		}
		chunk.End = m.Offset
		chunk.Records++
		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
			return Chunk{}, fmt.Errorf("write parquet row at offset %d: %w", m.Offset, err)
		}
	}
	if err := writer.Close(); err != nil {
		return Chunk{}, fmt.Errorf("close parquet writer: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return Chunk{}, fmt.Errorf("stat parquet temp file: %w", err)
	}
	chunk.File = file
	chunk.Size = info.Size()
	failed = false
	return chunk, nil
}

func parquetSchema(topicSchema *meta.TopicSchema) *parquet.Schema {
	fields := parquet.Group{
		"record_offset":    parquet.Int(64),
		"record_timestamp": parquet.Int(64),
		"key":              parquet.Leaf(parquet.ByteArrayType),
		"value":            parquet.Leaf(parquet.ByteArrayType),
		"headers":          parquet.String(),
		"dt":               parquet.String(),
		"hour":             parquet.Int(32),
	}
	if topicSchema != nil {
		for _, field := range topicSchema.Fields {
			node := parquetSchemaNode(field.Type)
			if field.Nullable {
				node = parquet.Optional(node)
			}
			fields[field.Name] = node
		}
	}
	return parquet.NewSchema("rows", fields)
}

func parquetSchemaNode(t string) parquet.Node {
	switch t {
	case "int64":
		return parquet.Int(64)
	case "float64":
		return parquet.Leaf(parquet.DoubleType)
	case "bool":
		return parquet.Leaf(parquet.BooleanType)
	case "timestamp":
		return parquet.Timestamp(parquet.Nanosecond)
	default:
		return parquet.String()
	}
}

type parquetRowEncoder struct {
	ctx      context.Context
	topic    string
	plan     *decodePlan
	resolver SchemaResolver
	builder  *parquet.RowBuilder
	columns  map[string]int
	buffer   parquet.Row
}

func newParquetRowEncoder(ctx context.Context, topic string, schema *meta.TopicSchema, resolver SchemaResolver, fileSchema *parquet.Schema) *parquetRowEncoder {
	columns := make(map[string]int, len(fileSchema.Columns()))
	for index, path := range fileSchema.Columns() {
		columns[path[0]] = index
	}
	var plan *decodePlan
	if schema != nil {
		if p, err := decodePlanFor(schema); err == nil {
			plan = p
		}
	}
	return &parquetRowEncoder{ctx: ctx, topic: topic, plan: plan, resolver: resolver, builder: parquet.NewRowBuilder(fileSchema), columns: columns}
}

func (e *parquetRowEncoder) row(m log.Message, dt string, hour int32) (parquet.Row, error) {
	headersJSON := ""
	if len(m.Headers) > 0 {
		b, _ := json.Marshal(m.Headers)
		headersJSON = string(b)
	}
	e.builder.Reset()
	e.builder.Add(e.columns["record_offset"], parquet.Int64Value(int64(m.Offset)))
	e.builder.Add(e.columns["record_timestamp"], parquet.Int64Value(m.Timestamp))
	e.builder.Add(e.columns["key"], parquet.ByteArrayValue(m.Key))
	e.builder.Add(e.columns["value"], parquet.ByteArrayValue(m.Value))
	e.builder.Add(e.columns["headers"], parquet.ValueOf(headersJSON))
	e.builder.Add(e.columns["dt"], parquet.ValueOf(dt))
	e.builder.Add(e.columns["hour"], parquet.Int32Value(hour))
	if e.plan == nil {
		e.buffer = e.builder.AppendRow(e.buffer[:0])
		return e.buffer, nil
	}
	values, err := e.plan.decode(e.ctx, e.topic, e.resolver, m.Value)
	if err != nil {
		return nil, err
	}
	for index, field := range e.plan.fields {
		if !values[index].Present {
			continue
		}
		e.builder.Add(e.columns[field.Name], values[index].Value)
	}
	e.buffer = e.builder.AppendRow(e.buffer[:0])
	return e.buffer, nil
}

// DecodedField is one typed column value selected by the topic schema.
type DecodedField struct {
	Present bool
	Value   parquet.Value
}

// DecodeTypedFields decodes a topic value into the schema's projected fields,
// dispatching on the topic schema encoding. resolver is used by Avro values
// wrapped in the schema-id envelope to resolve the writer schema (nil decodes
// against the topic's own schema). The old map[string]any JSON decode retained
// every source field, including large payloads that are not exported as
// columns.
func DecodeTypedFields(ctx context.Context, topic string, schema *meta.TopicSchema, resolver SchemaResolver, input []byte) ([]DecodedField, error) {
	plan, err := decodePlanFor(schema)
	if err != nil {
		return nil, err
	}
	return plan.decode(ctx, topic, resolver, input)
}

// ParseTimestamp parses an RFC3339 timestamp and rejects values outside the
// Unix nanosecond range representable by a Parquet timestamp column.
func ParseTimestamp(v any) (time.Time, error) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("not string")
	}
	timestamp, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	// time.Time.UnixNano is only defined within this interval. Reject values
	// outside it before they can reach Parquet export or the schema DLQ path.
	min := time.Unix(-9223372037, 145224192).UTC()
	max := time.Unix(9223372036, 854775807).UTC()
	timestamp = timestamp.UTC()
	if timestamp.Before(min) || timestamp.After(max) {
		return time.Time{}, fmt.Errorf("timestamp outside Unix nanosecond range")
	}
	return timestamp, nil
}
