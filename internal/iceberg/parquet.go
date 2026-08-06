package iceberg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
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
// written on every row (they drive the Iceberg partition spec). It does not
// retain a second []log.Message containing every valid record: the source
// reader's batch stays the only full in-memory source range. The chunk file is
// removed by Cleanup.
func EncodeChunk(dir string, messages []log.Message, schema *meta.TopicSchema, dt string, hour int32) (Chunk, error) {
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
	encoder := newParquetRowEncoder(fileSchema, schema)
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
	schema  *meta.TopicSchema
	builder *parquet.RowBuilder
	columns map[string]int
	buffer  parquet.Row
}

func newParquetRowEncoder(fileSchema *parquet.Schema, schema *meta.TopicSchema) *parquetRowEncoder {
	columns := make(map[string]int, len(fileSchema.Columns()))
	for index, path := range fileSchema.Columns() {
		columns[path[0]] = index
	}
	return &parquetRowEncoder{schema: schema, builder: parquet.NewRowBuilder(fileSchema), columns: columns}
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
	if e.schema == nil {
		e.buffer = e.builder.AppendRow(e.buffer[:0])
		return e.buffer, nil
	}
	values, err := DecodeTypedFields(e.schema, m.Value)
	if err != nil {
		return nil, err
	}
	for index, field := range e.schema.Fields {
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

// DecodeTypedFields walks the JSON token stream and only materializes values
// named by the topic schema. The old map[string]any decode retained every
// source field, including large payloads that are not exported as columns.
func DecodeTypedFields(schema *meta.TopicSchema, input []byte) ([]DecodedField, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	root, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("value is not valid JSON: %w", err)
	}
	if delimiter, ok := root.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("value must be a JSON object")
	}
	values := make([]DecodedField, len(schema.Fields))
	tree := newJSONFieldTree(schema)
	if err := scanJSONFieldObject(decoder, tree, schema, values); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("value is not valid JSON: multiple JSON values")
		}
		return nil, fmt.Errorf("value is not valid JSON: %w", err)
	}
	for index, field := range schema.Fields {
		if !values[index].Present && !field.Nullable {
			return nil, fmt.Errorf("required field %q is missing", field.Name)
		}
	}
	return values, nil
}

type jsonFieldTree struct {
	children map[string]*jsonFieldTree
	fields   []int
}

func newJSONFieldTree(schema *meta.TopicSchema) *jsonFieldTree {
	root := &jsonFieldTree{children: make(map[string]*jsonFieldTree)}
	for index, field := range schema.Fields {
		current := root
		for _, part := range strings.Split(strings.TrimPrefix(field.Path, "$."), ".") {
			if current.children[part] == nil {
				current.children[part] = &jsonFieldTree{children: make(map[string]*jsonFieldTree)}
			}
			current = current.children[part]
		}
		current.fields = append(current.fields, index)
	}
	return root
}

// scanJSONFieldObject consumes an object after its opening delimiter. Unknown
// fields are skipped token-by-token, so their values do not become part of a
// decoded object graph.
func scanJSONFieldObject(decoder *json.Decoder, tree *jsonFieldTree, schema *meta.TopicSchema, values []DecodedField) error {
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("value is not valid JSON: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("value is not valid JSON: object key is not string")
		}
		next := tree.children[key]
		if next == nil {
			if err := skipJSONValue(decoder); err != nil {
				return fmt.Errorf("value is not valid JSON: %w", err)
			}
			continue
		}
		if len(next.fields) > 0 {
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return fmt.Errorf("value is not valid JSON: %w", err)
			}
			for _, index := range next.fields {
				value, present, err := decodeParquetField(raw, schema.Fields[index])
				if err != nil {
					return err
				}
				values[index] = DecodedField{Present: present, Value: value}
			}
			continue
		}
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("value is not valid JSON: %w", err)
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{':
				if err := scanJSONFieldObject(decoder, next, schema, values); err != nil {
					return err
				}
			case '[':
				if err := skipJSONContainer(decoder, delimiter); err != nil {
					return fmt.Errorf("value is not valid JSON: %w", err)
				}
			}
		}
	}
	if token, err := decoder.Token(); err != nil {
		return fmt.Errorf("value is not valid JSON: %w", err)
	} else if delimiter, ok := token.(json.Delim); !ok || delimiter != '}' {
		return fmt.Errorf("value is not valid JSON: expected object end")
	}
	return nil
}

func skipJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || (delimiter != '{' && delimiter != '[') {
		return nil
	}
	return skipJSONContainer(decoder, delimiter)
}

func skipJSONContainer(decoder *json.Decoder, delimiter json.Delim) error {
	for decoder.More() {
		if delimiter == '{' {
			if _, err := decoder.Token(); err != nil { // object key
				return err
			}
		}
		if err := skipJSONValue(decoder); err != nil {
			return err
		}
	}
	_, err := decoder.Token() // closing delimiter
	return err
}

func decodeParquetField(raw json.RawMessage, field meta.SchemaField) (parquet.Value, bool, error) {
	if bytes.Equal(raw, []byte("null")) {
		return parquet.Value{}, false, nil
	}
	switch field.Type {
	case "string":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be string", field.Name)
		}
		return parquet.ValueOf(value), true, nil
	case "int64":
		var value float64
		if err := json.Unmarshal(raw, &value); err != nil || value != float64(int64(value)) {
			return parquet.Value{}, false, fmt.Errorf("field %q must be int64", field.Name)
		}
		return parquet.Int64Value(int64(value)), true, nil
	case "float64":
		var value float64
		if err := json.Unmarshal(raw, &value); err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be number", field.Name)
		}
		return parquet.DoubleValue(value), true, nil
	case "bool":
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be bool", field.Name)
		}
		return parquet.BooleanValue(value), true, nil
	case "timestamp":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be RFC3339 timestamp", field.Name)
		}
		parsed, err := ParseTimestamp(value)
		if err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be RFC3339 timestamp", field.Name)
		}
		return parquet.Int64Value(parsed.UnixNano()), true, nil
	default:
		return parquet.Value{}, false, fmt.Errorf("unsupported schema field type %q", field.Type)
	}
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
