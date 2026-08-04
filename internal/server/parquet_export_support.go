package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/parquet-go/parquet-go"
)

var waitForReplicatedOffsetFn = waitForReplicatedOffset

type schemaFailure struct {
	message log.Message
	err     error
}

func (s *Server) handleSchemaDecodeFailures(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity, failures []schemaFailure) error {
	if tc.Schema == nil || tc.Schema.DeadLetterTopic == "" {
		for _, f := range failures {
			slog.Warn("parquet_schema_decode_skipped", "topic", tc.Name, "partition", identity.Partition, "offset", f.message.Offset, "error", f.err)
		}
		return nil
	}
	dlq := tc.Schema.DeadLetterTopic
	dlqCfg, err := s.topicStore.Get(ctx, dlq)
	if err != nil {
		return fmt.Errorf("load schema dead-letter topic %q: %w", dlq, err)
	}
	if dlqCfg.Schema != nil || dlqCfg.Partitions != tc.Partitions {
		return fmt.Errorf("invalid schema dead-letter topic %q", dlq)
	}
	const (
		dlqPipeline = "schema-dead-letter"
		dlqVersion  = "v1"
	)
	store := pipeline.NewCheckpointStore(s.s3Client, serverPipelineFence{server: s})
	appender := &serverDLQAppender{server: s, destination: dlq}
	sink := pipeline.NewDLQSink(appender, tc.Name, dlq, serverPipelineFence{server: s})
	cp, loadErr := store.Load(ctx, dlqPipeline, tc.Name, identity.Partition)
	if loadErr != nil && !errors.Is(loadErr, storage.ErrNotFound) {
		return fmt.Errorf("load schema dead-letter checkpoint: %w", loadErr)
	}
	if errors.Is(loadErr, storage.ErrNotFound) {
		cp = pipeline.Checkpoint{SourceTopic: tc.Name, Partition: identity.Partition, Sink: dlq, SinkVersion: dlqVersion}
	} else if loadErr == nil && (cp.Sink != dlq || cp.SinkVersion != dlqVersion) {
		return fmt.Errorf("schema dead-letter checkpoint has incompatible sink version")
	}
	for i := 0; i < len(failures); {
		if failures[i].message.Offset < cp.NextOffset {
			i++
			continue
		}
		j := i + 1
		for j < len(failures) && failures[j].err.Error() == failures[i].err.Error() {
			j++
		}
		messages := make([]log.Message, 0, j-i)
		for _, f := range failures[i:j] {
			messages = append(messages, f.message)
		}
		sequence := cp.OutputEnd + 1
		if cp.Generation == 0 {
			// Producer sequences begin at zero. OutputEnd is also zero for an
			// empty checkpoint, so it cannot by itself represent this initial
			// state.
			sequence = 0
		}
		result, writeErr := sink.Write(ctx, pipeline.Batch{SourceTopic: tc.Name, Partition: identity.Partition, SourceEpoch: identity.LeaderEpoch, StartOffset: messages[0].Offset, EndOffset: messages[len(messages)-1].Offset, SinkStartSequence: sequence, Messages: messages, Error: failures[i].err.Error(), ErrorMetadata: map[string]any{"schema_encoding": tc.Schema.Encoding}})
		if writeErr != nil {
			return fmt.Errorf("wait for schema dead-letter durability: %w", writeErr)
		}
		cp = pipeline.Checkpoint{SourceTopic: tc.Name, Partition: identity.Partition, NextOffset: messages[len(messages)-1].Offset + 1, SourceEpoch: identity.LeaderEpoch, Sink: dlq, SinkVersion: dlqVersion, OutputStart: result.OutputStart, OutputEnd: result.OutputEnd, Generation: cp.Generation + 1}
		if err := store.Publish(ctx, dlqPipeline, cp); err != nil {
			return fmt.Errorf("publish schema dead-letter checkpoint: %w", err)
		}
		i = j
	}
	return nil
}

func putImmutableParquetFile(ctx context.Context, client *storage.S3Client, objectKey string, file *os.File, size int64) error {
	if _, err := client.ConditionalPutFile(ctx, objectKey, file, size, ""); err != nil {
		if !errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("create parquet chunk %q: %w", objectKey, err)
		}
		equal, compareErr := client.ObjectEqualsFile(ctx, objectKey, file, size)
		if compareErr != nil {
			return fmt.Errorf("read conflicting parquet chunk %q: %w", objectKey, compareErr)
		}
		if !equal {
			return fmt.Errorf("immutable parquet chunk conflict at %q: existing bytes differ", objectKey)
		}
	}
	return nil
}

func writeParquetChunk(messages []log.Message, schema *meta.TopicSchema) ([]byte, error) {
	chunk, err := encodeParquetChunk(messages, schema)
	if err != nil {
		return nil, err
	}
	defer chunk.cleanup()
	if _, err := chunk.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind parquet chunk: %w", err)
	}
	data, err := io.ReadAll(chunk.file)
	if err != nil {
		return nil, fmt.Errorf("read parquet chunk: %w", err)
	}
	return data, nil
}

func (s *Server) encodeParquetChunk(messages []log.Message, schema *meta.TopicSchema) (parquetChunk, error) {
	return encodeParquetChunkInDir(s.cfg.SQL.TempDirectoryValue(), messages, schema)
}

// parquetChunk is the result of converting one committed source range. Failed
// typed records are deliberately kept separate from the Parquet rows: callers
// must make their DLQ durable before they can checkpoint the source range.
type parquetChunk struct {
	file     *os.File
	size     int64
	records  int
	start    uint64
	end      uint64
	startTS  int64
	failures []schemaFailure
}

func (c parquetChunk) cleanup() {
	if c.file == nil {
		return
	}
	name := c.file.Name()
	_ = c.file.Close()
	_ = os.Remove(name)
}

// encodeParquetChunk validates and encodes in one pass. In particular, it does
// not retain a second []log.Message containing every valid record. This keeps
// the source reader's batch as the only full in-memory source range.
func encodeParquetChunk(messages []log.Message, schema *meta.TopicSchema) (parquetChunk, error) {
	return encodeParquetChunkInDir("", messages, schema)
}

func encodeParquetChunkInDir(dir string, messages []log.Message, schema *meta.TopicSchema) (parquetChunk, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return parquetChunk{}, fmt.Errorf("create parquet temp directory: %w", err)
		}
	}
	file, err := os.CreateTemp(dir, "camu-parquet-*.parquet")
	if err != nil {
		return parquetChunk{}, fmt.Errorf("create parquet temp file: %w", err)
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
	chunk := parquetChunk{}
	for _, m := range messages {
		row, err := encoder.row(m)
		if err != nil {
			if schema != nil {
				chunk.failures = append(chunk.failures, schemaFailure{message: m, err: err})
				continue
			}
			return parquetChunk{}, fmt.Errorf("encode parquet row at offset %d: %w", m.Offset, err)
		}
		if chunk.records == 0 {
			chunk.start = m.Offset
			chunk.startTS = m.Timestamp
		}
		chunk.end = m.Offset
		chunk.records++
		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
			return parquetChunk{}, fmt.Errorf("write parquet row at offset %d: %w", m.Offset, err)
		}
	}
	if err := writer.Close(); err != nil {
		return parquetChunk{}, fmt.Errorf("close parquet writer: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return parquetChunk{}, fmt.Errorf("stat parquet temp file: %w", err)
	}
	chunk.file = file
	chunk.size = info.Size()
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

func (e *parquetRowEncoder) row(m log.Message) (parquet.Row, error) {
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
	if e.schema == nil {
		e.buffer = e.builder.AppendRow(e.buffer[:0])
		return e.buffer, nil
	}
	values, err := decodeTypedFields(e.schema, m.Value)
	if err != nil {
		return nil, err
	}
	for index, field := range e.schema.Fields {
		if !values[index].present {
			continue
		}
		e.builder.Add(e.columns[field.Name], values[index].value)
	}
	e.buffer = e.builder.AppendRow(e.buffer[:0])
	return e.buffer, nil
}

type decodedParquetField struct {
	present bool
	value   parquet.Value
}

// decodeTypedFields walks the JSON token stream and only materializes values
// named by the topic schema. The old map[string]any decode retained every
// source field, including large payloads that are not exported as columns.
func decodeTypedFields(schema *meta.TopicSchema, input []byte) ([]decodedParquetField, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	root, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("value is not valid JSON: %w", err)
	}
	if delimiter, ok := root.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("value must be a JSON object")
	}
	values := make([]decodedParquetField, len(schema.Fields))
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
		if !values[index].present && !field.Nullable {
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
func scanJSONFieldObject(decoder *json.Decoder, tree *jsonFieldTree, schema *meta.TopicSchema, values []decodedParquetField) error {
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
				values[index] = decodedParquetField{present: present, value: value}
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
		parsed, err := parseTimestamp(value)
		if err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be RFC3339 timestamp", field.Name)
		}
		return parquet.Int64Value(parsed.UnixNano()), true, nil
	default:
		return parquet.Value{}, false, fmt.Errorf("unsupported schema field type %q", field.Type)
	}
}

func parquetColumnValue(value any, fieldType string) (parquet.Value, error) {
	switch fieldType {
	case "string":
		text, err := asString(value)
		return parquet.ValueOf(text), err
	case "int64":
		number, err := asInt64(value)
		return parquet.Int64Value(number), err
	case "float64":
		number, err := asFloat64(value)
		return parquet.DoubleValue(number), err
	case "bool":
		boolean, err := asBool(value)
		return parquet.BooleanValue(boolean), err
	case "timestamp":
		parsed, err := parseTimestamp(value)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(parsed.UnixNano()), nil
	default:
		return parquet.Value{}, fmt.Errorf("unsupported schema field type %q", fieldType)
	}
}
