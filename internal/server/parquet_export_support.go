package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

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

func putImmutableParquetObject(ctx context.Context, client *storage.S3Client, objectKey string, parquetBytes []byte) error {
	if _, err := client.ConditionalPut(ctx, objectKey, parquetBytes, ""); err != nil {
		if !errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("create parquet chunk %q: %w", objectKey, err)
		}
		existing, getErr := client.Get(ctx, objectKey)
		if getErr != nil {
			return fmt.Errorf("read conflicting parquet chunk %q: %w", objectKey, getErr)
		}
		if !bytes.Equal(existing, parquetBytes) {
			return fmt.Errorf("immutable parquet chunk conflict at %q: existing bytes differ", objectKey)
		}
	}
	return nil
}

func writeParquetChunk(messages []log.Message, schema *meta.TopicSchema) ([]byte, error) {
	var output bytes.Buffer
	fileSchema := parquetSchema(schema)
	writer := parquet.NewWriter(&output, fileSchema, parquet.Compression(&parquet.Snappy))
	encoder := newParquetRowEncoder(fileSchema, schema)
	for _, m := range messages {
		row, ok := encoder.row(m)
		if !ok {
			continue
		}
		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
			return nil, fmt.Errorf("write parquet row at offset %d: %w", m.Offset, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return output.Bytes(), nil
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

func (e *parquetRowEncoder) row(m log.Message) (parquet.Row, bool) {
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
		return e.buffer, true
	}
	var value map[string]any
	if err := json.Unmarshal(m.Value, &value); err != nil {
		return nil, false
	}
	for _, field := range e.schema.Fields {
		fieldValue, found := jsonPathValue(value, field.Path)
		if !found || fieldValue == nil {
			if field.Nullable {
				continue
			}
			return nil, false
		}
		converted, err := parquetColumnValue(fieldValue, field.Type)
		if err != nil {
			return nil, false
		}
		e.builder.Add(e.columns[field.Name], converted)
	}
	e.buffer = e.builder.AppendRow(e.buffer[:0])
	return e.buffer, true
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
