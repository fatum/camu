package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
	_ "github.com/marcboeker/go-duckdb"
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

func writeParquetChunk(messages []log.Message, schema *meta.TopicSchema, tempDirectory string) ([]byte, error) {
	if err := os.MkdirAll(tempDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("create temp parent: %w", err)
	}
	tmpDir, err := os.MkdirTemp(tempDirectory, "camu-parquet-export-")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	columns := "record_offset BIGINT, record_timestamp BIGINT, key BLOB, value BLOB, headers VARCHAR"
	if schema != nil {
		for _, f := range schema.Fields {
			columns += ", " + quoteIdent(f.Name) + " " + parquetType(f.Type)
		}
	}
	if _, err := db.Exec(`CREATE TABLE rows (` + columns + `)`); err != nil {
		return nil, fmt.Errorf("create rows table: %w", err)
	}
	placeholders := "?, ?, ?, ?, ?"
	if schema != nil {
		for range schema.Fields {
			placeholders += ", ?"
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin parquet insert transaction: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO rows VALUES (` + placeholders + `)`)
	if err != nil {
		return nil, fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()
	for _, m := range messages {
		headersJSON := ""
		if len(m.Headers) > 0 {
			b, _ := json.Marshal(m.Headers)
			headersJSON = string(b)
		}
		args := []any{int64(m.Offset), m.Timestamp, m.Key, m.Value, headersJSON}
		skip := false
		if schema != nil {
			for _, f := range schema.Fields {
				v, found, ferr := typedValueAtPath(string(m.Value), f.Path)
				if ferr != nil || !found || v == nil {
					if f.Nullable {
						args = append(args, nil)
						continue
					}
					skip = true
					break
				}
				var cv any
				switch f.Type {
				case "string":
					cv, ferr = asString(v)
				case "int64":
					cv, ferr = asInt64(v)
				case "float64":
					cv, ferr = asFloat64(v)
				case "bool":
					cv, ferr = asBool(v)
				case "timestamp":
					cv, ferr = asTimestamp(v)
				}
				if ferr != nil {
					skip = true
					break
				}
				args = append(args, cv)
			}
		}
		if skip {
			continue
		}
		if _, err := stmt.Exec(args...); err != nil {
			return nil, fmt.Errorf("insert row at offset %d: %w", m.Offset, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit parquet rows: %w", err)
	}
	outPath := filepath.Join(tmpDir, "chunk.parquet")
	escaped := strings.ReplaceAll(outPath, `'`, `''`)
	if _, err := db.Exec(fmt.Sprintf(`COPY rows TO '%s' (FORMAT PARQUET)`, escaped)); err != nil {
		return nil, fmt.Errorf("copy to parquet: %w", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		return nil, fmt.Errorf("read parquet output: %w", err)
	}
	return data, nil
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func parquetType(t string) string {
	switch t {
	case "int64":
		return "BIGINT"
	case "float64":
		return "DOUBLE"
	case "bool":
		return "BOOLEAN"
	case "timestamp":
		return "TIMESTAMP"
	default:
		return "VARCHAR"
	}
}
