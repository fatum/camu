package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

var waitForReplicatedOffsetFn = waitForReplicatedOffset

func (s *Server) handleSchemaDecodeFailures(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity, failures []iceberg.SchemaFailure) error {
	if tc.Schema == nil || tc.Schema.DeadLetterTopic == "" {
		for _, f := range failures {
			slog.Warn("parquet_schema_decode_skipped", "topic", tc.Name, "partition", identity.Partition, "offset", f.Message.Offset, "error", f.Err)
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
		if failures[i].Message.Offset < cp.NextOffset {
			i++
			continue
		}
		j := i + 1
		for j < len(failures) && failures[j].Err.Error() == failures[i].Err.Error() {
			j++
		}
		messages := make([]log.Message, 0, j-i)
		for _, f := range failures[i:j] {
			messages = append(messages, f.Message)
		}
		sequence := cp.OutputEnd + 1
		if cp.Generation == 0 {
			// Producer sequences begin at zero. OutputEnd is also zero for an
			// empty checkpoint, so it cannot by itself represent this initial
			// state.
			sequence = 0
		}
		result, writeErr := sink.Write(ctx, pipeline.Batch{SourceTopic: tc.Name, Partition: identity.Partition, SourceEpoch: identity.LeaderEpoch, StartOffset: messages[0].Offset, EndOffset: messages[len(messages)-1].Offset, SinkStartSequence: sequence, Messages: messages, Error: failures[i].Err.Error(), ErrorMetadata: map[string]any{"schema_encoding": tc.Schema.Encoding}})
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
	chunk, err := iceberg.EncodeChunk("", messages, schema, "1970-01-01", 0)
	if err != nil {
		return nil, err
	}
	defer chunk.Cleanup()
	if _, err := chunk.File.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind parquet chunk: %w", err)
	}
	data, err := io.ReadAll(chunk.File)
	if err != nil {
		return nil, fmt.Errorf("read parquet chunk: %w", err)
	}
	return data, nil
}

// encodeParquetChunk encodes one committed source range under the configured
// export temp directory. The encoding itself lives in internal/iceberg.
func (s *Server) encodeParquetChunk(messages []log.Message, schema *meta.TopicSchema, dt string, hour int32) (iceberg.Chunk, error) {
	return iceberg.EncodeChunk(s.cfg.Maintenance.ParquetExport.TempDirectoryValue(), messages, schema, dt, hour)
}
