package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
)

// The Iceberg export pipeline is the committed-record sink that projects a
// topic into a self-managed Iceberg table: the data file is written immutably
// and then referenced by a new snapshot whose manifest is published through
// iceberg.TableStore. External engines read the table directly; the source
// checkpoint still advances last (output-before-checkpoint).
const icebergPipelineName = "iceberg-export"
const icebergPipelineVersion = "v1"

// icebergTableStoreFor returns an iceberg.TableStore bound to this server's
// object store, topic-deletion fence, and configured warehouse.
func (s *Server) icebergTableStoreFor() *iceberg.TableStore {
	return iceberg.NewTableStore(parquetObjectAdapter{client: s.s3Client}, serverFencer{s: s}, s.cfg.Maintenance.ParquetExport.WarehouseValue())
}

// ensureIcebergTable creates the topic's Iceberg table if it does not exist.
func (s *Server) ensureIcebergTable(ctx context.Context, tc meta.TopicConfig) error {
	store := s.icebergTableStoreFor()
	if _, err := store.Load(ctx, tc.Name); err == nil {
		return nil
	} else if !errors.Is(err, iceberg.ErrNotFound) {
		return err
	}
	// A concurrent leader may create it first; a conflict is fine.
	if _, err := store.Create(ctx, tc.Name, tc.Schema); err != nil && !errors.Is(err, iceberg.ErrConflict) {
		return err
	}
	return nil
}

func (s *Server) runIcebergExportPass(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity, cp *pipeline.Checkpoint) {
	if !tc.ExportEnabled || identity.Role != PartitionRoleLeader || tc.UncleanLeaderElection {
		return
	}
	var index *log.Index
	var highWatermark uint64
	if tc.StorageMode == meta.StorageModeDiskless {
		if s.disklessMeta == nil {
			return
		}
		committed, err := s.disklessMeta.GetCommittedHead(ctx, tc.Name, identity.Partition)
		if err != nil {
			slog.Warn("iceberg_pipeline_diskless_committed_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
			return
		}
		highWatermark = uint64(committed)
	} else {
		ps := s.partitionManager.GetPartitionState(tc.Name, identity.Partition)
		if ps == nil {
			return
		}
		ps.mu.RLock()
		index = ps.index.Clone()
		if index != nil {
			highWatermark = index.HighWatermark()
		}
		ps.mu.RUnlock()
	}
	if highWatermark == 0 {
		return
	}
	labels := map[string]string{"topic": tc.Name, "partition": strconv.Itoa(identity.Partition)}
	startOffset := cp.NextOffset
	if cp.NextOffset >= highWatermark {
		return
	}
	started := time.Now()
	result := "unknown"
	defer func() {
		s.metricInc("camu_iceberg_export_pipeline_passes_total", "Iceberg export pipeline passes", mergeMetricLabels(labels, "result", result))
		s.metricObserve("camu_iceberg_export_pipeline_pass_duration", "Iceberg export pipeline pass duration", mergeMetricLabels(labels, "result", result), time.Since(started))
	}()

	table := s.icebergTableStoreFor()
	if err := s.ensureIcebergTable(ctx, tc); err != nil {
		result = "table_error"
		slog.Warn("iceberg_pipeline_table_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}

	// Buffer source ranges into data files and commit one snapshot once the
	// byte target or the commit interval is reached, so snapshots (and the
	// manifest lists that carry them forward) stay bounded under sustained
	// load instead of one snapshot per tiny export pass.
	targetBytes := s.cfg.Maintenance.ParquetExport.TargetBytesValue()
	commitInterval := s.cfg.Maintenance.ParquetExport.MaxIntervalValue()
	maxDuration := s.cfg.Maintenance.ParquetExport.MaxDurationValue()
	maxRecords := s.cfg.Maintenance.ParquetExport.MaxRecordsValue()
	if maxRecords < 1 {
		maxRecords = 4096
	}
	fence := serverPipelineFence{server: s}
	passCtx, cancel := context.WithTimeout(ctx, maxDuration)
	defer cancel()

	var files []iceberg.DataFile
	var bufferedBytes, bufferedRecords int64
	nextOffset := cp.NextOffset
	for bufferedBytes < targetBytes && time.Since(started) < commitInterval {
		if nextOffset >= highWatermark {
			break
		}
		var messages []log.Message
		var next uint64
		var err error
		if tc.StorageMode == meta.StorageModeDiskless {
			messages, next, err = s.readDisklessCommittedBatch(passCtx, tc, identity.Partition, nextOffset, highWatermark, maxRecords)
		} else {
			reader := pipeline.NewReader(s.fetcher, fence)
			messages, next, err = reader.Read(passCtx, index, tc.Name, identity.Partition, nextOffset, highWatermark, identity.LeaderEpoch, maxRecords)
		}
		if err != nil {
			if !errors.Is(err, pipeline.ErrFenced) {
				result = "read_error"
				slog.Warn("iceberg_pipeline_read_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", nextOffset, "high_watermark", highWatermark, "error", err)
			} else {
				result = "fenced"
			}
			return
		}
		if len(messages) == 0 || next <= nextOffset {
			break
		}
		// The ingest (segment-flush) time is per-chunk and stable across retries;
		// it becomes the dt/hour partition values written on every row.
		ingestTime := parquetExportIngestTime(index, messages[0].Offset, messages[0].Timestamp)
		dt, hourStr := iceberg.BucketDateHour(ingestTime)
		hour, err := strconv.ParseInt(hourStr, 10, 32)
		if err != nil {
			result = "encode_error"
			slog.Warn("iceberg_pipeline_hour_parse_failed", "topic", tc.Name, "partition", identity.Partition, "hour", hourStr, "error", err)
			return
		}
		chunk, err := s.encodeParquetChunk(passCtx, tc, messages, tc.Schema, dt, int32(hour))
		if err != nil {
			result = "encode_error"
			slog.Warn("iceberg_pipeline_encode_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", nextOffset, "records", len(messages), "error", err)
			return
		}
		if len(chunk.Failures) > 0 {
			if err := s.handleSchemaDecodeFailures(passCtx, tc, identity, chunk.Failures); err != nil {
				chunk.Cleanup()
				result = "dlq_error"
				slog.Warn("iceberg_pipeline_dlq_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", nextOffset, "failed_records", len(chunk.Failures), "error", err)
				return
			}
		}
		objectKey := table.ExportDataFileKey(tc.Name, identity.Partition, ingestTime, int64(chunk.Start), int64(chunk.End), "iceberg")
		if err := putImmutableParquetFile(passCtx, s.s3Client, objectKey, chunk.File, chunk.Size); err != nil {
			chunk.Cleanup()
			result = "sink_error"
			slog.Warn("iceberg_pipeline_upload_failed", "topic", tc.Name, "partition", identity.Partition, "source_start_offset", chunk.Start, "source_end_offset", chunk.End, "parquet_object_key", objectKey, "parquet_bytes", chunk.Size, "error", err)
			return
		}
		files = append(files, iceberg.DataFile{
			Content:       iceberg.DataFileContentData,
			FilePath:      objectKey,
			FileFormat:    iceberg.DataFileFormatParquet,
			DT:            dt,
			Hour:          int(hour),
			RecordCount:   int64(chunk.Records),
			FileSizeBytes: chunk.Size,
		})
		bufferedBytes += chunk.Size
		bufferedRecords += int64(chunk.Records)
		nextOffset = next
		chunk.Cleanup()
	}
	if len(files) == 0 {
		result = "no_messages"
		return
	}
	if !s.CanRunOwnerJob(tc.Name, identity.Partition, identity.Leader, identity.LeaderEpoch) {
		result = "fenced"
		return
	}
	// CommitSnapshot is idempotent for the same file set, so a retry after
	// leadership moved or a crash converges on the same snapshot even though
	// the previous leader already published it.
	if _, err := table.CommitSnapshot(passCtx, tc.Name, files); err != nil {
		result = "sink_error"
		slog.Warn("iceberg_pipeline_commit_failed", "topic", tc.Name, "partition", identity.Partition, "files", len(files), "bytes", bufferedBytes, "error", err)
		return
	}
	s.metricAdd("camu_iceberg_export_pipeline_bytes_total", "Iceberg data bytes uploaded by the export pipeline", labels, float64(bufferedBytes))
	if !s.CanRunOwnerJob(tc.Name, identity.Partition, identity.Leader, identity.LeaderEpoch) {
		result = "fenced"
		return
	}

	nextCP := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: identity.Partition, NextOffset: nextOffset, SourceEpoch: identity.LeaderEpoch, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion, OutputStart: cp.OutputEnd + 1, OutputEnd: cp.OutputEnd + uint64(bufferedRecords), Generation: cp.Generation + 1}
	checkpoints := pipeline.NewCheckpointStore(s.s3Client, serverPipelineFence{server: s})
	if err := checkpoints.Publish(passCtx, icebergPipelineName, nextCP); err != nil {
		result = "checkpoint_error"
		slog.Warn("iceberg_pipeline_checkpoint_publish_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "next_offset", nextOffset, "error", err)
		return
	}
	*cp = nextCP
	result = "success"
	slog.Info("iceberg_pipeline_pass_completed", "topic", tc.Name, "partition", identity.Partition, "source_start_offset", startOffset, "source_end_offset", nextOffset-1, "records", nextOffset-startOffset, "checkpoint_offset", nextOffset-1, "high_watermark", highWatermark, "duration", time.Since(started))
}

// disklessExportFetchBytes bounds how much raw diskless data one export pass
// decodes; the pass itself is bounded by the checkpoint record limit.
const disklessExportFetchBytes = 16 << 20

// readDisklessCommittedBatch reads a batch of committed diskless messages
// starting at start, up to limit records and strictly below highWatermark. The
// diskless engine serves raw RecordBatch bytes from the shared metastore, which
// is read by the partition leader exactly like the classic pipeline reader.
func (s *Server) readDisklessCommittedBatch(ctx context.Context, tc meta.TopicConfig, partition int, start, highWatermark uint64, limit int) ([]log.Message, uint64, error) {
	if start >= highWatermark || limit <= 0 {
		return nil, start, nil
	}
	if s.disklessEngine == nil {
		return nil, start, errors.New("diskless export: engine unavailable")
	}
	data, _, err := s.disklessEngine.Fetch(ctx, tc.Name, partition, int64(start), disklessExportFetchBytes)
	if err != nil {
		return nil, start, fmt.Errorf("diskless export fetch: %w", err)
	}
	msgs, err := log.ReadSegmentBatchesAsMessages(data, start, limit)
	if err != nil {
		return nil, start, fmt.Errorf("diskless export decode: %w", err)
	}
	// Fetch takes its own committed-head snapshot, which can be newer than the
	// watermark captured by the caller when records commit concurrently. Never
	// export offsets at or above the captured watermark: they would be exported
	// again on the following pass, duplicating records.
	kept := msgs[:0]
	for _, m := range msgs {
		if m.Offset >= highWatermark {
			break // messages are ordered; the remainder is beyond the watermark
		}
		kept = append(kept, m)
	}
	msgs = kept
	if len(msgs) == 0 {
		return nil, start, nil
	}
	next := msgs[len(msgs)-1].Offset + 1
	if next > highWatermark {
		next = highWatermark
	}
	return msgs, next, nil
}

func mergeMetricLabels(labels map[string]string, key, value string) map[string]string {
	merged := make(map[string]string, len(labels)+1)
	for label, labelValue := range labels {
		merged[label] = labelValue
	}
	merged[key] = value
	return merged
}

// getExportCheckpointSink returns the checkpoint sink name used for export
// progress (Iceberg is the only export sink).
func (s *Server) getExportCheckpointSink(string) string {
	return icebergPipelineName
}

// parquetExportIngestTime returns the stable flush time used by the native
// segment containing offset.  It must not use time.Now: a retry of a record
// with a zero event timestamp would otherwise produce a different object key
// and bucket, leaving the first upload orphaned.  The record timestamp is
// only a fallback for indexes created without segment metadata (for example,
// old in-memory test indexes); zero timestamps use the Unix epoch so that
// retries remain deterministic even in that case.
func parquetExportIngestTime(index *log.Index, offset uint64, recordTimestamp int64) time.Time {
	if index != nil {
		if ref, ok := index.Lookup(offset); ok && !ref.CreatedAt.IsZero() {
			return ref.CreatedAt.UTC()
		}
	}
	if recordTimestamp == 0 {
		return time.Unix(0, 0).UTC()
	}
	return time.UnixMilli(recordTimestamp).UTC()
}
