package server

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
)

// The Iceberg export pipeline is a committed-record sink exactly like the
// legacy Parquet pipeline (output-before-checkpoint), but instead of publishing
// a per-bucket manifest it commits a standard Iceberg table: the data file is
// written immutably and then referenced by a new snapshot whose manifest is
// published through iceberg.TableStore. External engines read the table
// directly; the source checkpoint still advances last.
const icebergPipelineName = "iceberg-export"
const icebergPipelineVersion = "v1"

// exportSinkFor returns the checkpoint sink name/version a topic's export
// pipeline uses, and whether the Iceberg sink is active.
func (s *Server) exportSinkFor(tc meta.TopicConfig) (name, version string, iceberg bool) {
	if s.cfg.Maintenance.ParquetExport.Iceberg {
		return icebergPipelineName, icebergPipelineVersion, true
	}
	return parquetPipelineName, parquetPipelineVersion, false
}

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
	if cp.NextOffset >= highWatermark {
		return
	}
	startOffset := cp.NextOffset
	started := time.Now()
	result := "unknown"
	defer func() {
		s.metricInc("camu_iceberg_export_pipeline_passes_total", "Iceberg export pipeline passes", mergeMetricLabels(labels, "result", result))
		s.metricObserve("camu_iceberg_export_pipeline_pass_duration", "Iceberg export pipeline pass duration", mergeMetricLabels(labels, "result", result), time.Since(started))
	}()

	maxRecords := s.cfg.Maintenance.ParquetExport.MaxRecordsValue()
	if maxRecords < 1 {
		maxRecords = 4096
	}
	passCtx, cancel := context.WithTimeout(ctx, s.cfg.Maintenance.ParquetExport.MaxDurationValue())
	defer cancel()
	var messages []log.Message
	var next uint64
	var err error
	if tc.StorageMode == meta.StorageModeDiskless {
		messages, next, err = s.readDisklessCommittedBatch(passCtx, tc, identity.Partition, cp.NextOffset, highWatermark, maxRecords)
	} else {
		fence := serverPipelineFence{server: s}
		reader := pipeline.NewReader(s.fetcher, fence)
		messages, next, err = reader.Read(passCtx, index, tc.Name, identity.Partition, cp.NextOffset, highWatermark, identity.LeaderEpoch, maxRecords)
	}
	if err != nil {
		if !errors.Is(err, pipeline.ErrFenced) {
			result = "read_error"
			slog.Warn("iceberg_pipeline_read_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "high_watermark", highWatermark, "error", err)
		} else {
			result = "fenced"
		}
		return
	}
	if len(messages) == 0 || next <= cp.NextOffset {
		result = "no_messages"
		return
	}

	chunk, err := s.encodeParquetChunk(messages, tc.Schema)
	if err != nil {
		result = "encode_error"
		slog.Warn("iceberg_pipeline_encode_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "records", len(messages), "error", err)
		return
	}
	defer chunk.Cleanup()
	if len(chunk.Failures) > 0 {
		if err := s.handleSchemaDecodeFailures(passCtx, tc, identity, chunk.Failures); err != nil {
			result = "dlq_error"
			slog.Warn("iceberg_pipeline_dlq_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "failed_records", len(chunk.Failures), "error", err)
			return
		}
	}

	var outputStart, outputEnd uint64
	if chunk.Records > 0 {
		if !s.CanRunOwnerJob(tc.Name, identity.Partition, identity.Leader, identity.LeaderEpoch) {
			result = "fenced"
			return
		}
		ingestTime := parquetExportIngestTime(index, chunk.Start, chunk.StartTS)
		table := s.icebergTableStoreFor()
		if err := s.ensureIcebergTable(passCtx, tc); err != nil {
			result = "table_error"
			slog.Warn("iceberg_pipeline_table_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
			return
		}
		objectKey := table.ExportDataFileKey(tc.Name, identity.Partition, ingestTime, int64(chunk.Start), int64(chunk.End), "iceberg")
		if err := putImmutableParquetFile(passCtx, s.s3Client, objectKey, chunk.File, chunk.Size); err != nil {
			result = "sink_error"
			slog.Warn("iceberg_pipeline_upload_failed", "topic", tc.Name, "partition", identity.Partition, "source_start_offset", chunk.Start, "source_end_offset", chunk.End, "parquet_object_key", objectKey, "parquet_bytes", chunk.Size, "error", err)
			return
		}
		s.metricAdd("camu_iceberg_export_pipeline_bytes_total", "Iceberg data bytes uploaded by the export pipeline", labels, float64(chunk.Size))
		// Commit a snapshot referencing the data file. CommitSnapshot is
		// idempotent for the same file set, so a retry after leadership moved
		// converges on the same snapshot even though the old leader already
		// published it.
		if _, err := table.CommitSnapshot(passCtx, tc.Name, []iceberg.DataFile{{
			Content:       iceberg.DataFileContentData,
			FilePath:      objectKey,
			FileFormat:    iceberg.DataFileFormatParquet,
			RecordCount:   int64(chunk.Records),
			FileSizeBytes: chunk.Size,
		}}); err != nil {
			result = "sink_error"
			slog.Warn("iceberg_pipeline_commit_failed", "topic", tc.Name, "partition", identity.Partition, "source_start_offset", chunk.Start, "source_end_offset", chunk.End, "error", err)
			return
		}
		outputStart, outputEnd = cp.OutputEnd+1, cp.OutputEnd+uint64(chunk.Records)
	} else {
		outputStart, outputEnd = cp.OutputEnd, cp.OutputEnd
	}

	nextCP := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: identity.Partition, NextOffset: next, SourceEpoch: identity.LeaderEpoch, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion, OutputStart: outputStart, OutputEnd: outputEnd, Generation: cp.Generation + 1}
	checkpoints := pipeline.NewCheckpointStore(s.s3Client, serverPipelineFence{server: s})
	if err := checkpoints.Publish(passCtx, icebergPipelineName, nextCP); err != nil {
		result = "checkpoint_error"
		slog.Warn("iceberg_pipeline_checkpoint_publish_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "next_offset", next, "error", err)
		return
	}
	*cp = nextCP
	result = "success"
	slog.Info("iceberg_pipeline_pass_completed", "topic", tc.Name, "partition", identity.Partition, "source_start_offset", startOffset, "source_end_offset", next-1, "records", next-startOffset, "checkpoint_offset", next-1, "high_watermark", highWatermark, "duration", time.Since(started))
}

// getExportCheckpointSink returns the checkpoint sink name for a topic,
// falling back to the legacy parquet sink when Iceberg export is disabled.
func (s *Server) getExportCheckpointSink(topic string) string {
	if s.cfg.Maintenance.ParquetExport.Iceberg {
		return icebergPipelineName
	}
	return parquetPipelineName
}
