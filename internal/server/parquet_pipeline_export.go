package server

// The Parquet consumer is a normal committed-record pipeline. In particular,
// it never discovers source segments from object storage: the partition index
// and its high watermark are the source of truth, while the pipeline
// checkpoint is the only source position persisted by this consumer.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/parquet"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

const parquetPipelineName = "parquet-export"
const parquetPipelineVersion = "v1"

const (
	parquetSinkStageFencedBeforeUpload  = "fenced_before_upload"
	parquetSinkStageObjectUpload        = "object_upload"
	parquetSinkStageFencedAfterUpload   = "fenced_after_upload"
	parquetSinkStageManifestPublish     = "manifest_publish"
	parquetSinkStageFencedAfterManifest = "fenced_after_manifest"
)

// parquetSinkFailure retains the failing sink operation without turning a
// potentially unbounded backend error string into a metric label.
type parquetSinkFailure struct {
	stage string
	err   error
}

func (e *parquetSinkFailure) Error() string {
	return fmt.Sprintf("%s: %v", e.stage, e.err)
}

func (e *parquetSinkFailure) Unwrap() error {
	return e.err
}

func parquetSinkError(stage string, err error) error {
	return &parquetSinkFailure{stage: stage, err: err}
}

func parquetSinkFailureStage(err error) string {
	var failure *parquetSinkFailure
	if errors.As(err, &failure) {
		return failure.stage
	}
	return "unknown"
}

func (s *Server) runParquetExportPass(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity, cp *pipeline.Checkpoint) {
	if !tc.ExportEnabled || identity.Role != PartitionRoleLeader || tc.StorageMode == meta.StorageModeDiskless || tc.UncleanLeaderElection {
		return
	}
	ps := s.partitionManager.GetPartitionState(tc.Name, identity.Partition)
	if ps == nil {
		return
	}
	ps.mu.RLock()
	index, highWatermark := ps.index.Clone(), uint64(0)
	if index != nil {
		highWatermark = index.HighWatermark()
	}
	ps.mu.RUnlock()
	if index == nil || highWatermark == 0 {
		return
	}
	labels := map[string]string{"topic": tc.Name, "partition": strconv.Itoa(identity.Partition)}
	s.metricSet("camu_parquet_export_pipeline_high_watermark", "Latest readable source high watermark for the Parquet export pipeline", labels, float64(highWatermark-1))
	s.metricSet("camu_parquet_export_pipeline_checkpoint_offset", "Latest Parquet pipeline checkpoint offset", labels, checkpointMetricOffset(cp.NextOffset))
	s.metricSet("camu_parquet_export_pipeline_lag_records", "Committed source records not yet checkpointed by the Parquet export pipeline", labels, float64(pipelineLagRecords(highWatermark, cp.NextOffset)))

	fence := serverPipelineFence{server: s}
	reader := pipeline.NewReader(s.fetcher, fence)
	checkpoints := pipeline.NewCheckpointStore(s.s3Client, fence)
	if cp.NextOffset >= highWatermark {
		return
	}
	startOffset := cp.NextOffset
	started := time.Now()
	result := "unknown"
	defer func() {
		s.metricInc("camu_parquet_export_pipeline_passes_total", "Parquet export pipeline passes", mergeMetricLabels(labels, "result", result))
		s.metricObserve("camu_parquet_export_pipeline_pass_duration", "Parquet export pipeline pass duration", mergeMetricLabels(labels, "result", result), time.Since(started))
	}()
	slog.Debug("parquet_pipeline_pass_started", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", startOffset, "high_watermark", highWatermark, "lag_records", pipelineLagRecords(highWatermark, startOffset))
	maxRecords := s.cfg.Maintenance.ParquetExport.MaxRecordsValue()
	if maxRecords < 1 {
		maxRecords = 4096
	}
	passCtx, cancel := context.WithTimeout(ctx, s.cfg.Maintenance.ParquetExport.MaxDurationValue())
	defer cancel()
	messages, next, err := reader.Read(passCtx, index, tc.Name, identity.Partition, cp.NextOffset, highWatermark, identity.LeaderEpoch, maxRecords)
	if err != nil {
		if !errors.Is(err, pipeline.ErrFenced) {
			result = "read_error"
			slog.Warn("parquet_pipeline_read_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "high_watermark", highWatermark, "error", err)
		} else {
			result = "fenced"
		}
		return
	}
	if len(messages) == 0 || next <= cp.NextOffset {
		result = "no_messages"
		slog.Debug("parquet_pipeline_pass_no_messages", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "high_watermark", highWatermark, "next_offset", next)
		return
	}

	valid := make([]log.Message, 0, len(messages))
	failed := make([]schemaFailure, 0)
	for _, m := range messages {
		if tc.Schema != nil {
			if err := validateTypedValue(tc.Schema, string(m.Value)); err != nil {
				failed = append(failed, schemaFailure{message: m, err: err})
				continue
			}
		}
		valid = append(valid, m)
	}
	if len(failed) > 0 {
		if err := s.handleSchemaDecodeFailures(passCtx, tc, identity, failed); err != nil {
			result = "dlq_error"
			slog.Warn("parquet_pipeline_dlq_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "failed_records", len(failed), "error", err)
			return
		}
	}

	var objectKey string
	var pendingKey string
	var outputStart, outputEnd uint64
	if len(valid) > 0 {
		ingestTime := parquetExportIngestTime(index, valid[0].Offset, valid[0].Timestamp)
		pendingKey = parquetPendingExportKey(tc.Name, identity.Partition, valid[0].Offset, valid[len(valid)-1].Offset)
		if persisted, err := s.loadOrCreateParquetPendingExport(passCtx, pendingKey, ingestTime); err != nil {
			result = "pending_error"
			slog.Warn("parquet_pipeline_pending_export_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "source_end_offset", valid[len(valid)-1].Offset, "error", err)
			return
		} else {
			ingestTime = persisted
		}
		// Epoch is deliberately not part of the object identity. A retry after
		// leader reassignment must converge on the same immutable source-range
		// object rather than creating one object per epoch.
		objectKey = parquetPipelineObjectKey(tc.Name, identity.Partition, ingestTime, valid[0].Offset, valid[len(valid)-1].Offset)
		data, err := writeParquetChunk(valid, tc.Schema, s.cfg.Maintenance.ParquetExport.TempDirectoryValue())
		if err != nil {
			result = "encode_error"
			slog.Warn("parquet_pipeline_encode_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "records", len(valid), "error", err)
			return
		}
		if err := fenceWriteParquet(passCtx, s, fence, tc, identity, objectKey, data, ingestTime, valid[0].Offset, valid[len(valid)-1].Offset); err != nil {
			result = "sink_error"
			stage := parquetSinkFailureStage(err)
			sinkLabels := mergeMetricLabels(labels, "stage", stage)
			s.metricInc("camu_parquet_export_pipeline_sink_failures_total", "Parquet export pipeline sink failures", sinkLabels)
			s.metricSet("camu_parquet_export_pipeline_last_sink_failure_unixtime", "Unix timestamp of the latest Parquet export pipeline sink failure", sinkLabels, float64(time.Now().Unix()))
			slog.Warn("parquet_pipeline_sink_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "source_start_offset", valid[0].Offset, "source_end_offset", valid[len(valid)-1].Offset, "parquet_object_key", objectKey, "parquet_bytes", len(data), "stage", stage, "error", err)
			return
		}
		s.metricAdd("camu_parquet_export_pipeline_bytes_total", "Parquet bytes uploaded by the export pipeline", labels, float64(len(data)))
		outputStart, outputEnd = cp.OutputEnd+1, cp.OutputEnd+uint64(len(valid))
	} else {
		outputStart, outputEnd = cp.OutputEnd, cp.OutputEnd
	}

	nextCP := pipeline.Checkpoint{SourceTopic: tc.Name, Partition: identity.Partition, NextOffset: next, SourceEpoch: identity.LeaderEpoch, Sink: parquetPipelineName, SinkVersion: parquetPipelineVersion, OutputStart: outputStart, OutputEnd: outputEnd, Generation: cp.Generation + 1}
	if err := checkpoints.Publish(passCtx, parquetPipelineName, nextCP); err != nil {
		result = "checkpoint_error"
		slog.Warn("parquet_pipeline_checkpoint_publish_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "next_offset", next, "error", err)
		return
	}
	if pendingKey != "" {
		_ = s.s3Client.Delete(context.Background(), pendingKey)
	}
	*cp = nextCP
	result = "success"
	s.metricSet("camu_parquet_export_pipeline_checkpoint_offset", "Latest Parquet pipeline checkpoint offset", map[string]string{"topic": tc.Name, "partition": strconv.Itoa(identity.Partition)}, float64(next-1))
	s.metricSet("camu_parquet_export_pipeline_lag_records", "Committed source records not yet checkpointed by the Parquet export pipeline", labels, float64(pipelineLagRecords(highWatermark, next)))
	s.metricSet("camu_parquet_export_pipeline_last_success_unixtime", "Unix timestamp of the latest successful Parquet export pass", labels, float64(time.Now().Unix()))
	s.metricAdd("camu_parquet_export_pipeline_records_total", "Source records checkpointed by the Parquet export pipeline", labels, float64(next-startOffset))
	slog.Info("parquet_pipeline_pass_completed", "topic", tc.Name, "partition", identity.Partition, "source_start_offset", startOffset, "source_end_offset", next-1, "records", next-startOffset, "checkpoint_offset", next-1, "high_watermark", highWatermark, "lag_records", pipelineLagRecords(highWatermark, next), "duration", time.Since(started))
}

func checkpointMetricOffset(nextOffset uint64) float64 {
	if nextOffset == 0 {
		return -1
	}
	return float64(nextOffset - 1)
}

func pipelineLagRecords(highWatermark, nextOffset uint64) uint64 {
	if nextOffset >= highWatermark {
		return 0
	}
	return highWatermark - nextOffset
}

func mergeMetricLabels(labels map[string]string, key, value string) map[string]string {
	merged := make(map[string]string, len(labels)+1)
	for label, labelValue := range labels {
		merged[label] = labelValue
	}
	merged[key] = value
	return merged
}

type parquetPendingExport struct {
	IngestTime time.Time `json:"ingest_time"`
}

func parquetPendingExportKey(topic string, partition int, start, end uint64) string {
	return fmt.Sprintf("_meta/pipelines/%s-pending/%s/%d/%d-%d.json", parquetPipelineName, topic, partition, start, end)
}

// loadOrCreateParquetPendingExport persists the bucket identity before the
// immutable upload. If leadership is lost and the source segment metadata is
// later rewritten, retries still reuse the original bucket and object identity.
func (s *Server) loadOrCreateParquetPendingExport(ctx context.Context, key string, ingestTime time.Time) (time.Time, error) {
	data, err := json.Marshal(parquetPendingExport{IngestTime: ingestTime.UTC()})
	if err != nil {
		return time.Time{}, err
	}
	if _, err := s.s3Client.ConditionalPut(ctx, key, data, ""); err == nil {
		return ingestTime.UTC(), nil
	} else if !errors.Is(err, storage.ErrConflict) {
		return time.Time{}, err
	}
	existing, err := s.s3Client.Get(ctx, key)
	if err != nil {
		return time.Time{}, err
	}
	var pending parquetPendingExport
	if err := json.Unmarshal(existing, &pending); err != nil || pending.IngestTime.IsZero() {
		return time.Time{}, fmt.Errorf("invalid pending parquet export metadata")
	}
	return pending.IngestTime.UTC(), nil
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

func parquetPipelineObjectKey(topic string, partition int, ingestTime time.Time, start, end uint64) string {
	return parquet.ExportObjectKey(topic, partition, ingestTime, int64(start), int64(end), 1, "pipeline")
}

func fenceWriteParquet(ctx context.Context, s *Server, fence pipeline.Fence, tc meta.TopicConfig, identity PartitionIdentity, objectKey string, data []byte, ingestTime time.Time, start, end uint64) error {
	if fence.Fenced(ctx, tc.Name, identity.Partition, identity.LeaderEpoch) {
		return parquetSinkError(parquetSinkStageFencedBeforeUpload, pipeline.ErrFenced)
	}
	date, hour := parquet.BucketDateHour(ingestTime)
	if err := putImmutableParquetObject(ctx, s.s3Client, objectKey, data); err != nil {
		return parquetSinkError(parquetSinkStageObjectUpload, err)
	}
	store := s.newParquetStore()
	entry := parquet.Entry{ObjectKey: objectKey, BaseOffset: int64(start), EndOffset: int64(end), SchemaVersion: 1, SourceKey: "pipeline", SourceEpoch: identity.LeaderEpoch}
	if fence.Fenced(ctx, tc.Name, identity.Partition, identity.LeaderEpoch) {
		return parquetSinkError(parquetSinkStageFencedAfterUpload, cleanupUnreferencedParquetUpload(ctx, store, s.s3Client, tc.Name, identity.Partition, ingestTime, objectKey, pipeline.ErrFenced))
	}
	if _, err := store.ReplaceOverlappingEntries(ctx, tc.Name, identity.Partition, date, hour, []parquet.Entry{entry}); err != nil {
		return parquetSinkError(parquetSinkStageManifestPublish, cleanupUnreferencedParquetUpload(ctx, store, s.s3Client, tc.Name, identity.Partition, ingestTime, objectKey, err))
	}
	if fence.Fenced(ctx, tc.Name, identity.Partition, identity.LeaderEpoch) {
		return parquetSinkError(parquetSinkStageFencedAfterManifest, pipeline.ErrFenced)
	}
	return nil
}

// cleanupPipelineParquetUpload deletes an object only when the manifest does
// not reference it. A manifest write can be acknowledged ambiguously, so an
// unconditional delete would corrupt a durable export.
func cleanupUnreferencedParquetUpload(ctx context.Context, store *parquet.Store, client *storage.S3Client, topic string, partition int, ingestTime time.Time, objectKey string, cause error) error {
	checkCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if m, err := store.GetManifest(checkCtx, topic, partition, ingestTime); err == nil {
		for _, entry := range m.Entries {
			if entry.ObjectKey == objectKey {
				return cause
			}
		}
	} else if !errors.Is(err, parquet.ErrNotFound) && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w; manifest verification failed: %v", cause, err)
	}
	if err := client.Delete(checkCtx, objectKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w; cleanup uploaded object: %v", cause, err)
	}
	return cause
}
