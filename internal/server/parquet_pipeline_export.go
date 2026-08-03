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
	"os"
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

func parquetManifestErrorDetails(err error) (category, key string, attempts int) {
	var conflict *parquet.ManifestCASConflictError
	if errors.As(err, &conflict) {
		return "cas_conflict_exhausted", conflict.Key, conflict.Attempts
	}
	return "manifest_write_error", "", 0
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

	chunk, err := s.encodeParquetChunk(messages, tc.Schema)
	if err != nil {
		result = "encode_error"
		slog.Warn("parquet_pipeline_encode_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "records", len(messages), "error", err)
		return
	}
	defer chunk.cleanup()
	if len(chunk.failures) > 0 {
		if err := s.handleSchemaDecodeFailures(passCtx, tc, identity, chunk.failures); err != nil {
			result = "dlq_error"
			slog.Warn("parquet_pipeline_dlq_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "failed_records", len(chunk.failures), "error", err)
			return
		}
	}

	var objectKey string
	var pendingKey string
	var outputStart, outputEnd uint64
	if chunk.records > 0 {
		ingestTime := parquetExportIngestTime(index, chunk.start, chunk.startTS)
		pendingKey = parquetPendingExportKey(tc.Name, identity.Partition, chunk.start, chunk.end)
		if persisted, err := s.loadOrCreateParquetPendingExport(passCtx, pendingKey, ingestTime); err != nil {
			result = "pending_error"
			slog.Warn("parquet_pipeline_pending_export_failed", "topic", tc.Name, "partition", identity.Partition, "checkpoint_offset", cp.NextOffset, "source_end_offset", chunk.end, "error", err)
			return
		} else {
			ingestTime = persisted
		}
		// Epoch is deliberately not part of the object identity. A retry after
		// leader reassignment must converge on the same immutable source-range
		// object rather than creating one object per epoch.
		objectKey = parquetPipelineObjectKey(tc.Name, identity.Partition, ingestTime, chunk.start, chunk.end)
		if err := fenceWriteParquet(passCtx, s, fence, tc, identity, objectKey, chunk.file, chunk.size, ingestTime, chunk.start, chunk.end); err != nil {
			result = "sink_error"
			stage := parquetSinkFailureStage(err)
			sinkLabels := mergeMetricLabels(labels, "stage", stage)
			s.metricInc("camu_parquet_export_pipeline_sink_failures_total", "Parquet export pipeline sink failures", sinkLabels)
			s.metricSet("camu_parquet_export_pipeline_last_sink_failure_unixtime", "Unix timestamp of the latest Parquet export pipeline sink failure", sinkLabels, float64(time.Now().Unix()))
			attributes := []any{"topic", tc.Name, "partition", identity.Partition, "leader_epoch", identity.LeaderEpoch, "checkpoint_offset", cp.NextOffset, "source_start_offset", chunk.start, "source_end_offset", chunk.end, "parquet_object_key", objectKey, "parquet_bytes", chunk.size, "stage", stage, "error", err}
			if stage == parquetSinkStageManifestPublish {
				category, key, attempts := parquetManifestErrorDetails(err)
				s.metricInc("camu_parquet_export_pipeline_manifest_failures_total", "Parquet manifest publication failures", mergeMetricLabels(labels, "category", category))
				attributes = append(attributes, "manifest_error_category", category, "manifest_key", key, "manifest_attempts", attempts)
			}
			slog.Warn("parquet_pipeline_sink_failed", attributes...)
			return
		}
		s.metricAdd("camu_parquet_export_pipeline_bytes_total", "Parquet bytes uploaded by the export pipeline", labels, float64(chunk.size))
		outputStart, outputEnd = cp.OutputEnd+1, cp.OutputEnd+uint64(chunk.records)
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

// TODO: Add server-scheduled reconciliation/GC for deterministic Parquet
// uploads retained after fencing or manifest publication failure. They must not
// be deleted inline: a successor can concurrently publish the same object.
func fenceWriteParquet(ctx context.Context, s *Server, fence pipeline.Fence, tc meta.TopicConfig, identity PartitionIdentity, objectKey string, file *os.File, size int64, ingestTime time.Time, start, end uint64) error {
	if fence.Fenced(ctx, tc.Name, identity.Partition, identity.LeaderEpoch) {
		return parquetSinkError(parquetSinkStageFencedBeforeUpload, pipeline.ErrFenced)
	}
	date, hour := parquet.BucketDateHour(ingestTime)
	if err := putImmutableParquetFile(ctx, s.s3Client, objectKey, file, size); err != nil {
		return parquetSinkError(parquetSinkStageObjectUpload, err)
	}
	store := s.newParquetStore()
	store.SetManifestPublishFence(func(checkCtx context.Context) error {
		if fence.Fenced(checkCtx, tc.Name, identity.Partition, identity.LeaderEpoch) {
			return pipeline.ErrFenced
		}
		return nil
	})
	store.SetManifestConflictObserver(func(conflict parquet.ManifestConflict) {
		labels := map[string]string{"topic": tc.Name, "partition": strconv.Itoa(identity.Partition), "category": "cas_conflict"}
		s.metricInc("camu_parquet_export_pipeline_manifest_conflicts_total", "Parquet manifest conditional-write conflicts", labels)
		slog.Warn("parquet_pipeline_manifest_cas_conflict", "topic", tc.Name, "partition", identity.Partition, "leader_epoch", identity.LeaderEpoch, "manifest_key", conflict.Key, "attempt", conflict.Attempt, "max_attempts", conflict.MaxAttempts, "error_category", "cas_conflict", "error", conflict.Err)
	})
	entry := parquet.Entry{ObjectKey: objectKey, BaseOffset: int64(start), EndOffset: int64(end), SchemaVersion: 1, SourceKey: "pipeline", SourceEpoch: identity.LeaderEpoch}
	if fence.Fenced(ctx, tc.Name, identity.Partition, identity.LeaderEpoch) {
		// The object key is deterministic for this source range.  Do not delete
		// it on fencing: a successor may concurrently reuse and publish this
		// object, and a read-then-delete cleanup can race that publication.
		// Retain unreferenced immutable uploads safely pending a future GC
		// mechanism.
		return parquetSinkError(parquetSinkStageFencedAfterUpload, pipeline.ErrFenced)
	}
	if _, err := store.ReplaceOverlappingEntries(ctx, tc.Name, identity.Partition, date, hour, []parquet.Entry{entry}); err != nil {
		// A manifest outcome can be ambiguous (and a successor can publish the
		// same deterministic object while this call returns). Retain the
		// immutable upload safely pending a future GC mechanism rather than
		// risking deletion of a now-referenced object.
		return parquetSinkError(parquetSinkStageManifestPublish, err)
	}
	if fence.Fenced(ctx, tc.Name, identity.Partition, identity.LeaderEpoch) {
		return parquetSinkError(parquetSinkStageFencedAfterManifest, pipeline.ErrFenced)
	}
	return nil
}
