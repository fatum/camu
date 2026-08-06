package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

type ClassicRetentionPayload struct {
	StorageMode    string `json:"storage_mode,omitempty"`
	SegmentKey     string `json:"segment_key"`
	OffsetIndexKey string `json:"offset_index_key"`
	MetadataKey    string `json:"metadata_key"`
	FileKey        string `json:"file_key,omitempty"`
	// EndOffset makes the export-coverage check independent of metadata reads
	// for newly-enqueued jobs. A pointer preserves compatibility with jobs
	// written before this field existed, which derive it from MetadataKey.
	EndOffset *uint64 `json:"end_offset,omitempty"`
}

var errRetentionAwaitingParquetExport = errors.New("retention awaiting parquet export")

func (s *Server) discoverClassicRetentionJobs(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity) {
	cutoff := time.Now().Add(-tc.Retention)
	exportedThrough := int64(-1)
	if tc.ExportEnabled {
		checkpoint, err := s.getParquetPipelineCheckpoint(ctx, tc.Name, identity.Partition)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			slog.Warn("classic_retention_export_checkpoint_read_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
			return
		}
		if err == nil {
			if checkpoint.NextOffset > 0 {
				exportedThrough = int64(checkpoint.NextOffset - 1)
			}
		}
	}
	keys, err := s.s3Client.List(ctx, log.ListSegmentPrefix(tc.Name, identity.Partition))
	if err != nil {
		slog.Warn("classic_retention_list_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".meta.json") {
			continue
		}
		data, err := s.s3Client.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			slog.Warn("classic_retention_meta_read_failed", "topic", tc.Name, "partition", identity.Partition, "key", key, "error", err)
			continue
		}
		var segMeta log.SegmentMetadata
		if err := json.Unmarshal(data, &segMeta); err != nil {
			slog.Warn("classic_retention_meta_decode_failed", "topic", tc.Name, "partition", identity.Partition, "key", key, "error", err)
			continue
		}
		if segMeta.CreatedAt.After(cutoff) {
			continue
		}
		if tc.ExportEnabled && (exportedThrough < 0 || segMeta.EndOffset > uint64(exportedThrough)) {
			continue
		}
		endOffset := segMeta.EndOffset
		payload, err := json.Marshal(ClassicRetentionPayload{
			StorageMode:    tc.StorageMode,
			SegmentKey:     segMeta.SegmentKey,
			OffsetIndexKey: segMeta.OffsetIndexKey,
			MetadataKey:    key,
			EndOffset:      &endOffset,
		})
		if err != nil {
			slog.Warn("classic_retention_payload_failed", "topic", tc.Name, "partition", identity.Partition, "segment_key", segMeta.SegmentKey, "error", err)
			continue
		}
		job := PartitionJob{
			ID:            partitionJobID(PartitionJobTypeRetention, segMeta.SegmentKey),
			Topic:         tc.Name,
			Partition:     identity.Partition,
			Type:          PartitionJobTypeRetention,
			ExpectedOwner: identity.Leader,
			ExpectedEpoch: identity.LeaderEpoch,
			State:         PartitionJobStatePending,
			Phase:         PartitionJobPhaseDeleteData,
			Payload:       payload,
		}
		if err := s.putPartitionJob(ctx, job); err != nil {
			slog.Warn("classic_retention_enqueue_failed", "topic", tc.Name, "partition", identity.Partition, "segment_key", segMeta.SegmentKey, "error", err)
		}
	}
}

func (s *Server) discoverDisklessRetentionJobs(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity, fileIdx *diskless.FileIndex) {
	if s.disklessMeta == nil {
		return
	}

	cutoff := time.Now().Add(-tc.Retention)
	var fileKeys []string
	if fileIdx != nil {
		// Use the shared per-pass index: no per-partition metadata scans.
		fileKeys = planExpiredFilesFromIndex(fileIdx, tc.Name, identity.Partition, cutoff)
	} else {
		var err error
		fileKeys, err = s.disklessMeta.PlanExpiredFileDeletes(ctx, tc.Name, identity.Partition, cutoff)
		if err != nil {
			slog.Warn("diskless_retention_plan_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
			return
		}
	}

	// A flush can pack batches for several partitions into one data file, so a
	// file may be shared across partitions. Retention must not delete a file
	// until every partition referencing it has exported its refs: an exporting
	// partition behind its Parquet checkpoint would otherwise permanently lose
	// committed records. Mirror the classic checkpoint gate per partition.
	for _, fileKey := range fileKeys {
		safe, err := s.disklessFileExported(ctx, fileKey, fileIdx)
		if err != nil {
			slog.Warn("diskless_retention_export_gate_failed", "topic", tc.Name, "partition", identity.Partition, "file", fileKey, "error", err)
			return
		}
		if !safe {
			continue // keep the data until every referencing exporter catches up
		}
		payload, err := json.Marshal(ClassicRetentionPayload{
			StorageMode: tc.StorageMode,
			FileKey:     fileKey,
		})
		if err != nil {
			slog.Warn("diskless_retention_payload_failed", "topic", tc.Name, "partition", identity.Partition, "file_key", fileKey, "error", err)
			continue
		}
		job := PartitionJob{
			ID:            partitionJobID(PartitionJobTypeRetention, fileKey),
			Topic:         tc.Name,
			Partition:     identity.Partition,
			Type:          PartitionJobTypeRetention,
			ExpectedOwner: identity.Leader,
			ExpectedEpoch: identity.LeaderEpoch,
			State:         PartitionJobStatePending,
			Phase:         PartitionJobPhaseDeleteData,
			Payload:       payload,
		}
		if err := s.putPartitionJob(ctx, job); err != nil {
			slog.Warn("diskless_retention_enqueue_failed", "topic", tc.Name, "partition", identity.Partition, "file_key", fileKey, "error", err)
		}
	}
}

// planExpiredFilesFromIndex returns, for one partition, the file keys whose refs
// in that partition are all expired and whose remaining refs anywhere (if any)
// are also expired — equivalent to PlanExpiredFileDeletes, but computed from
// the shared per-pass index instead of re-enumerating metadata per file.
func planExpiredFilesFromIndex(idx *diskless.FileIndex, topic string, partition int, cutoff time.Time) []string {
	pl := idx.PartitionFileLatest(topic, partition)
	var out []string
	for file, latestInPartition := range pl {
		if latestInPartition.After(cutoff) {
			continue // this partition still has a fresh ref to the file
		}
		if idx.FileLatest[file].After(cutoff) {
			continue // another partition still references it freshly
		}
		out = append(out, file)
	}
	return out
}

// disklessFileExported reports whether every ref into fileKey is covered by the
// Parquet checkpoint of its owning partition. A data file can be shared across
// partitions (one flush packs several partitions into a single object), so all
// of them must be exported before retention may delete it. Non-exporting
// partitions have no dependent pipeline and are not gated. A missing checkpoint
// (boundary 0, nothing exported) blocks deletion.
func (s *Server) disklessFileExported(ctx context.Context, fileKey string, fileIdx *diskless.FileIndex) (bool, error) {
	var refs []diskless.FileRef
	if fileIdx != nil {
		refs = fileIdx.ByFile[fileKey]
	} else {
		var err error
		refs, err = s.disklessMeta.ListFileRefs(ctx, fileKey)
		if err != nil {
			return false, err
		}
	}
	boundaries := make(map[string]int64)
	for _, fr := range refs {
		tc, err := s.topicStore.Get(ctx, fr.Topic)
		if err != nil {
			return false, err
		}
		if !tc.ExportEnabled {
			continue
		}
		key := partitionKey(fr.Topic, fr.Partition)
		boundary, ok := boundaries[key]
		if !ok {
			boundary, err = s.disklessExportBoundary(ctx, fr.Topic, fr.Partition)
			if err != nil {
				return false, err
			}
			boundaries[key] = boundary
		}
		if fr.Ref.EndOffset > boundary {
			return false, nil
		}
	}
	return true, nil
}

// disklessExportBoundary returns the Parquet checkpoint's exclusive next-export
// offset for a partition, or 0 when nothing has been exported.
func (s *Server) disklessExportBoundary(ctx context.Context, topic string, partition int) (int64, error) {
	checkpoint, err := s.getParquetPipelineCheckpoint(ctx, topic, partition)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return int64(checkpoint.NextOffset), nil
}

func (s *Server) runRetentionJob(ctx context.Context, job PartitionJob) error {
	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}

	var payload ClassicRetentionPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("decode retention payload: %w", err)
	}

	if payload.StorageMode == meta.StorageModeDiskless || payload.FileKey != "" {
		job.State = PartitionJobStateRunning
		if job.Phase == "" {
			job.Phase = PartitionJobPhaseDeleteData
		}
		if err := s.putPartitionJob(ctx, job); err != nil {
			return err
		}
		return s.runDisklessRetentionJob(ctx, job, payload)
	}

	if err := s.ensureClassicRetentionExported(ctx, job, payload); err != nil {
		return err
	}

	job.State = PartitionJobStateRunning
	if job.Phase == "" {
		job.Phase = PartitionJobPhaseDeleteData
	}
	if err := s.putPartitionJob(ctx, job); err != nil {
		return err
	}

	if job.Phase == PartitionJobPhaseDeleteData {
		for _, key := range []string{payload.SegmentKey, payload.OffsetIndexKey} {
			if key == "" {
				continue
			}
			if err := s.s3Client.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
		s.partitionManager.RemoveSealedSegmentObjects(job.Topic, job.Partition, payload.SegmentKey, payload.OffsetIndexKey)
		job.Phase = PartitionJobPhaseDeleteMeta
		if err := s.putPartitionJob(ctx, job); err != nil {
			return err
		}
	}

	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}

	if payload.MetadataKey != "" {
		if err := s.s3Client.Delete(ctx, payload.MetadataKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		s.partitionManager.RemoveSealedSegmentObjects(job.Topic, job.Partition, payload.MetadataKey)
	}
	return s.deletePartitionJob(ctx, job.Topic, job.Partition, job.ID)
}

// ensureClassicRetentionExported prevents retention from deleting source data
// before a durable Parquet checkpoint covers it. It is intentionally called
// before a job transitions to running so a blocked job stays retryable.
func (s *Server) ensureClassicRetentionExported(ctx context.Context, job PartitionJob, payload ClassicRetentionPayload) error {
	if job.Phase == PartitionJobPhaseDeleteMeta {
		return nil
	}
	tc, err := s.topicStore.Get(ctx, job.Topic)
	if err != nil {
		return fmt.Errorf("%w: read topic %q: %v", errRetentionAwaitingParquetExport, job.Topic, err)
	}
	if tc.StorageMode == meta.StorageModeDiskless {
		return nil
	}
	if !tc.ExportEnabled {
		return nil
	}

	endOffset, err := s.classicRetentionEndOffset(ctx, payload)
	if err != nil {
		return fmt.Errorf("%w: resolve segment end offset for %q: %v", errRetentionAwaitingParquetExport, payload.SegmentKey, err)
	}
	checkpoint, err := s.getParquetPipelineCheckpoint(ctx, job.Topic, job.Partition)
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w: checkpoint for %s/%d is absent", errRetentionAwaitingParquetExport, job.Topic, job.Partition)
	}
	if err != nil {
		return fmt.Errorf("%w: read checkpoint for %s/%d: %v", errRetentionAwaitingParquetExport, job.Topic, job.Partition, err)
	}
	exportedThrough := uint64(0)
	if checkpoint.NextOffset > 0 {
		exportedThrough = checkpoint.NextOffset - 1
	}
	if checkpoint.NextOffset == 0 || exportedThrough < endOffset {
		return fmt.Errorf("%w: checkpoint %d does not cover segment end offset %d for %s/%d", errRetentionAwaitingParquetExport, exportedThrough, endOffset, job.Topic, job.Partition)
	}
	return nil
}

func (s *Server) getParquetPipelineCheckpoint(ctx context.Context, topic string, partition int) (pipeline.Checkpoint, error) {
	return pipeline.NewCheckpointStore(s.s3Client, serverPipelineFence{server: s}).Load(ctx, parquetPipelineName, topic, partition)
}

func (s *Server) classicRetentionEndOffset(ctx context.Context, payload ClassicRetentionPayload) (uint64, error) {
	if payload.EndOffset != nil {
		return *payload.EndOffset, nil
	}
	if payload.MetadataKey == "" {
		return 0, errors.New("missing end_offset and metadata_key")
	}
	data, err := s.s3Client.Get(ctx, payload.MetadataKey)
	if err != nil {
		return 0, err
	}
	var segMeta log.SegmentMetadata
	if err := json.Unmarshal(data, &segMeta); err != nil {
		return 0, err
	}
	return segMeta.EndOffset, nil
}

func (s *Server) runDisklessRetentionJob(ctx context.Context, job PartitionJob, payload ClassicRetentionPayload) error {
	if job.Phase == PartitionJobPhaseDeleteData {
		if payload.FileKey != "" {
			// Re-check export coverage at run time: a job enqueued before the
			// checkpoint gate, or one that raced the exporter, must not delete a
			// shared file while any partition's refs into it are unexported.
			if ok, err := s.disklessFileExported(ctx, payload.FileKey, nil); err != nil {
				return err
			} else if !ok {
				return nil // retry on a later maintenance tick
			}
			if err := s.s3Client.Delete(ctx, payload.FileKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
		job.Phase = PartitionJobPhaseDeleteMeta
		if err := s.putPartitionJob(ctx, job); err != nil {
			return err
		}
	}

	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}

	if s.disklessMeta != nil && payload.FileKey != "" {
		if err := s.disklessMeta.DeleteFileRefs(ctx, payload.FileKey); err != nil {
			return err
		}
	}
	return s.deletePartitionJob(ctx, job.Topic, job.Partition, job.ID)
}
