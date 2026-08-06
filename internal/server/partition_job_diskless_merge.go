package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

// DisklessMergePayload describes one diskless small-segment merge. Sources is
// the contiguous, fully-committed run being replaced; the merged data object and
// ref are published before the source data is deleted.
type DisklessMergePayload struct {
	StorageMode string                `json:"storage_mode,omitempty"`
	Sources     []diskless.SegmentRef `json:"sources"`
	MergedFile  string                `json:"merged_file,omitempty"`
	MergedBytes int64                 `json:"merged_bytes,omitempty"`
	PublishedAt time.Time             `json:"published_at,omitempty"`
}

// rollDisklessMetadata bounds the S3 metastore's hot head object by archiving
// compaction-final refs into immutable checkpoints. Run by the partition leader
// each maintenance tick; no-ops when there is nothing to archive.
func (s *Server) rollDisklessMetadata(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity) {
	if s.disklessMeta == nil {
		return
	}
	targetBytes := int64(0)
	if s.cfg.Diskless.Compaction.Enabled {
		targetBytes = s.cfg.Diskless.Compaction.TargetBytesValue()
	}
	if _, err := s.disklessMeta.ArchiveCommitted(ctx, tc.Name, identity.Partition, targetBytes, time.Now().Add(-tc.Retention)); err != nil {
		slog.Warn("diskless_archive_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
	}
}

// discoverDisklessSegmentMergeJobs merges contiguous runs of small committed
// segments below the committed watermark. It never advances the watermark and
// only touches refs older than the grace period.
func (s *Server) discoverDisklessSegmentMergeJobs(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity, jobs []PartitionJob) {
	cfg := s.cfg.Diskless.Compaction
	if !cfg.Enabled || s.disklessMeta == nil {
		return
	}
	active, err := s.hasActiveSegmentMergeJob(ctx, identity, jobs)
	if err != nil {
		slog.Warn("diskless_merge_stale_cleanup_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}
	if active {
		return
	}
	committed, err := s.disklessMeta.GetCommittedHead(ctx, tc.Name, identity.Partition)
	if err != nil {
		slog.Warn("diskless_merge_committed_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}
	if committed <= 0 {
		return
	}
	target := cfg.TargetBytesValue()
	// Query refs from the partition start without a byte cap. A single ref can
	// exceed target (e.g. the merged object of a prior run), and capping the
	// query at target bytes would return only that oversized ref, hiding the
	// small refs behind it and stalling compaction. The refs below the committed
	// watermark stay bounded because compaction replaces whole runs with one ref.
	refs, err := s.disklessMeta.QuerySegments(ctx, tc.Name, identity.Partition, 0, math.MaxInt)
	if err != nil {
		slog.Warn("diskless_merge_query_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}
	grace, err := cfg.GraceDuration()
	if err != nil {
		slog.Warn("diskless_merge_grace_invalid", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}
	graceCutoff := time.Now().Add(-grace)
	retentionCutoff := time.Now().Add(-tc.Retention)
	maxSegments := s.effectiveDisklessMergeMaxSegments(cfg)

	var run []diskless.SegmentRef
	var total int64
	for _, ref := range refs {
		if ref.EndOffset > committed {
			break // beyond the committed watermark
		}
		if !ref.CreatedAt.After(retentionCutoff) {
			// Retention will delete this ref and its data in the same tick;
			// never merge a retention-pending source.
			if len(run) > 0 {
				break // retention-pending ref inside the run: stop.
			}
			continue // skip the retention-pending prefix
		}
		if ref.CreatedAt.After(graceCutoff) {
			break // too recent; stop the run
		}
		if ref.ByteLength >= target {
			// Already compaction-sized: skip an oversized prefix so a prior
			// run's merged object never blocks the small refs behind it, and
			// terminate a run that reaches one.
			if len(run) > 0 {
				break
			}
			continue
		}
		if len(run) > 0 && ref.BaseOffset != run[len(run)-1].EndOffset {
			break // gap; stop the run
		}
		run = append(run, ref)
		total += ref.ByteLength
		if len(run) >= maxSegments || total >= target {
			break
		}
	}
	if len(run) < cfg.MinSegmentsValue() {
		return
	}
	job, err := buildDisklessMergeJob(tc.Name, identity.Partition, identity, run)
	if err != nil {
		slog.Warn("diskless_merge_job_build_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}
	if err := s.putPartitionJob(ctx, job); err != nil {
		slog.Warn("diskless_merge_enqueue_failed", "topic", tc.Name, "partition", identity.Partition, "job", job.ID, "error", err)
	}
}

// maxDisklessMergeSegmentsUnbounded is the per-run file-count safety cap for
// metastores without a transaction item limit (S3 head CAS, in-memory). It sits
// far above the default target/file-size ratio so the byte target normally
// bounds the run and a merged chunk reaches target in one pass, while still
// bounding pathological runs of very small files.
const maxDisklessMergeSegmentsUnbounded = 4096

// effectiveDisklessMergeMaxSegments returns the per-run file-count cap. The
// configured default (90) exists to fit DynamoDB's 100-item TransactWriteItems
// limit. Metastores without that limit can merge a full target-sized run in one
// pass, which is what makes a merged chunk immediately byte-final so compaction
// never re-reads it. An explicit config override is honored, and clamped to the
// DynamoDB limit when that metastore is in use.
func (s *Server) effectiveDisklessMergeMaxSegments(cfg config.CompactionConfig) int {
	if limited, ok := s.disklessMeta.(diskless.ReplaceItemLimited); ok {
		max := cfg.MaxSegmentsPerMergeValue()
		if limit := limited.ReplaceItemLimit() - 1; max > limit {
			return limit
		}
		return max
	}
	if max := cfg.MaxSegmentsPerMerge; max > 0 {
		return max
	}
	return maxDisklessMergeSegmentsUnbounded
}

func buildDisklessMergeJob(topic string, partition int, identity PartitionIdentity, refs []diskless.SegmentRef) (PartitionJob, error) {
	payload, err := json.Marshal(DisklessMergePayload{
		StorageMode: meta.StorageModeDiskless,
		Sources:     refs,
	})
	if err != nil {
		return PartitionJob{}, fmt.Errorf("marshal diskless merge payload: %w", err)
	}
	jobID := partitionJobID(PartitionJobTypeSegmentMerge, fmt.Sprintf("%s/%d/%d-%d", topic, partition, refs[0].BaseOffset, refs[len(refs)-1].EndOffset))
	return PartitionJob{
		ID:            jobID,
		Topic:         topic,
		Partition:     partition,
		Type:          PartitionJobTypeSegmentMerge,
		ExpectedOwner: identity.Leader,
		ExpectedEpoch: identity.LeaderEpoch,
		State:         PartitionJobStatePending,
		Phase:         PartitionJobPhasePublishData,
		Payload:       payload,
	}, nil
}

// runDisklessSegmentMergeJob executes the phases publish_data -> publish_meta
// (replace refs) -> delete_data (after the delete grace). Compaction changes
// physical layout only: the committed watermark is never advanced.
func (s *Server) runDisklessSegmentMergeJob(ctx context.Context, job PartitionJob) error {
	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}
	var payload DisklessMergePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("decode diskless merge payload: %w", err)
	}
	if payload.StorageMode != meta.StorageModeDiskless || len(payload.Sources) < 2 {
		return fmt.Errorf("invalid diskless merge payload")
	}

	if job.State != PartitionJobStateRunning || job.Phase == "" {
		job.State = PartitionJobStateRunning
		job.Phase = PartitionJobPhasePublishData
		if err := s.putPartitionJob(ctx, job); err != nil {
			return err
		}
	}

	if job.Phase == PartitionJobPhasePublishData {
		artifact, err := s.buildDisklessMergeArtifact(ctx, job.Topic, job.Partition, payload.Sources)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// A source was retained away before this merge published. The
				// merge is moot; drop it so it cannot block later merges.
				slog.Warn("diskless_merge_sources_gone", "topic", job.Topic, "partition", job.Partition, "job", job.ID, "error", err)
				return s.deletePartitionJob(ctx, job.Topic, job.Partition, job.ID)
			}
			return err
		}
		if err := s.s3Client.Put(ctx, artifact.mergedKey, artifact.data, storage.PutOpts{}); err != nil {
			return fmt.Errorf("publish merged diskless object %s: %w", artifact.mergedKey, err)
		}
		payload.MergedFile = artifact.mergedKey
		payload.MergedBytes = int64(len(artifact.data))
		job.Payload, err = json.Marshal(payload)
		if err != nil {
			return err
		}
		job.Phase = PartitionJobPhasePublishMeta
		if err := s.putPartitionJob(ctx, job); err != nil {
			return err
		}
	}

	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}
	if job.Phase == PartitionJobPhasePublishMeta {
		first, last := payload.Sources[0], payload.Sources[len(payload.Sources)-1]
		remove := make([]diskless.RefKey, 0, len(payload.Sources))
		for _, ref := range payload.Sources {
			remove = append(remove, diskless.RefKey{BaseOffset: ref.BaseOffset, EndOffset: ref.EndOffset})
		}
		add := []diskless.SegmentRef{{
			FileKey:    payload.MergedFile,
			ByteOffset: 0,
			ByteLength: payload.MergedBytes,
			BaseOffset: first.BaseOffset,
			EndOffset:  last.EndOffset,
		}}
		if err := s.disklessMeta.ReplaceSegmentRefs(ctx, job.Topic, job.Partition, remove, add); err != nil {
			return fmt.Errorf("publish merged refs %s/%d: %w", job.Topic, job.Partition, err)
		}
		payload.PublishedAt = time.Now()
		var marshalErr error
		job.Payload, marshalErr = json.Marshal(payload)
		if marshalErr != nil {
			return marshalErr
		}
		job.Phase = PartitionJobPhaseDeleteData
		if err := s.putPartitionJob(ctx, job); err != nil {
			return err
		}
	}

	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}
	if job.Phase == PartitionJobPhaseDeleteData {
		// Keep source data for the delete grace so in-flight readers that
		// already parsed the old refs can still range-fetch it.
		deleteGrace, err := s.cfg.Diskless.Compaction.DeleteGraceDuration()
		if err != nil {
			return err
		}
		if time.Since(payload.PublishedAt) < deleteGrace {
			return nil // retried on a later maintenance tick
		}
		fileKeys := make([]string, 0, len(payload.Sources))
		seen := make(map[string]bool, len(payload.Sources))
		for _, ref := range payload.Sources {
			if !seen[ref.FileKey] {
				seen[ref.FileKey] = true
				fileKeys = append(fileKeys, ref.FileKey)
			}
		}
		deletable, err := s.disklessMeta.PlanUnreferencedFileDeletes(ctx, fileKeys)
		if err != nil {
			return err
		}
		for _, fileKey := range deletable {
			if err := s.s3Client.Delete(ctx, fileKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("delete compacted source %s: %w", fileKey, err)
			}
		}
	}
	return s.deletePartitionJob(ctx, job.Topic, job.Partition, job.ID)
}

type disklessMergeArtifact struct {
	mergedKey string
	data      []byte
}

// buildDisklessMergeArtifact concatenates the source byte ranges in offset
// order into a single object. Byte-exact concatenation is safe because diskless
// refs point at raw RecordBatch bytes. The merged buffer is pre-allocated and
// each range is read directly into it, so no intermediate copies are made.
func (s *Server) buildDisklessMergeArtifact(ctx context.Context, topic string, partition int, sources []diskless.SegmentRef) (disklessMergeArtifact, error) {
	for i, ref := range sources {
		if i > 0 && ref.BaseOffset != sources[i-1].EndOffset {
			return disklessMergeArtifact{}, fmt.Errorf("diskless merge requires contiguous sources: %s starts at %d after %d", ref.FileKey, ref.BaseOffset, sources[i-1].EndOffset)
		}
	}
	var total int64
	for _, ref := range sources {
		total += ref.ByteLength
	}
	data := make([]byte, total)
	pos := int64(0)
	for _, ref := range sources {
		if err := s.s3Client.GetRangeInto(ctx, ref.FileKey, ref.ByteOffset, ref.ByteLength, data[pos:pos+ref.ByteLength]); err != nil {
			return disklessMergeArtifact{}, fmt.Errorf("read diskless source %s [%d:%d): %w", ref.FileKey, ref.ByteOffset, ref.ByteOffset+ref.ByteLength, err)
		}
		pos += ref.ByteLength
	}
	first, last := sources[0], sources[len(sources)-1]
	key := fmt.Sprintf("_diskless_merge/%s/%d/%020d-%020d.data", sanitizeTopic(topic), partition, first.BaseOffset, last.EndOffset)
	return disklessMergeArtifact{mergedKey: key, data: data}, nil
}

func sanitizeTopic(topic string) string {
	return strings.ReplaceAll(topic, "/", "_")
}
