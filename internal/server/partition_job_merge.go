package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

type SegmentMergePayload struct {
	StorageMode string           `json:"storage_mode,omitempty"`
	Sources     []log.SegmentRef `json:"sources"`
}

type mergedSegmentArtifact struct {
	Ref          log.SegmentRef
	SegmentData  []byte
	SidecarData  []byte
	MetadataData []byte
}

func (s *Server) discoverClassicSegmentMergeJobs(ctx context.Context, tc meta.TopicConfig, identity PartitionIdentity, jobs []PartitionJob) {
	for _, job := range jobs {
		if job.Type == PartitionJobTypeSegmentMerge {
			return
		}
	}

	cutoff := time.Now().Add(-tc.Retention)
	keys, err := s.s3Client.List(ctx, log.ListSegmentPrefix(tc.Name, identity.Partition))
	if err != nil {
		slog.Warn("classic_segment_merge_list_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		return
	}
	var refs []log.SegmentRef
	for _, key := range keys {
		if !strings.HasSuffix(key, ".meta.json") {
			continue
		}
		data, err := s.s3Client.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			slog.Warn("classic_segment_merge_meta_read_failed", "topic", tc.Name, "partition", identity.Partition, "key", key, "error", err)
			continue
		}
		var segMeta log.SegmentMetadata
		if err := json.Unmarshal(data, &segMeta); err != nil {
			slog.Warn("classic_segment_merge_meta_decode_failed", "topic", tc.Name, "partition", identity.Partition, "key", key, "error", err)
			continue
		}
		if !segMeta.CreatedAt.After(cutoff) {
			continue
		}
		refs = append(refs, log.SegmentRef{
			BaseOffset:     segMeta.BaseOffset,
			EndOffset:      segMeta.EndOffset,
			Epoch:          segMeta.Epoch,
			Key:            segMeta.SegmentKey,
			OffsetIndexKey: segMeta.OffsetIndexKey,
			MetaKey:        key,
			MinTimestamp:   segMeta.MinTimestamp,
			MaxTimestamp:   segMeta.MaxTimestamp,
			CreatedAt:      segMeta.CreatedAt,
		})
	}
	if len(refs) < 2 {
		return
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].BaseOffset < refs[j].BaseOffset
	})

	for i := 0; i < len(refs)-1; i++ {
		a, b := refs[i], refs[i+1]
		if a.Epoch != b.Epoch || a.EndOffset+1 != b.BaseOffset {
			continue
		}
		job, err := buildSegmentMergeJob(tc.Name, identity.Partition, identity, []log.SegmentRef{a, b})
		if err != nil {
			slog.Warn("classic_segment_merge_job_build_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
			return
		}
		if err := s.putPartitionJob(ctx, job); err != nil {
			slog.Warn("classic_segment_merge_enqueue_failed", "topic", tc.Name, "partition", identity.Partition, "job", job.ID, "error", err)
		}
		return
	}
}

func (s *Server) buildClassicSegmentMergeArtifact(ctx context.Context, refs []log.SegmentRef) (mergedSegmentArtifact, error) {
	if len(refs) < 2 {
		return mergedSegmentArtifact{}, fmt.Errorf("segment merge requires at least 2 source segments")
	}

	first := refs[0]
	last := refs[len(refs)-1]
	for i, ref := range refs {
		if ref.Epoch != first.Epoch {
			return mergedSegmentArtifact{}, fmt.Errorf("segment merge requires same epoch: %s has epoch %d, want %d", ref.Key, ref.Epoch, first.Epoch)
		}
		if i > 0 && ref.BaseOffset != refs[i-1].EndOffset+1 {
			return mergedSegmentArtifact{}, fmt.Errorf("segment merge requires contiguous offsets: %s starts at %d after %d", ref.Key, ref.BaseOffset, refs[i-1].EndOffset)
		}
	}

	var (
		segmentBuf     bytes.Buffer
		sidecarEntries []log.IndexEntry
		tsEntries      []log.TimestampIndexEntry
		recordCount    int
		minTimestamp   int64
		maxTimestamp   int64
		haveTS         bool
		compression    string
	)

	for _, ref := range refs {
		segData, err := s.partitionManager.readSealedSegmentData(ctx, ref)
		if err != nil {
			return mergedSegmentArtifact{}, fmt.Errorf("read source segment %s: %w", ref.Key, err)
		}
		sidecarData, err := s.partitionManager.readSealedSegmentSidecar(ctx, ref)
		if err != nil {
			return mergedSegmentArtifact{}, fmt.Errorf("read source sidecar %s: %w", ref.OffsetIndexObjectKey(), err)
		}
		metaData, err := s.s3Client.Get(ctx, ref.MetaObjectKey())
		if err != nil {
			return mergedSegmentArtifact{}, fmt.Errorf("read source metadata %s: %w", ref.MetaObjectKey(), err)
		}
		var meta log.SegmentMetadata
		if err := json.Unmarshal(metaData, &meta); err != nil {
			return mergedSegmentArtifact{}, fmt.Errorf("decode source metadata %s: %w", ref.MetaObjectKey(), err)
		}
		if compression == "" {
			compression = meta.Compression
		}

		entries, tsIdx, err := log.ReadSidecar(sidecarData)
		if err != nil {
			return mergedSegmentArtifact{}, fmt.Errorf("read source sidecar entries %s: %w", ref.OffsetIndexObjectKey(), err)
		}
		posBase := int64(segmentBuf.Len())
		for _, entry := range entries {
			entry.Position += posBase
			sidecarEntries = append(sidecarEntries, entry)
			recordCount += int(entry.LastOffset-entry.BaseOffset) + 1
			if !haveTS || entry.FirstTimestamp < minTimestamp {
				minTimestamp = entry.FirstTimestamp
			}
			if !haveTS || entry.MaxTimestamp > maxTimestamp {
				maxTimestamp = entry.MaxTimestamp
			}
			haveTS = true
		}
		tsEntries = append(tsEntries, tsIdx...)
		if _, err := segmentBuf.Write(segData); err != nil {
			return mergedSegmentArtifact{}, fmt.Errorf("append source segment %s: %w", ref.Key, err)
		}
	}

	mergedRef := log.SegmentRef{
		BaseOffset:   first.BaseOffset,
		EndOffset:    last.EndOffset,
		Epoch:        first.Epoch,
		Key:          log.FormatSegmentKey(topicFromSegmentKey(first.Key), partitionFromSegmentKey(first.Key), first.BaseOffset, last.EndOffset, first.Epoch),
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		CreatedAt:    refs[len(refs)-1].CreatedAt,
	}
	mergedRef.OffsetIndexKey = log.SegmentOffsetIndexKey(mergedRef.Key)
	mergedRef.MetaKey = log.SegmentMetadataKey(mergedRef.Key)

	var sidecarBuf bytes.Buffer
	if err := log.WriteSidecar(&sidecarBuf, sidecarEntries, tsEntries); err != nil {
		return mergedSegmentArtifact{}, fmt.Errorf("build merged sidecar: %w", err)
	}
	metadataData, err := log.BuildSegmentMetadata(mergedRef, recordCount, int64(segmentBuf.Len()), compression)
	if err != nil {
		return mergedSegmentArtifact{}, fmt.Errorf("build merged metadata: %w", err)
	}

	return mergedSegmentArtifact{
		Ref:          mergedRef,
		SegmentData:  segmentBuf.Bytes(),
		SidecarData:  sidecarBuf.Bytes(),
		MetadataData: metadataData,
	}, nil
}

func topicFromSegmentKey(key string) string {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func partitionFromSegmentKey(key string) int {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) < 2 {
		return 0
	}
	partition, _ := strconv.Atoi(parts[1])
	return partition
}

func (s *Server) runSegmentMergeJob(ctx context.Context, job PartitionJob) error {
	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}

	var payload SegmentMergePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("decode segment merge payload: %w", err)
	}
	if payload.StorageMode == meta.StorageModeDiskless {
		return fmt.Errorf("segment merge is unsupported for diskless topics")
	}

	job.State = PartitionJobStateRunning
	if job.Phase == "" {
		job.Phase = PartitionJobPhasePublishData
	}
	if err := s.putPartitionJob(ctx, job); err != nil {
		return err
	}

	artifact, err := s.buildClassicSegmentMergeArtifact(ctx, payload.Sources)
	if err != nil {
		return err
	}

	if job.Phase == PartitionJobPhasePublishData {
		for _, item := range []struct {
			key  string
			data []byte
		}{
			{key: artifact.Ref.Key, data: artifact.SegmentData},
			{key: artifact.Ref.OffsetIndexObjectKey(), data: artifact.SidecarData},
		} {
			if err := s.s3Client.Put(ctx, item.key, item.data, storage.PutOpts{}); err != nil {
				return fmt.Errorf("publish merged segment artifact %s: %w", item.key, err)
			}
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
		if err := s.s3Client.Put(ctx, artifact.Ref.MetaObjectKey(), artifact.MetadataData, storage.PutOpts{
			ContentType: "application/json",
		}); err != nil {
			return fmt.Errorf("publish merged segment metadata %s: %w", artifact.Ref.MetaObjectKey(), err)
		}
		oldMetaKeys := make([]string, 0, len(payload.Sources))
		for _, ref := range payload.Sources {
			if key := ref.MetaObjectKey(); key != "" && key != artifact.Ref.MetaObjectKey() {
				oldMetaKeys = append(oldMetaKeys, key)
			}
		}
		for _, key := range oldMetaKeys {
			if err := s.s3Client.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("hide old merged source metadata %s: %w", key, err)
			}
		}
		job.Phase = PartitionJobPhaseDeleteData
		if err := s.putPartitionJob(ctx, job); err != nil {
			return err
		}
	}

	if !s.CanRunOwnerJob(job.Topic, job.Partition, job.ExpectedOwner, job.ExpectedEpoch) {
		return nil
	}

	removeKeys := make([]string, 0, len(payload.Sources)*3)
	for _, ref := range payload.Sources {
		if ref.Key == artifact.Ref.Key {
			continue
		}
		removeKeys = append(removeKeys, ref.Key, ref.OffsetIndexObjectKey(), ref.MetaObjectKey())
	}
	if job.Phase == PartitionJobPhaseDeleteData {
		for _, ref := range payload.Sources {
			for _, key := range []string{ref.Key, ref.OffsetIndexObjectKey()} {
				if key == "" || key == artifact.Ref.Key || key == artifact.Ref.OffsetIndexObjectKey() {
					continue
				}
				if err := s.s3Client.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
					return fmt.Errorf("delete merged source data %s: %w", key, err)
				}
			}
		}
		s.partitionManager.InstallSealedSegment(job.Topic, job.Partition, artifact.Ref, artifact.SegmentData, artifact.SidecarData, artifact.MetadataData, removeKeys...)
	}

	return s.deletePartitionJob(ctx, job.Topic, job.Partition, job.ID)
}

func buildSegmentMergeJob(topic string, partition int, identity PartitionIdentity, refs []log.SegmentRef) (PartitionJob, error) {
	payload, err := json.Marshal(SegmentMergePayload{
		StorageMode: meta.StorageModeClassic,
		Sources:     refs,
	})
	if err != nil {
		return PartitionJob{}, fmt.Errorf("marshal segment merge payload: %w", err)
	}
	jobID := partitionJobID(PartitionJobTypeSegmentMerge, filepath.Base(refs[0].Key)+"-"+filepath.Base(refs[len(refs)-1].Key))
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
