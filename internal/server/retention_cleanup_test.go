package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

func cloneTestServerForInstance(t *testing.T, src *Server) *Server {
	t.Helper()

	cfg := *src.cfg
	cfg.Server.InstanceID = "n2"

	s, err := NewWithS3Client(&cfg, src.s3Client)
	if err != nil {
		t.Fatalf("NewWithS3Client() error = %v", err)
	}
	s.registry = coordination.NewRegistry(src.s3Client, "n2", "127.0.0.1:8080", "127.0.0.1:8081", "", "", time.Minute)
	return s
}

func TestClassicRetentionOwnerJobDeletesMetadataLastAndInvalidatesLocalState(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "classic-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "classic",
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, tc.Name, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 3},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 3},
	}
	s.assignmentsMu.Unlock()
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{0: 3}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	segKey := "classic-topic/0/00000000000000000000-9-1.segment"
	idxKey := log.SegmentOffsetIndexKey(segKey)
	metaKey := log.SegmentMetadataKey(segKey)
	segRef := log.SegmentRef{
		BaseOffset:     0,
		EndOffset:      9,
		Epoch:          1,
		Key:            segKey,
		OffsetIndexKey: idxKey,
		MetaKey:        metaKey,
		CreatedAt:      time.Now().Add(-2 * time.Hour),
	}
	segMeta := log.SegmentMetadata{
		BaseOffset:     segRef.BaseOffset,
		EndOffset:      segRef.EndOffset,
		Epoch:          segRef.Epoch,
		SegmentKey:     segKey,
		OffsetIndexKey: idxKey,
		CreatedAt:      segRef.CreatedAt,
	}
	metaData, err := json.Marshal(segMeta)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, item := range []struct {
		key  string
		data []byte
	}{
		{key: segKey, data: []byte("segment")},
		{key: idxKey, data: []byte("index")},
		{key: metaKey, data: metaData},
	} {
		if err := s.s3Client.Put(ctx, item.key, item.data, storage.PutOpts{}); err != nil {
			t.Fatalf("s3Client.Put(%s) error = %v", item.key, err)
		}
	}
	ps := s.partitionManager.GetPartitionState(tc.Name, 0)
	ps.mu.Lock()
	ps.index.Add(segRef)
	ps.mu.Unlock()
	if err := s.partitionManager.GetDiskCache().Put(segKey, []byte("segment")); err != nil {
		t.Fatalf("diskCache.Put(seg) error = %v", err)
	}
	if err := s.partitionManager.GetDiskCache().Put(idxKey, []byte("index")); err != nil {
		t.Fatalf("diskCache.Put(idx) error = %v", err)
	}
	if err := s.partitionManager.GetDiskCache().Put(metaKey, metaData); err != nil {
		t.Fatalf("diskCache.Put(meta) error = %v", err)
	}

	s.runPartitionMaintenance(ctx, []meta.TopicConfig{tc}, nil)

	jobs, err := s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition jobs after maintenance = %+v, want none", jobs)
	}

	for _, key := range []string{segKey, idxKey, metaKey} {
		if _, err := s.s3Client.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected %s to be deleted after owner maintenance, got %v", key, err)
		}
	}
	if s.partitionManager.GetDiskCache().Has(segKey) {
		t.Fatal("expected segment cache entry to be evicted")
	}
	if s.partitionManager.GetDiskCache().Has(idxKey) {
		t.Fatal("expected offset index cache entry to be evicted")
	}
	if s.partitionManager.GetDiskCache().Has(metaKey) {
		t.Fatal("expected metadata cache entry to be evicted")
	}
	ps.mu.RLock()
	_, ok := ps.index.Lookup(0)
	ps.mu.RUnlock()
	if ok {
		t.Fatal("expected local sealed-segment index ref to be removed")
	}
}

func TestClassicRetentionOwnerJobResumesAfterSegmentDataDeleted(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	job := PartitionJob{
		ID:            partitionJobID(PartitionJobTypeRetention, "classic-topic/0/00000000000000000000-9-1.segment"),
		Topic:         "classic-topic",
		Partition:     0,
		Type:          PartitionJobTypeRetention,
		ExpectedOwner: s.instanceID,
		ExpectedEpoch: 4,
		State:         PartitionJobStateRunning,
		Phase:         PartitionJobPhaseDeleteMeta,
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	payload, err := json.Marshal(ClassicRetentionPayload{
		SegmentKey:     "classic-topic/0/00000000000000000000-9-1.segment",
		OffsetIndexKey: "classic-topic/0/00000000000000000000-9-1.offset.idx",
		MetadataKey:    "classic-topic/0/00000000000000000000-9-1.meta.json",
	})
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	job.Payload = payload
	if err := s.assignmentStore.Write(ctx, job.Topic, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 4},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[job.Topic] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 4},
	}
	s.assignmentsMu.Unlock()

	if err := s.putPartitionJob(ctx, job); err != nil {
		t.Fatalf("putPartitionJob() error = %v", err)
	}
	if err := s.s3Client.Put(ctx, "classic-topic/0/00000000000000000000-9-1.meta.json", []byte("{}"), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(meta) error = %v", err)
	}

	jobs, err := s.listPartitionJobs(ctx, job.Topic, job.Partition)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("listPartitionJobs() = %d jobs, want 1", len(jobs))
	}

	if err := s.runClaimedPartitionJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runClaimedPartitionJob() error = %v", err)
	}

	if _, err := s.s3Client.Get(ctx, "classic-topic/0/00000000000000000000-9-1.meta.json"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected classic metadata to be deleted on resume, got %v", err)
	}
	jobs, err = s.listPartitionJobs(ctx, job.Topic, job.Partition)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition jobs after resume = %+v, want none", jobs)
	}
}

// TestDisklessRetentionExportCheckpointGate verifies that diskless retention
// for an exporting topic never deletes refs the Parquet pipeline has not
// exported yet, and proceeds once the checkpoint covers them.
func TestDisklessRetentionExportCheckpointGate(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	ctx := context.Background()

	tc := meta.TopicConfig{Name: "diskless-export", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, tc.Name, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	// Four expired refs [0,1),[1,2),[2,3),[3,4), each in its own data file.
	for i := int64(0); i < 4; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
			BatchID: fmt.Sprintf("batch-%d", i), FileKey: fileKey, Topic: tc.Name, Partition: 0,
			Count: 1, ByteLength: 10, CreatedAt: time.Now().Add(-2 * time.Hour),
		}}); err != nil {
			t.Fatalf("CommitUploadedBatches([%d,%d)) error = %v", i, i+1, err)
		}
	}

	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	generation := uint64(0)
	publishCheckpoint := func(nextOffset uint64) {
		t.Helper()
		generation++
		store := pipeline.NewCheckpointStore(s.s3Client, pipeline.NoFence{})
		if err := store.Publish(ctx, icebergPipelineName, pipeline.Checkpoint{
			SourceTopic: tc.Name, Partition: 0, NextOffset: nextOffset, SourceEpoch: 1,
			Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion, Generation: generation,
		}); err != nil {
			t.Fatalf("publish checkpoint %d: %v", nextOffset, err)
		}
	}

	// No checkpoint yet: nothing is safe to retain.
	s.discoverDisklessRetentionJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("no checkpoint: retention enqueued %d jobs, want 0", len(jobs))
	}

	// Checkpoint covers [0,2): only f0 and f1 are eligible.
	publishCheckpoint(2)
	s.discoverDisklessRetentionJobs(ctx, tc, identity, nil)
	jobs, err = s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("checkpoint 2: retention enqueued %d jobs, want 2", len(jobs))
	}

	// Checkpoint covers the whole log: all four files are eligible.
	publishCheckpoint(4)
	s.discoverDisklessRetentionJobs(ctx, tc, identity, nil)
	jobs, err = s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 4 {
		t.Fatalf("checkpoint 4: retention enqueued %d jobs, want 4", len(jobs))
	}
}

// TestDisklessRetentionSharedFileWaitsForEveryPartition verifies that a shared
// backing object referenced by several partitions is not deleted until every
// exporting partition has exported its refs.
func TestDisklessRetentionSharedFileWaitsForEveryPartition(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	ctx := context.Background()

	tc := meta.TopicConfig{Name: "diskless-shared", Partitions: 2, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless, ExportEnabled: true}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, tc.Name, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
			1: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 1},
		1: {Owned: true, LeaderEpoch: 1},
	}
	s.assignmentsMu.Unlock()

	// One shared file holds refs for both partitions [0,2), both old enough to
	// be retained.
	for _, batch := range []diskless.UploadedBatch{
		{BatchID: "shared-0", FileKey: "_diskless/test-node/shared.data", Topic: tc.Name, Partition: 0, Count: 2, ByteLength: 10, CreatedAt: time.Now().Add(-2 * time.Hour)},
		{BatchID: "shared-1", FileKey: "_diskless/test-node/shared.data", Topic: tc.Name, Partition: 1, Count: 2, ByteOffset: 10, ByteLength: 10, CreatedAt: time.Now().Add(-2 * time.Hour)},
	} {
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{batch}); err != nil {
			t.Fatalf("CommitUploadedBatches() error = %v", err)
		}
	}

	identity := PartitionIdentity{Topic: tc.Name, Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	publishCheckpoint := func(partition int) {
		t.Helper()
		store := pipeline.NewCheckpointStore(s.s3Client, pipeline.NoFence{})
		if err := store.Publish(ctx, icebergPipelineName, pipeline.Checkpoint{
			SourceTopic: tc.Name, Partition: partition, NextOffset: 2, SourceEpoch: 1,
			Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion, Generation: 1,
		}); err != nil {
			t.Fatalf("publish checkpoint for partition %d: %v", partition, err)
		}
	}

	// Partition 0 exported, partition 1 not: the shared file must be kept.
	publishCheckpoint(0)
	s.discoverDisklessRetentionJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition 1 unexported: retention enqueued %d jobs, want 0", len(jobs))
	}

	// Both partitions exported: the shared file may be retained.
	publishCheckpoint(1)
	s.discoverDisklessRetentionJobs(ctx, tc, identity, nil)
	jobs, err = s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("all partitions exported: retention enqueued %d jobs, want 1", len(jobs))
	}
}

func TestDisklessRetentionOwnerJobResumesAfterS3Delete(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	ctx := context.Background()

	if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
		BatchID: "expired", FileKey: "_diskless/test-node/expired.data", Topic: "diskless-topic", Partition: 0,
		Count: 5, ByteLength: 64, CreatedAt: time.Now().Add(-2 * time.Hour),
	}}); err != nil {
		t.Fatalf("CommitUploadedBatches() error = %v", err)
	}
	job := PartitionJob{
		ID:            partitionJobID(PartitionJobTypeRetention, "_diskless/test-node/expired.data"),
		Topic:         "diskless-topic",
		Partition:     0,
		Type:          PartitionJobTypeRetention,
		ExpectedOwner: s.instanceID,
		ExpectedEpoch: 7,
		State:         PartitionJobStateRunning,
		Phase:         PartitionJobPhaseDeleteMeta,
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	payload, err := json.Marshal(ClassicRetentionPayload{
		StorageMode: meta.StorageModeDiskless,
		FileKey:     "_diskless/test-node/expired.data",
	})
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	job.Payload = payload
	if err := s.assignmentStore.Write(ctx, job.Topic, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 7},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[job.Topic] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 7},
	}
	s.assignmentsMu.Unlock()
	if err := s.putPartitionJob(ctx, job); err != nil {
		t.Fatalf("putPartitionJob() error = %v", err)
	}

	jobs, err := s.listPartitionJobs(ctx, job.Topic, job.Partition)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("listPartitionJobs() = %d jobs, want 1", len(jobs))
	}
	if err := s.runClaimedPartitionJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runClaimedPartitionJob() error = %v", err)
	}

	refs, err := s.disklessMeta.QuerySegments(ctx, "diskless-topic", 0, 0, 100)
	if err != nil {
		t.Fatalf("QuerySegments() error = %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("QuerySegments() = %d refs, want 0 after resumed retention cleanup", len(refs))
	}
	jobs, err = s.listPartitionJobs(ctx, job.Topic, job.Partition)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition jobs after resume = %+v, want none", jobs)
	}
}

func TestClassicRetentionOwnerJobResumesAfterReassignment(t *testing.T) {
	s1 := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "classic-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       meta.StorageModeClassic,
	}
	if err := s1.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	metaKey := "classic-topic/0/00000000000000000000-9-1.meta.json"
	metaData, err := json.Marshal(log.SegmentMetadata{
		BaseOffset:     0,
		EndOffset:      9,
		Epoch:          1,
		SegmentKey:     "classic-topic/0/00000000000000000000-9-1.segment",
		OffsetIndexKey: "classic-topic/0/00000000000000000000-9-1.offset.idx",
		CreatedAt:      time.Now().Add(-2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("json.Marshal(meta) error = %v", err)
	}
	if err := s1.s3Client.Put(ctx, metaKey, metaData, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(meta) error = %v", err)
	}
	job := PartitionJob{
		ID:            partitionJobID(PartitionJobTypeRetention, "classic-topic/0/00000000000000000000-9-1.segment"),
		Topic:         tc.Name,
		Partition:     0,
		Type:          PartitionJobTypeRetention,
		ExpectedOwner: s1.instanceID,
		ExpectedEpoch: 1,
		State:         PartitionJobStateRunning,
		Phase:         PartitionJobPhaseDeleteMeta,
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	payload, err := json.Marshal(ClassicRetentionPayload{
		StorageMode:    meta.StorageModeClassic,
		SegmentKey:     "classic-topic/0/00000000000000000000-9-1.segment",
		OffsetIndexKey: "classic-topic/0/00000000000000000000-9-1.offset.idx",
		MetadataKey:    metaKey,
	})
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	job.Payload = payload
	if err := s1.putPartitionJob(ctx, job); err != nil {
		t.Fatalf("putPartitionJob() error = %v", err)
	}

	if err := s1.assignmentStore.Write(ctx, tc.Name, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: "n2", Replicas: []string{"n2"}, LeaderEpoch: 2},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}

	jobs, err := s1.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("listPartitionJobs() = %d jobs, want 1", len(jobs))
	}
	if err := s1.runClaimedPartitionJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runClaimedPartitionJob(old owner) error = %v", err)
	}
	if _, err := s1.s3Client.Get(ctx, metaKey); err != nil {
		t.Fatalf("expected metadata to remain after stale-owner attempt: %v", err)
	}

	s2 := cloneTestServerForInstance(t, s1)
	s2.runPartitionMaintenance(ctx, []meta.TopicConfig{tc}, nil)

	if _, err := s2.s3Client.Get(ctx, metaKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected metadata to be deleted by new owner, got %v", err)
	}
	jobs, err = s2.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition jobs after reassignment resume = %+v, want none", jobs)
	}
}

func TestDisklessRetentionOwnerJobResumesAfterReassignment(t *testing.T) {
	s1 := newTestServer(t)
	s1.disklessMeta = diskless.NewMemoryMetaStore()
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "diskless-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       meta.StorageModeDiskless,
	}
	if err := s1.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if _, err := s1.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
		BatchID: "reassign", FileKey: "_diskless/test-node/reassign.data", Topic: tc.Name, Partition: 0,
		Count: 3, ByteLength: 32, CreatedAt: time.Now().Add(-2 * time.Hour),
	}}); err != nil {
		t.Fatalf("CommitUploadedBatches() error = %v", err)
	}

	job := PartitionJob{
		ID:            partitionJobID(PartitionJobTypeRetention, "_diskless/test-node/reassign.data"),
		Topic:         tc.Name,
		Partition:     0,
		Type:          PartitionJobTypeRetention,
		ExpectedOwner: s1.instanceID,
		ExpectedEpoch: 1,
		State:         PartitionJobStateRunning,
		Phase:         PartitionJobPhaseDeleteMeta,
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	payload, err := json.Marshal(ClassicRetentionPayload{
		StorageMode: meta.StorageModeDiskless,
		FileKey:     "_diskless/test-node/reassign.data",
	})
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	job.Payload = payload
	if err := s1.putPartitionJob(ctx, job); err != nil {
		t.Fatalf("putPartitionJob() error = %v", err)
	}

	if err := s1.assignmentStore.Write(ctx, tc.Name, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: "n2", Replicas: []string{"n2"}, LeaderEpoch: 2},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}

	jobs, err := s1.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("listPartitionJobs() = %d jobs, want 1", len(jobs))
	}
	if err := s1.runClaimedPartitionJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runClaimedPartitionJob(old owner) error = %v", err)
	}
	refs, err := s1.disklessMeta.QuerySegments(ctx, tc.Name, 0, 0, 100)
	if err != nil {
		t.Fatalf("QuerySegments() error = %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected refs to remain after stale-owner attempt, got %d", len(refs))
	}

	s2 := cloneTestServerForInstance(t, s1)
	s2.disklessMeta = s1.disklessMeta
	s2.runPartitionMaintenance(ctx, []meta.TopicConfig{tc}, nil)

	refs, err = s2.disklessMeta.QuerySegments(ctx, tc.Name, 0, 0, 100)
	if err != nil {
		t.Fatalf("QuerySegments() error = %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("expected refs to be removed by new owner, got %d", len(refs))
	}
	jobs, err = s2.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition jobs after reassignment resume = %+v, want none", jobs)
	}
}

func TestClassicRetentionWaitsForPipelineCheckpoint(t *testing.T) {
	s, tc, identity, segments := setupClassicRetentionGate(t)
	ctx := context.Background()

	// Without a checkpoint, discovery must fail closed and enqueue nothing.
	s.discoverClassicRetentionJobs(ctx, tc, identity)
	jobs, err := s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("jobs without Parquet pipeline checkpoint = %+v, want none", jobs)
	}

	store := pipeline.NewCheckpointStore(s.s3Client, pipeline.NoFence{})
	if err := store.Publish(ctx, icebergPipelineName, pipeline.Checkpoint{SourceTopic: tc.Name, Partition: 0, NextOffset: 10, Sink: icebergPipelineName, SinkVersion: icebergPipelineVersion, Generation: 1}); err != nil {
		t.Fatalf("publish parquet pipeline checkpoint: %v", err)
	}
	s.discoverClassicRetentionJobs(ctx, tc, identity)
	jobs, err = s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs with checkpoint through first segment = %+v, want one", jobs)
	}
	if err := s.runRetentionJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runRetentionJob(covered) error = %v", err)
	}
	if _, err := s.s3Client.Get(ctx, segments[0].segmentKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("covered segment was not removed: %v", err)
	}
	if _, err := s.s3Client.Get(ctx, segments[1].segmentKey); err != nil {
		t.Fatalf("uncovered segment was removed: %v", err)
	}
}

func TestClassicRetentionRechecksStaleJobBeforeDeleting(t *testing.T) {
	s, tc, identity, segments := setupClassicRetentionGate(t)
	ctx := context.Background()

	// This models a job queued before the checkpoint became stale. Omit
	// EndOffset to also verify compatibility with pre-gate job payloads.
	payload, err := json.Marshal(ClassicRetentionPayload{
		StorageMode:    meta.StorageModeClassic,
		SegmentKey:     segments[1].segmentKey,
		OffsetIndexKey: segments[1].indexKey,
		MetadataKey:    segments[1].metaKey,
	})
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	job := PartitionJob{
		ID:            partitionJobID(PartitionJobTypeRetention, segments[1].segmentKey),
		Topic:         tc.Name,
		Partition:     0,
		Type:          PartitionJobTypeRetention,
		ExpectedOwner: identity.Leader,
		ExpectedEpoch: identity.LeaderEpoch,
		State:         PartitionJobStatePending,
		Phase:         PartitionJobPhaseDeleteData,
		Payload:       payload,
	}
	if err := s.putPartitionJob(ctx, job); err != nil {
		t.Fatalf("putPartitionJob() error = %v", err)
	}
	err = s.runRetentionJob(ctx, job)
	if !errors.Is(err, errRetentionAwaitingParquetExport) {
		t.Fatalf("runRetentionJob() error = %v, want parquet export block", err)
	}
	if _, err := s.s3Client.Get(ctx, segments[1].segmentKey); err != nil {
		t.Fatalf("blocked retention deleted segment: %v", err)
	}
	jobs, err := s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != PartitionJobStatePending {
		t.Fatalf("blocked job = %+v, want retained pending job", jobs)
	}
}

type retentionGateSegment struct {
	segmentKey string
	indexKey   string
	metaKey    string
}

func setupClassicRetentionGate(t *testing.T) (*Server, meta.TopicConfig, PartitionIdentity, []retentionGateSegment) {
	t.Helper()
	s := newTestServer(t)
	ctx := context.Background()
	tc := meta.TopicConfig{
		Name: "parquet-retention", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(),
		ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeClassic,
		ExportEnabled: true,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	identity := PartitionIdentity{Partition: 0, Leader: s.instanceID, LeaderEpoch: 1}
	if err := s.assignmentStore.Write(ctx, tc.Name, coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{0: {
			Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: identity.LeaderEpoch,
		}},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: identity.LeaderEpoch}}
	s.assignmentsMu.Unlock()

	segments := make([]retentionGateSegment, 0, 2)
	for _, end := range []uint64{9, 19} {
		segmentKey := fmt.Sprintf("%s/0/00000000000000000000-%d-1.segment", tc.Name, end)
		indexKey := log.SegmentOffsetIndexKey(segmentKey)
		metaKey := log.SegmentMetadataKey(segmentKey)
		metaData, err := json.Marshal(log.SegmentMetadata{
			BaseOffset: end - 9, EndOffset: end, Epoch: 1, SegmentKey: segmentKey,
			OffsetIndexKey: indexKey, CreatedAt: time.Now().Add(-2 * time.Hour),
		})
		if err != nil {
			t.Fatalf("json.Marshal(segment metadata) error = %v", err)
		}
		for _, item := range []struct {
			key  string
			data []byte
		}{
			{segmentKey, []byte("segment")}, {indexKey, []byte("index")}, {metaKey, metaData},
		} {
			if err := s.s3Client.Put(ctx, item.key, item.data, storage.PutOpts{}); err != nil {
				t.Fatalf("s3Client.Put(%s) error = %v", item.key, err)
			}
		}
		segments = append(segments, retentionGateSegment{segmentKey, indexKey, metaKey})
	}
	return s, tc, identity, segments
}
