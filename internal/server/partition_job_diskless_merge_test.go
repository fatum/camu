package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

// stubLimitedMetaStore implements diskless.MetaStore plus the item-transaction
// limit of the DynamoDB metastore, so the effective merge cap can be tested
// without a live DynamoDB. No methods are exercised by those tests.
type stubLimitedMetaStore struct{ limit int }

func (s *stubLimitedMetaStore) ReplaceItemLimit() int { return s.limit }
func (s *stubLimitedMetaStore) CommitUploadedBatches(context.Context, []diskless.UploadedBatch) ([]diskless.OffsetResult, error) {
	return nil, nil
}
func (s *stubLimitedMetaStore) QuerySegments(context.Context, string, int, int64, int) ([]diskless.SegmentRef, error) {
	return nil, nil
}
func (s *stubLimitedMetaStore) ReplaceSegmentRefs(context.Context, string, int, []diskless.RefKey, []diskless.SegmentRef) error {
	return nil
}
func (s *stubLimitedMetaStore) GetPartitionHead(context.Context, string, int) (int64, error) {
	return 0, nil
}
func (s *stubLimitedMetaStore) GetCommittedHead(context.Context, string, int) (int64, error) {
	return 0, nil
}
func (s *stubLimitedMetaStore) GetPartitionStart(context.Context, string, int) (int64, error) {
	return 0, nil
}
func (s *stubLimitedMetaStore) PlanExpiredFileDeletes(context.Context, string, int, time.Time) ([]string, error) {
	return nil, nil
}
func (s *stubLimitedMetaStore) DeleteFileRefs(context.Context, string) error { return nil }
func (s *stubLimitedMetaStore) ListFileRefs(context.Context, string) ([]diskless.FileRef, error) {
	return nil, nil
}
func (s *stubLimitedMetaStore) PlanUnreferencedFileDeletes(context.Context, []string) ([]string, error) {
	return nil, nil
}
func (s *stubLimitedMetaStore) ArchiveCommitted(context.Context, string, int, int64, time.Time) (int, error) {
	return 0, nil
}
func (s *stubLimitedMetaStore) DeleteTopic(context.Context, string) error { return nil }
func (s *stubLimitedMetaStore) Close() error                              { return nil }

// TestDisklessSegmentMerge verifies the full compaction flow: discovery of a
// contiguous committed run, publication of a merged ref, and deletion of the
// now-unreferenced source data — without moving the committed watermark.
func TestDisklessSegmentMerge(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 1 << 20

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	var sourceData []byte
	for i := 0; i < 4; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		sourceData = append(sourceData, data...)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
	}
	committed, err := s.disklessMeta.GetCommittedHead(ctx, "t", 0)
	if err != nil {
		t.Fatalf("committed head: %v", err)
	}
	if committed != 4 {
		t.Fatalf("committed = %d, want 4", committed)
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 diskless merge job, got %d", len(jobs))
	}
	if err := s.runSegmentMergeJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runSegmentMergeJob() error = %v", err)
	}

	refs, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 1 || refs[0].BaseOffset != 0 || refs[0].EndOffset != 4 {
		t.Fatalf("refs after compaction = %+v, want single merged [0,4)", refs)
	}
	committed, err = s.disklessMeta.GetCommittedHead(ctx, "t", 0)
	if err != nil {
		t.Fatalf("committed head: %v", err)
	}
	if committed != 4 {
		t.Fatalf("committed after compaction = %d, want 4 (compaction must not move the watermark)", committed)
	}

	merged, err := s.s3Client.Get(ctx, refs[0].FileKey)
	if err != nil {
		t.Fatalf("merged object %s missing: %v", refs[0].FileKey, err)
	}
	if !bytes.Equal(merged, sourceData) {
		t.Fatalf("merged bytes mismatch: got %d bytes, want %d", len(merged), len(sourceData))
	}

	for i := 0; i < 4; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		if _, err := s.s3Client.Get(ctx, fileKey); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected source %s to be deleted, got %v", fileKey, err)
		}
	}
}

// TestDisklessSegmentMergeStaleJobUnblocksDiscovery verifies that an orphaned
// merge job whose expected owner or epoch no longer matches the current leader
// (e.g. leadership moved after a restart) is deleted during discovery instead
// of permanently blocking compaction, and that a fresh job is created. A valid
// merge job for the current leader still blocks discovery.
func TestDisklessSegmentMergeStaleJobUnblocksDiscovery(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 1 << 20

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	// The current leader identity is (self, epoch 2) — higher than the stale
	// job's epoch to model leadership having moved after a restart.
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 2},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 2}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	for i := 0; i < 4; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
	}
	refs, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 4 {
		t.Fatalf("setup: expected 4 refs, got %d", len(refs))
	}

	// Seed a stale merge job owned by an old leader at an old epoch.
	stale, err := buildDisklessMergeJob("t", 0, PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: "old-leader", LeaderEpoch: 1}, refs)
	if err != nil {
		t.Fatalf("build stale merge job: %v", err)
	}
	if err := s.putPartitionJob(ctx, stale); err != nil {
		t.Fatalf("seed stale job: %v", err)
	}

	// Discovery with the current identity must delete the stale job and create
	// a fresh one owned by the current leader.
	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 2}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, []PartitionJob{stale})
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 fresh merge job after stale cleanup, got %d", len(jobs))
	}
	if jobs[0].ExpectedOwner != s.instanceID || jobs[0].ExpectedEpoch != 2 {
		t.Fatalf("fresh job owner = %s/%d, want %s/2", jobs[0].ExpectedOwner, jobs[0].ExpectedEpoch, s.instanceID)
	}

	// A now-current merge job must block further discovery (no duplicate job).
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, jobs)
	jobs, err = s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 merge job while one is in flight, got %d", len(jobs))
	}
}

// not stall when a ref already exceeds the target size (the merged object of a
// prior run, for instance): the oversized ref is treated as a boundary and the
// small refs behind it are still merged.
func TestDisklessSegmentMergeAdvancesPastOversizedRef(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 1 << 16

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	// First ref is already larger than the 64KiB target; the four following
	// refs are small and must still be picked up.
	if err := s.s3Client.Put(ctx, "_diskless/test-node/big.data", bytes.Repeat([]byte{'b'}, 1<<16+1), storage.PutOpts{}); err != nil {
		t.Fatalf("put big source: %v", err)
	}
	if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: "big:0", FileKey: "_diskless/test-node/big.data", Topic: "t", Partition: 0, Count: 1, ByteLength: 1<<16 + 1, CreatedAt: now}}); err != nil {
		t.Fatalf("commit big [0,1): %v", err)
	}
	var smallData []byte
	for i := 0; i < 4; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		smallData = append(smallData, data...)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i+1, i+2, err)
		}
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 diskless merge job, got %d", len(jobs))
	}
	payload := struct {
		Sources []diskless.SegmentRef `json:"sources"`
	}{}
	if err := json.Unmarshal(jobs[0].Payload, &payload); err != nil {
		t.Fatalf("decode job payload: %v", err)
	}
	if len(payload.Sources) != 4 || payload.Sources[0].BaseOffset != 1 {
		t.Fatalf("merge job sources = %+v, want the 4 small refs starting at offset 1", payload.Sources)
	}
	if err := s.runSegmentMergeJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runSegmentMergeJob() error = %v", err)
	}

	refs, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("refs after compaction = %+v, want [big] + [merged smalls]", refs)
	}
	if refs[0].FileKey != "_diskless/test-node/big.data" || refs[0].EndOffset != 1 {
		t.Fatalf("refs[0] = %+v, want untouched oversized ref [0,1)", refs[0])
	}
	if refs[1].BaseOffset != 1 || refs[1].EndOffset != 5 {
		t.Fatalf("refs[1] = %+v, want merged [1,5)", refs[1])
	}
	merged, err := s.s3Client.Get(ctx, refs[1].FileKey)
	if err != nil {
		t.Fatalf("merged object %s missing: %v", refs[1].FileKey, err)
	}
	if !bytes.Equal(merged, smallData) {
		t.Fatalf("merged bytes mismatch")
	}
	for i := 0; i < 4; i++ {
		if _, err := s.s3Client.Get(ctx, fmt.Sprintf("_diskless/test-node/f%d.data", i)); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected small source %d to be deleted, got %v", i, err)
		}
	}
	if _, err := s.s3Client.Get(ctx, "_diskless/test-node/big.data"); err != nil {
		t.Fatalf("oversized source must be retained: %v", err)
	}
}

// TestDisklessSegmentMergeSkipsRetentionPendingRefs verifies that merge
// discovery never selects refs that retention will delete in the same tick, so
// the merge cannot race retention and wedge on deleted sources.
func TestDisklessSegmentMergeSkipsRetentionPendingRefs(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 1 << 20

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	register := func(base int64, createdAt time.Time) {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", base)
		data := bytes.Repeat([]byte(fmt.Sprintf("%c", 'a'+base)), 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: createdAt}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", base, base+1, err)
		}
	}

	now := time.Now()
	// [0,4) is old enough for retention; [4,8) is fresh.
	for i := int64(0); i < 4; i++ {
		register(i, now.Add(-2*time.Hour))
	}
	for i := int64(4); i < 8; i++ {
		register(i, now)
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 merge job, got %d", len(jobs))
	}
	var payload DisklessMergePayload
	if err := json.Unmarshal(jobs[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(payload.Sources) != 4 || payload.Sources[0].BaseOffset != 4 || payload.Sources[3].EndOffset != 8 {
		t.Fatalf("merge run = %+v, want fresh [4,8) only, excluding retention-pending [0,4)", payload.Sources)
	}
}

// TestDisklessSegmentMergeDropsJobWhenSourcesRetained verifies that a merge job
// whose source objects were deleted (e.g. by retention) is dropped instead of
// failing forever and blocking later merges.
func TestDisklessSegmentMergeDropsJobWhenSourcesRetained(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 1 << 20

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	var sources []diskless.SegmentRef
	for i := 0; i < 4; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
		sources = append(sources, diskless.SegmentRef{FileKey: fileKey, ByteOffset: 0, ByteLength: int64(len(data)), BaseOffset: int64(i), EndOffset: int64(i) + 1})
	}

	// Retention removes the source data objects before the merge runs.
	for _, ref := range sources {
		if err := s.s3Client.Delete(ctx, ref.FileKey); err != nil {
			t.Fatalf("delete source %s: %v", ref.FileKey, err)
		}
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	job, err := buildDisklessMergeJob(tc.Name, 0, identity, sources)
	if err != nil {
		t.Fatalf("buildDisklessMergeJob() error = %v", err)
	}
	if err := s.putPartitionJob(ctx, job); err != nil {
		t.Fatalf("putPartitionJob() error = %v", err)
	}
	if err := s.runSegmentMergeJob(ctx, job); err != nil {
		t.Fatalf("runSegmentMergeJob() error = %v", err)
	}
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("wedged merge job not dropped: %d job(s) remain", len(jobs))
	}
}

// TestEffectiveDisklessMergeMaxSegments verifies the per-run file cap is
// metastore-aware: the S3 metastore defaults to the unbounded safety cap so a
// run reaches the byte target in one pass (never re-reading a merged chunk),
// while the DynamoDB metastore keeps its transaction-tuned default and clamps
// an explicit override to fit the 100-item limit.
func TestEffectiveDisklessMergeMaxSegments(t *testing.T) {
	s := newTestServer(t)
	cfg := config.CompactionConfig{Enabled: true, TargetBytes: 64 << 20}

	// S3 metastore: no transaction item limit, so the default is the unbounded
	// safety cap and the byte target bounds the run.
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	if got := s.effectiveDisklessMergeMaxSegments(cfg); got != maxDisklessMergeSegmentsUnbounded {
		t.Fatalf("s3 default cap = %d, want %d", got, maxDisklessMergeSegmentsUnbounded)
	}
	cfg.MaxSegmentsPerMerge = 2000
	if got := s.effectiveDisklessMergeMaxSegments(cfg); got != 2000 {
		t.Fatalf("s3 explicit cap = %d, want 2000", got)
	}

	// DynamoDB metastore: DynamoDB-tuned default, clamped to the transaction limit.
	cfg.MaxSegmentsPerMerge = 0
	s.disklessMeta = &stubLimitedMetaStore{limit: 100}
	if got := s.effectiveDisklessMergeMaxSegments(cfg); got != 90 {
		t.Fatalf("dynamo default cap = %d, want 90", got)
	}
	cfg.MaxSegmentsPerMerge = 5000
	if got := s.effectiveDisklessMergeMaxSegments(cfg); got != 99 {
		t.Fatalf("dynamo clamped cap = %d, want 99", got)
	}
}

// TestDisklessSegmentMergeReachesByteTargetInOnePass verifies that on the S3
// metastore a discovery run is bounded by the byte target rather than the
// DynamoDB-tuned 90-file cap, so a merged chunk reaches target in a single
// merge and compaction never re-reads it. With 100-byte refs and a 10,000-byte
// target the run must cover 100 refs, not 90.
func TestDisklessSegmentMergeReachesByteTargetInOnePass(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 10000

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	for i := 0; i < 300; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i%26)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 merge jobs (300 refs in runs of 100), got %d", len(jobs))
	}
	// Each run must reach the byte target in one pass (100 refs, not the
	// DynamoDB-tuned 90-file cap), and the parallel runs must be disjoint and
	// contiguous, jointly covering [0,300).
	prevEnd := int64(0)
	for i, job := range jobs {
		var payload DisklessMergePayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			t.Fatalf("decode merge payload %d: %v", i, err)
		}
		if len(payload.Sources) != 100 {
			t.Fatalf("run %d covers %d sources, want 100 (byte target must bound the run, not the 90-file cap)", i, len(payload.Sources))
		}
		if payload.Sources[0].BaseOffset != prevEnd {
			t.Fatalf("run %d starts at %d, want %d (runs must be disjoint and contiguous)", i, payload.Sources[0].BaseOffset, prevEnd)
		}
		prevEnd = payload.Sources[len(payload.Sources)-1].EndOffset
	}
	if prevEnd != 300 {
		t.Fatalf("runs cover [0,%d), want [0,300)", prevEnd)
	}
}

// TestDisklessSegmentMergeNearTargetFrontierRefDoesNotStall verifies that a
// frontier ref just under the byte target cannot wedge the partition. Such a
// ref fills nearly the whole merge budget, leaving room for only a couple of
// small refs before total crosses target; if the byte budget stopped the run
// there, it would fall below min_segments and discovery would never schedule
// another merge for the partition (reproducing the live-cluster stall where
// merged frontiers froze just under 64 MiB while committed offsets kept
// climbing). The run must extend past the byte target to reach min_segments.
func TestDisklessSegmentMergeNearTargetFrontierRefDoesNotStall(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 10000

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	// Frontier ref lands just under the 10,000-byte target: only two 100-byte
	// refs fit before total crosses target, which would leave a 3-ref run below
	// min_segments without the fix. Four more small refs trail behind it.
	frontier := 9800
	if err := s.s3Client.Put(ctx, "_diskless/test-node/frontier.data", bytes.Repeat([]byte{'f'}, frontier), storage.PutOpts{}); err != nil {
		t.Fatalf("put frontier source: %v", err)
	}
	if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: "frontier:0", FileKey: "_diskless/test-node/frontier.data", Topic: "t", Partition: 0, Count: 1, ByteLength: int64(frontier), CreatedAt: now}}); err != nil {
		t.Fatalf("commit frontier [0,1): %v", err)
	}
	for i := 0; i < 8; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/s%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i+1, i+2, err)
		}
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 merge jobs (frontier run [0,4) + trailing [4,9)), got %d", len(jobs))
	}
	var frontierRun, trailingRun DisklessMergePayload
	for _, job := range jobs {
		var p DisklessMergePayload
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			t.Fatalf("decode merge payload: %v", err)
		}
		if p.Sources[0].BaseOffset == 0 {
			frontierRun = p
		} else {
			trailingRun = p
		}
	}
	// The frontier run must extend past the byte target (9800 + 2x100 already
	// crosses 10000) to gather min_segments refs: frontier + three small refs.
	if len(frontierRun.Sources) != 4 || frontierRun.Sources[0].BaseOffset != 0 || frontierRun.Sources[3].EndOffset != 4 {
		t.Fatalf("frontier run = %+v, want the 4-ref run [0,4)", frontierRun.Sources)
	}
	// The trailing refs form a disjoint run enqueued in the same pass: the
	// pipeline must keep moving instead of freezing at the frontier.
	if len(trailingRun.Sources) != 5 || trailingRun.Sources[0].BaseOffset != 4 || trailingRun.Sources[4].EndOffset != 9 {
		t.Fatalf("trailing run = %+v, want the 5 small refs [4,9)", trailingRun.Sources)
	}
	for _, job := range jobs {
		if err := s.runSegmentMergeJob(ctx, job); err != nil {
			t.Fatalf("runSegmentMergeJob() error = %v", err)
		}
	}
	// The merged [0,4) ref is now >= target (byte-final) and the merged [4,9)
	// is a single small ref below min_segments: two contiguous refs remain.
	refs, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 2 || refs[0].BaseOffset != 0 || refs[0].EndOffset != 4 || refs[1].BaseOffset != 4 || refs[1].EndOffset != 9 {
		t.Fatalf("refs after both merges = %+v, want contiguous [0,4) + [4,9)", refs)
	}
}

// TestDisklessSegmentMergeDeleteDataDoesNotBlockDiscovery verifies that a merge
// job owned by the current leader but already in the delete_data phase (its
// merged ref is published and only source deletion remains) does not stall the
// next merge: discovery proceeds and schedules the following contiguous run
// while the prior job awaits its delete grace. Without this, delete_grace
// (default 5 minutes) caps compaction at one merge per grace window, far below
// sustained production.
func TestDisklessSegmentMergeDeleteDataDoesNotBlockDiscovery(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = time.Hour.String()
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 400

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	for i := 0; i < 12; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 merge jobs for the [0,12) runs, got %d", len(jobs))
	}
	if err := s.runSegmentMergeJob(ctx, jobs[0]); err != nil {
		t.Fatalf("runSegmentMergeJob() error = %v", err)
	}

	// The first job now sits in delete_data (delete grace has not elapsed);
	// the other two are pending with their ranges in flight. Discovery must
	// neither duplicate the in-flight runs nor stall on the delete_data job.
	jobs, err = s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("after first merge: %d jobs, want 3 (delete_data + 2 pending)", len(jobs))
	}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, jobs)
	jobs, err = s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs after re-discovery (delete_data + 2 in-flight, no duplicates), got %d", len(jobs))
	}

	// New eligible refs beyond the in-flight ranges must still be scheduled
	// even while the delete_data job awaits its grace.
	for i := 12; i < 16; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
	}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, jobs)
	jobs, err = s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 4 {
		t.Fatalf("expected delete_data + 2 pending jobs plus a new [12,16) job, got %d", len(jobs))
	}
	var next DisklessMergePayload
	found := false
	for _, job := range jobs {
		if job.Phase == PartitionJobPhaseDeleteData {
			continue
		}
		var p DisklessMergePayload
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			t.Fatalf("decode merge payload: %v", err)
		}
		if p.Sources[0].BaseOffset == 12 {
			next = p
			found = true
		}
	}
	if !found || len(next.Sources) != 4 || next.Sources[0].BaseOffset != 12 || next.Sources[3].EndOffset != 16 {
		t.Fatalf("new run = %+v (found=%t), want the fresh [12,16) run", next.Sources, found)
	}
}

// TestDisklessCompactionLoopCatchUp verifies that the dedicated compaction
// loop pipelines merges back-to-back so compaction converges to byte-final refs
// even though each merge job briefly occupies the partition: after enough ticks
// every source run is merged, the committed watermark is untouched, and every
// source object is deleted.
func TestDisklessCompactionLoopCatchUp(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 400

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	for i := 0; i < 24; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
	}

	// Run ticks until a full pass shrinks the ref count no further: each tick
	// discovers one run, drains it to completion (delete grace is zero), and the
	// next tick schedules the following run.
	refs0, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	lastRefs := len(refs0)
	for iter := 0; iter < 12; iter++ {
		s.runDisklessCompactionForPartition(ctx, tc, 0)
		refsNow, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
		if err != nil {
			t.Fatalf("query segments: %v", err)
		}
		if len(refsNow) == lastRefs {
			break
		}
		lastRefs = len(refsNow)
	}

	committed, err := s.disklessMeta.GetCommittedHead(ctx, "t", 0)
	if err != nil {
		t.Fatalf("GetCommittedHead: %v", err)
	}
	if committed != 24 {
		t.Fatalf("committed after compaction = %d, want 24 (compaction must not move the watermark)", committed)
	}
	refs, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 6 {
		t.Fatalf("refs after compaction = %d, want 6 byte-final refs covering [0,24)", len(refs))
	}
	prevEnd := int64(0)
	for _, ref := range refs {
		if ref.BaseOffset != prevEnd {
			t.Fatalf("non-contiguous refs: %+v", refs)
		}
		if ref.ByteLength < 400 {
			t.Fatalf("ref [%d,%d) = %d bytes, want >= target 400", ref.BaseOffset, ref.EndOffset, ref.ByteLength)
		}
		prevEnd = ref.EndOffset
	}
	if prevEnd != 24 {
		t.Fatalf("compacted range ends at %d, want 24", prevEnd)
	}
	for i := 0; i < 24; i++ {
		if _, err := s.s3Client.Get(ctx, fmt.Sprintf("_diskless/test-node/f%d.data", i)); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected source %d to be deleted, got %v", i, err)
		}
	}
}

// TestDisklessParallelDiscoveryEnqueuesDisjointRuns verifies that a single
// discovery pass partitions a large backlog into multiple disjoint contiguous
// merge jobs (each bounded by the byte target) rather than one serial chain,
// and that re-discovery while those jobs are in flight does not schedule
// overlapping work.
func TestDisklessParallelDiscoveryEnqueuesDisjointRuns(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	s.cfg.Diskless.Compaction.Enabled = true
	s.cfg.Diskless.Compaction.Grace = "0s"
	s.cfg.Diskless.Compaction.DeleteGrace = "0s"
	s.cfg.Diskless.Compaction.MinSegments = 4
	s.cfg.Diskless.Compaction.TargetBytes = 300

	tc := meta.TopicConfig{Name: "t", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "t", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["t"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	now := time.Now()
	// 16 refs of 100 bytes: with a 300-byte target and min_segments 4, each run
	// must extend past the target to gather 4 refs, so one pass enqueues 4
	// disjoint runs ([0,4) [4,8) [8,12) [12,16)) instead of one serial chain.
	for i := 0; i < 16; i++ {
		fileKey := fmt.Sprintf("_diskless/test-node/f%d.data", i)
		data := bytes.Repeat([]byte{byte('a' + i)}, 100)
		if err := s.s3Client.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
			t.Fatalf("put source %s: %v", fileKey, err)
		}
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{BatchID: fmt.Sprintf("%s:0", fileKey), FileKey: fileKey, Topic: "t", Partition: 0, Count: 1, ByteLength: int64(len(data)), CreatedAt: now}}); err != nil {
			t.Fatalf("commit [%d,%d): %v", i, i+1, err)
		}
	}

	identity := PartitionIdentity{Topic: "t", Partition: 0, Role: PartitionRoleLeader, Leader: s.instanceID, LeaderEpoch: 1}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, nil)
	jobs, err := s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 4 {
		t.Fatalf("expected 4 disjoint merge jobs, got %d", len(jobs))
	}
	prevEnd := int64(0)
	for i, job := range jobs {
		var payload DisklessMergePayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			t.Fatalf("decode merge payload %d: %v", i, err)
		}
		if payload.Sources[0].BaseOffset != prevEnd {
			t.Fatalf("job %d starts at %d, want %d (disjoint, contiguous runs)", i, payload.Sources[0].BaseOffset, prevEnd)
		}
		if len(payload.Sources) != 4 {
			t.Fatalf("job %d covers %d sources, want 4 (run extends past the byte target to min_segments)", i, len(payload.Sources))
		}
		prevEnd = payload.Sources[len(payload.Sources)-1].EndOffset
	}
	if prevEnd != 16 {
		t.Fatalf("runs cover [0,%d), want [0,16)", prevEnd)
	}

	// Re-discovery while all four jobs are in flight must not duplicate them.
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, jobs)
	jobs, err = s.listPartitionJobs(ctx, "t", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 4 {
		t.Fatalf("expected still 4 jobs after re-discovery, got %d", len(jobs))
	}

	// Executing the parallel jobs through the compaction loop must converge to
	// contiguous, byte-final merged refs covering [0,16).
	s.runDisklessCompactionForPartition(ctx, tc, 0)
	s.runDisklessCompactionForPartition(ctx, tc, 0)
	refs, err := s.disklessMeta.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 4 {
		t.Fatalf("refs after parallel compaction = %d, want 4 byte-final refs", len(refs))
	}
	prevEnd = 0
	for _, ref := range refs {
		if ref.BaseOffset != prevEnd {
			t.Fatalf("non-contiguous refs: %+v", refs)
		}
		prevEnd = ref.EndOffset
	}
	if prevEnd != 16 {
		t.Fatalf("compacted range ends at %d, want 16", prevEnd)
	}
}
