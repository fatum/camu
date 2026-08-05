package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

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

// TestDisklessSegmentMergeAdvancesPastOversizedRef verifies that discovery does
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
