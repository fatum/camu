package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

func TestBuildClassicSegmentMergeArtifactConcatenatesSegmentsAndSidecars(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "orders",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       meta.StorageModeClassic,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{0: 1}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ref1, seg1 := seedSealedSegmentForMerge(t, s, ctx, 0, 1, 1, 2)
	ref2, seg2 := seedSealedSegmentForMerge(t, s, ctx, 2, 3, 1, 2)

	artifact, err := s.buildClassicSegmentMergeArtifact(ctx, []log.SegmentRef{ref1, ref2})
	if err != nil {
		t.Fatalf("buildClassicSegmentMergeArtifact() error = %v", err)
	}

	if artifact.Ref.BaseOffset != 0 || artifact.Ref.EndOffset != 3 {
		t.Fatalf("merged ref offsets = %d..%d, want 0..3", artifact.Ref.BaseOffset, artifact.Ref.EndOffset)
	}
	if !bytes.Equal(artifact.SegmentData, append(seg1, seg2...)) {
		t.Fatal("merged segment data is not concatenated source data")
	}

	entries, _, err := log.ReadSidecar(artifact.SidecarData)
	if err != nil {
		t.Fatalf("ReadSidecar() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("merged sidecar entry count = %d, want 2", len(entries))
	}
	if entries[0].Position != 0 {
		t.Fatalf("first merged sidecar position = %d, want 0", entries[0].Position)
	}
	if entries[1].Position != int64(len(seg1)) {
		t.Fatalf("second merged sidecar position = %d, want %d", entries[1].Position, len(seg1))
	}

	var metaDoc log.SegmentMetadata
	if err := json.Unmarshal(artifact.MetadataData, &metaDoc); err != nil {
		t.Fatalf("json.Unmarshal(metadata) error = %v", err)
	}
	if metaDoc.RecordCount != 4 {
		t.Fatalf("merged metadata record count = %d, want 4", metaDoc.RecordCount)
	}
	if metaDoc.SegmentKey != artifact.Ref.Key {
		t.Fatalf("merged metadata segment key = %q, want %q", metaDoc.SegmentKey, artifact.Ref.Key)
	}

}

func TestBuildClassicSegmentMergeArtifactRejectsEpochMismatch(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	ref1, _ := seedSealedSegmentForMerge(t, s, ctx, 0, 1, 1, 1)
	ref2, _ := seedSealedSegmentForMerge(t, s, ctx, 2, 3, 2, 1)

	if _, err := s.buildClassicSegmentMergeArtifact(ctx, []log.SegmentRef{ref1, ref2}); err == nil {
		t.Fatal("expected epoch mismatch error")
	}
}

func TestRunSegmentMergeJobPublishesMergedSegmentAndRemovesSources(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "orders",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       meta.StorageModeClassic,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{0: 3}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
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

	ref1, _ := seedSealedSegmentForMerge(t, s, ctx, 0, 1, 1, 2)
	ref2, _ := seedSealedSegmentForMerge(t, s, ctx, 2, 3, 1, 2)
	ps := s.partitionManager.GetPartitionState("orders", 0)
	ps.mu.Lock()
	ps.index.Add(ref1)
	ps.index.Add(ref2)
	ps.mu.Unlock()

	identity := PartitionIdentity{
		Topic:       tc.Name,
		Partition:   0,
		Role:        PartitionRoleLeader,
		Leader:      s.instanceID,
		LeaderEpoch: 3,
		StorageMode: meta.StorageModeClassic,
	}
	job, err := buildSegmentMergeJob(tc.Name, 0, identity, []log.SegmentRef{ref1, ref2})
	if err != nil {
		t.Fatalf("buildSegmentMergeJob() error = %v", err)
	}
	artifact, err := s.buildClassicSegmentMergeArtifact(ctx, []log.SegmentRef{ref1, ref2})
	if err != nil {
		t.Fatalf("buildClassicSegmentMergeArtifact() error = %v", err)
	}

	if err := s.runSegmentMergeJob(ctx, job); err != nil {
		t.Fatalf("runSegmentMergeJob() error = %v", err)
	}

	for _, key := range []string{artifact.Ref.Key, artifact.Ref.OffsetIndexObjectKey(), artifact.Ref.MetaObjectKey()} {
		if _, err := s.s3Client.Get(ctx, key); err != nil {
			t.Fatalf("expected merged object %s to exist: %v", key, err)
		}
	}
	for _, key := range []string{
		ref1.Key, ref1.OffsetIndexObjectKey(), ref1.MetaObjectKey(),
		ref2.Key, ref2.OffsetIndexObjectKey(), ref2.MetaObjectKey(),
	} {
		if _, err := s.s3Client.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected source object %s to be removed, got %v", key, err)
		}
	}

	ps.mu.RLock()
	segs := ps.index.SegmentsFrom(0, 10)
	ps.mu.RUnlock()
	if len(segs) != 1 {
		t.Fatalf("local index segments = %d, want 1 merged ref", len(segs))
	}
	if segs[0].Key != artifact.Ref.Key {
		t.Fatalf("local index key = %q, want %q", segs[0].Key, artifact.Ref.Key)
	}

	jobs, err := s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition jobs after segment merge = %+v, want none", jobs)
	}
}

func TestRunPartitionMaintenanceAutoDiscoversAndExecutesClassicSegmentMerge(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "orders",
		Partitions:        1,
		Retention:         24 * time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       meta.StorageModeClassic,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{0: 3}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
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

	ref1, _ := seedSealedSegmentForMerge(t, s, ctx, 0, 1, 1, 2)
	ref2, _ := seedSealedSegmentForMerge(t, s, ctx, 2, 3, 1, 2)
	ps := s.partitionManager.GetPartitionState("orders", 0)
	ps.mu.Lock()
	ps.index.Add(ref1)
	ps.index.Add(ref2)
	ps.mu.Unlock()

	expected, err := s.buildClassicSegmentMergeArtifact(ctx, []log.SegmentRef{ref1, ref2})
	if err != nil {
		t.Fatalf("buildClassicSegmentMergeArtifact() error = %v", err)
	}

	s.runPartitionMaintenance(ctx, []meta.TopicConfig{tc}, nil)

	if _, err := s.s3Client.Get(ctx, expected.Ref.MetaObjectKey()); err != nil {
		t.Fatalf("expected merged metadata to exist: %v", err)
	}
	for _, key := range []string{
		ref1.Key, ref1.OffsetIndexObjectKey(), ref1.MetaObjectKey(),
		ref2.Key, ref2.OffsetIndexObjectKey(), ref2.MetaObjectKey(),
	} {
		if _, err := s.s3Client.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected source object %s to be removed, got %v", key, err)
		}
	}
	jobs, err := s.listPartitionJobs(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("listPartitionJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("partition jobs after auto merge = %+v, want none", jobs)
	}
}

func seedSealedSegmentForMerge(t *testing.T, s *Server, ctx context.Context, baseOffset, endOffset, epoch uint64, batchRecords int) (log.SegmentRef, []byte) {
	t.Helper()

	messages := make([]log.Message, 0, batchRecords)
	for i := 0; i < batchRecords; i++ {
		messages = append(messages, log.Message{
			Offset:    baseOffset + uint64(i),
			Timestamp: time.Now().UnixMilli(),
			Value:     []byte("v"),
		})
	}
	segData := log.EncodeRecordBatch(int64(baseOffset), messages)
	segKey := log.FormatSegmentKey("orders", 0, baseOffset, endOffset, epoch)
	idxKey := log.SegmentOffsetIndexKey(segKey)
	metaKey := log.SegmentMetadataKey(segKey)
	entry := log.IndexEntry{
		BaseOffset:     int64(baseOffset),
		LastOffset:     int64(endOffset),
		Position:       0,
		BatchSize:      int32(len(segData)),
		FirstTimestamp: messages[0].Timestamp,
		MaxTimestamp:   messages[len(messages)-1].Timestamp,
	}
	var sidecar bytes.Buffer
	if err := log.WriteSidecar(&sidecar, []log.IndexEntry{entry}, []log.TimestampIndexEntry{{
		Timestamp:  entry.FirstTimestamp,
		BaseOffset: entry.BaseOffset,
	}}); err != nil {
		t.Fatalf("WriteSidecar() error = %v", err)
	}
	ref := log.SegmentRef{
		BaseOffset:     baseOffset,
		EndOffset:      endOffset,
		Epoch:          epoch,
		Key:            segKey,
		OffsetIndexKey: idxKey,
		MetaKey:        metaKey,
		MinTimestamp:   entry.FirstTimestamp,
		MaxTimestamp:   entry.MaxTimestamp,
		CreatedAt:      time.Now().Add(-2 * time.Hour),
	}
	metaData, err := log.BuildSegmentMetadata(ref, batchRecords, int64(len(segData)), log.CompressionNone)
	if err != nil {
		t.Fatalf("BuildSegmentMetadata() error = %v", err)
	}
	for _, item := range []struct {
		key  string
		data []byte
	}{
		{key: segKey, data: segData},
		{key: idxKey, data: sidecar.Bytes()},
		{key: metaKey, data: metaData},
	} {
		if err := s.s3Client.Put(ctx, item.key, item.data, storage.PutOpts{}); err != nil {
			t.Fatalf("s3Client.Put(%s) error = %v", item.key, err)
		}
	}
	return ref, segData
}
