package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

// TestSweepDisklessOrphansCoversDataAndMergePrefixes verifies the orphan sweep
// reclaims unreferenced objects under both _diskless/ (per-flush uploads) and
// _diskless_merge/ (compaction artifacts, including a deleted topic's merged
// data) while keeping referenced objects.
func TestSweepDisklessOrphansCoversDataAndMergePrefixes(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	ctx := context.Background()

	oldGrace := disklessOrphanGrace
	disklessOrphanGrace = -time.Second
	defer func() { disklessOrphanGrace = oldGrace }()

	// One referenced data object: a committed ref points at it.
	if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
		BatchID: "ref:0:10", FileKey: "_diskless/node1/ref.data", Topic: "t", Partition: 0,
		Count: 1, ByteLength: 10, CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("CommitUploadedBatches() error = %v", err)
	}
	referenced := "_diskless/node1/ref.data"
	orphanData := "_diskless/node1/orphan.data"
	orphanMerge := "_diskless_merge/t/0/orphan.data"
	for _, key := range []string{referenced, orphanData, orphanMerge} {
		if err := s.s3Client.Put(ctx, key, []byte("x"), storage.PutOpts{}); err != nil {
			t.Fatalf("s3Client.Put(%s) error = %v", key, err)
		}
	}

	s.sweepDisklessOrphans(ctx, s.buildDisklessFileIndex(ctx))

	if _, err := s.s3Client.Get(ctx, referenced); err != nil {
		t.Fatalf("referenced object %s must survive the sweep, got %v", referenced, err)
	}
	for _, key := range []string{orphanData, orphanMerge} {
		if _, err := s.s3Client.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("orphan %s must be swept, got %v", key, err)
		}
	}
}

// TestSweepDisklessArchiveOrphansUsesIndex verifies the checkpoint sweep driven
// by the shared per-pass index removes unreferenced checkpoint objects.
func TestSweepDisklessArchiveOrphansUsesIndex(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	ctx := context.Background()

	oldGrace := disklessOrphanGrace
	disklessOrphanGrace = -time.Second
	defer func() { disklessOrphanGrace = oldGrace }()

	// A live checkpoint (referenced via a head's archive chain).
	if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
		BatchID: "ref:0:10", FileKey: "_diskless/node1/ref.data", Topic: "t", Partition: 0,
		Count: 1, ByteLength: 100, CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("CommitUploadedBatches() error = %v", err)
	}
	// Commit enough data to exceed the head window (128KiB of refs) so the
	// archive job rolls refs into a checkpoint.
	for i := 0; i < 140; i++ {
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
			BatchID: fmt.Sprintf("ref-%d:0:1000", i), FileKey: fmt.Sprintf("_diskless/node1/ref-%d.data", i),
			Topic: "t", Partition: 0, Count: 1, ByteLength: 1000, CreatedAt: time.Now(),
		}}); err != nil {
			t.Fatalf("CommitUploadedBatches() error = %v", err)
		}
	}
	if n, err := s.disklessMeta.ArchiveCommitted(ctx, "t", 0, 10, time.Now().Add(-time.Hour)); err != nil || n == 0 {
		t.Fatalf("ArchiveCommitted() = %d, %v; want >0", n, err)
	}
	// A stray checkpoint object (from a lost head CAS).
	if err := s.s3Client.Put(ctx, "_diskless_meta/archive/t/0/00000000000000000099.json", []byte(`{"version":1,"end":99}`), storage.PutOpts{}); err != nil {
		t.Fatalf("put stray checkpoint: %v", err)
	}

	s.sweepDisklessArchiveOrphans(ctx, s.buildDisklessFileIndex(ctx))

	// The referenced checkpoint survives; the stray is deleted.
	orphans, err := s.s3Client.List(ctx, "_diskless_meta/archive/t/0/")
	if err != nil {
		t.Fatalf("list archive: %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("archive after sweep = %v, want only the live checkpoint", orphans)
	}
}

func TestDeleteTopicEnqueuesAsyncDisklessCleanupAndPreservesMetaUntilS3Deleted(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()

	ctx := context.Background()
	tc := meta.TopicConfig{
		Name:              "diskless-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "diskless",
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
		BatchID: "delete-segment", FileKey: tc.Name + "/0/segment.data", Topic: tc.Name, Partition: 0,
		Count: 5, ByteLength: 100, CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("CommitUploadedBatches() error = %v", err)
	}
	for _, key := range []string{
		tc.Name + "/0/segment.data",
		"_coordination/assignments/" + tc.Name + ".json",
		"_coordination/epochs/" + tc.Name + "/0.json",
		"warehouse/" + tc.Name + "/metadata/version-hint.text",
	} {
		if err := s.s3Client.Put(ctx, key, []byte("x"), storage.PutOpts{}); err != nil {
			t.Fatalf("s3Client.Put(%s) error = %v", key, err)
		}
	}

	if err := s.deleteTopic(ctx, tc.Name); err != nil {
		t.Fatalf("deleteTopic() error = %v", err)
	}

	if _, err := s.topicStore.Get(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("topicStore.Get(after enqueue) error = %v, want ErrNotFound", err)
	}
	if err := s.getTopicDeletion(ctx, tc.Name); err != nil {
		t.Fatalf("getTopicDeletion() error = %v, want marker to remain", err)
	}
	if _, err := s.s3Client.Get(ctx, tc.Name+"/0/segment.data"); err != nil {
		t.Fatalf("expected topic S3 data to remain before GC: %v", err)
	}
	head, err := s.disklessMeta.GetPartitionHead(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("GetPartitionHead(before GC) error = %v", err)
	}
	if head != 5 {
		t.Fatalf("GetPartitionHead(before GC) = %d, want 5", head)
	}
	refs, err := s.disklessMeta.QuerySegments(ctx, tc.Name, 0, 0, 100)
	if err != nil {
		t.Fatalf("QuerySegments(before GC) error = %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("QuerySegments(before GC) = %d refs, want 1", len(refs))
	}

	s.gcPendingTopicDeletions(ctx)

	if _, err := s.s3Client.Get(ctx, tc.Name+"/0/segment.data"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected topic S3 data to be deleted after GC, got %v", err)
	}
	for _, key := range []string{
		"warehouse/" + tc.Name + "/metadata/version-hint.text",
	} {
		if _, err := s.s3Client.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected iceberg table %q to be deleted after GC, got %v", key, err)
		}
	}
	head, err = s.disklessMeta.GetPartitionHead(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("GetPartitionHead(after GC) error = %v", err)
	}
	if head != 0 {
		t.Fatalf("GetPartitionHead(after GC) = %d, want 0", head)
	}
	refs, err = s.disklessMeta.QuerySegments(ctx, tc.Name, 0, 0, 100)
	if err != nil {
		t.Fatalf("QuerySegments(after GC) error = %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("QuerySegments(after GC) = %d refs, want 0", len(refs))
	}
	if err := s.getTopicDeletion(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected topic deletion marker to be removed, got %v", err)
	}
}

func TestTopicDeletionGCResumesFromMarkerAfterRestart(t *testing.T) {
	s1 := newTestServer(t)
	ms := diskless.NewMemoryMetaStore()
	s1.disklessMeta = ms

	ctx := context.Background()
	tc := meta.TopicConfig{
		Name:              "diskless-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "diskless",
	}
	if err := s1.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if _, err := ms.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
		BatchID: "restart-segment", FileKey: tc.Name + "/0/segment.data", Topic: tc.Name, Partition: 0,
		Count: 1, ByteLength: 10, CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("CommitUploadedBatches() error = %v", err)
	}
	if err := s1.s3Client.Put(ctx, tc.Name+"/0/segment.data", []byte("x"), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put() error = %v", err)
	}
	if err := s1.enqueueTopicDeletion(ctx, tc); err != nil {
		t.Fatalf("enqueueTopicDeletion() error = %v", err)
	}

	s2, err := NewWithS3Client(s1.cfg, s1.s3Client)
	if err != nil {
		t.Fatalf("NewWithS3Client() restart error = %v", err)
	}
	s2.registry = coordination.NewRegistry(s1.s3Client, s1.cfg.Server.InstanceID, "127.0.0.1:8080", "127.0.0.1:8081", "", "", time.Minute)
	s2.disklessMeta = ms

	s2.gcPendingTopicDeletions(ctx)

	if _, err := s2.s3Client.Get(ctx, tc.Name+"/0/segment.data"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected topic S3 data to be deleted after resumed GC, got %v", err)
	}
	head, err := ms.GetPartitionHead(ctx, tc.Name, 0)
	if err != nil {
		t.Fatalf("GetPartitionHead(after resumed GC) error = %v", err)
	}
	if head != 0 {
		t.Fatalf("GetPartitionHead(after resumed GC) = %d, want 0", head)
	}
	if err := s2.getTopicDeletion(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected deletion marker to be cleared after resumed GC, got %v", err)
	}
}

// TestTopicDeletionGCRemovesRegistryEntryAfterCrashWindow verifies the leader
// GC also deletes the registry entry: enqueueTopicDeletion writes the marker and
// then deletes the registry, so a crash in between would otherwise leave a
// registered topic whose data has been erased.
func TestTopicDeletionGCRemovesRegistryEntryAfterCrashWindow(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "crash-window",
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
	// Simulate the crash: the deletion marker was written but the registry
	// delete in enqueueTopicDeletion never ran.
	if err := s.putTopicDeletion(ctx, topicDeletionRecord{Topic: tc, StartedAt: time.Now()}); err != nil {
		t.Fatalf("putTopicDeletion() error = %v", err)
	}

	s.gcPendingTopicDeletions(ctx)

	if _, err := s.topicStore.Get(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("topicStore.Get(after GC) error = %v, want ErrNotFound", err)
	}
	if err := s.getTopicDeletion(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected deletion marker to be cleared, got %v", err)
	}
}

// TestTopicDeletionAsyncWorkersProcessPendingMarkers verifies the leader's
// async enqueue hands markers to the worker pool, which completes the cleanup
// (data, registry, marker) off the GC tick.
func TestTopicDeletionAsyncWorkersProcessPendingMarkers(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	s.startTopicDeletionWorkers()
	defer s.stopTopicDeletionWorkers()
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "async-del",
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
	if err := s.s3Client.Put(ctx, tc.Name+"/0/segment.data", []byte("x"), storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put() error = %v", err)
	}
	if err := s.putTopicDeletion(ctx, topicDeletionRecord{Topic: tc, StartedAt: time.Now()}); err != nil {
		t.Fatalf("putTopicDeletion() error = %v", err)
	}

	s.enqueueTopicDeletions(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := s.getTopicDeletion(ctx, tc.Name); errors.Is(err, storage.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not complete deletion in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := s.s3Client.Get(ctx, tc.Name+"/0/segment.data"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("topic data not deleted, got %v", err)
	}
	if _, err := s.topicStore.Get(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("registry entry not deleted, got %v", err)
	}
}

// TestDeleteTopicS3DataStreamsLargeTopics verifies a topic with more objects
// than one delete batch is fully removed (the streaming list + batched delete
// path).
func TestDeleteTopicS3DataStreamsLargeTopics(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	for i := 0; i < 1100; i++ {
		key := fmt.Sprintf("big/0/%05d.segment", i)
		if err := s.s3Client.Put(ctx, key, []byte("x"), storage.PutOpts{}); err != nil {
			t.Fatalf("s3Client.Put(%s) error = %v", key, err)
		}
	}
	if err := s.deleteTopicS3Data(ctx, "big"); err != nil {
		t.Fatalf("deleteTopicS3Data() error = %v", err)
	}
	keys, err := s.s3Client.List(ctx, "big/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("remaining keys after delete = %d, want 0", len(keys))
	}
}

// TestPlanExpiredFilesFromIndexMatchesFallback verifies the index-based
// retention planning agrees with the metastore's PlanExpiredFileDeletes.
func TestPlanExpiredFilesFromIndexMatchesFallback(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewS3MetaStore(s.s3Client)
	ctx := context.Background()
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	commit := func(fileKey string, partition int, createdAt time.Time) {
		t.Helper()
		if _, err := s.disklessMeta.CommitUploadedBatches(ctx, []diskless.UploadedBatch{{
			BatchID: fmt.Sprintf("%s-%d:0:100", fileKey, partition), FileKey: fileKey, Topic: "t",
			Partition: partition, Count: 1, ByteLength: 100, CreatedAt: createdAt,
		}}); err != nil {
			t.Fatalf("commit %s p%d: %v", fileKey, partition, err)
		}
	}
	// A: expired in p0, fresh in p1 (not deletable from p0's view)
	// B: expired in p0 only (deletable)
	// C: fresh in p0 (not a candidate)
	commit("A", 0, old)
	commit("A", 1, now)
	commit("B", 0, old)
	commit("C", 0, now)

	idx := s.buildDisklessFileIndex(ctx)
	if idx == nil {
		t.Fatal("expected a file index for the S3 metastore")
	}
	cutoff := now.Add(-24 * time.Hour)
	got := planExpiredFilesFromIndex(idx, "t", 0, cutoff)
	want, err := s.disklessMeta.PlanExpiredFileDeletes(ctx, "t", 0, cutoff)
	if err != nil {
		t.Fatalf("PlanExpiredFileDeletes() error = %v", err)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("index planning = %v, fallback = %v", got, want)
	}
}

func TestCreateTopicRejectsPendingDeletion(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "reused-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "classic",
	}
	if err := s.putTopicDeletion(ctx, topicDeletionRecord{
		Topic:     tc,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("putTopicDeletion() error = %v", err)
	}

	_, err := s.createTopic(ctx, createTopicRequest{
		Name:       tc.Name,
		Partitions: 1,
		Retention:  time.Hour.String(),
	})
	if err == nil || err.Error() != `topic "reused-topic" deletion in progress` {
		t.Fatalf("createTopic() error = %v, want deletion-in-progress conflict", err)
	}
}

func TestHandleConsumeLowLevelRejectsDeletedTopicDespiteStaleRuntime(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "gone-topic",
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
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = map[int]localPartitionAssignment{0: {Owned: true}}
	s.assignmentsMu.Unlock()

	if err := s.topicStore.Delete(ctx, tc.Name); err != nil {
		t.Fatalf("topicStore.Delete() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/gone-topic/partitions/0/messages", nil)
	req.SetPathValue("topic", tc.Name)
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()
	s.handleConsumeLowLevel(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("handleConsumeLowLevel() code = %d, want 404; body=%s", rec.Code, rec.Body.String())
	}
}
