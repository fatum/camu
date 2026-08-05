package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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

	s.sweepDisklessOrphans(ctx)

	if _, err := s.s3Client.Get(ctx, referenced); err != nil {
		t.Fatalf("referenced object %s must survive the sweep, got %v", referenced, err)
	}
	for _, key := range []string{orphanData, orphanMerge} {
		if _, err := s.s3Client.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("orphan %s must be swept, got %v", key, err)
		}
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
		parquetManifestKey(tc.Name, 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)),
		parquetQueryCatalogTopicKey(tc.Name),
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
	if _, err := s.getTopicDeletion(ctx, tc.Name); err != nil {
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
		parquetManifestKey(tc.Name, 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)),
		parquetQueryCatalogTopicKey(tc.Name),
	} {
		if _, err := s.s3Client.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected parquet metadata %q to be deleted after GC, got %v", key, err)
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
	if _, err := s.getTopicDeletion(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
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
	if _, err := s2.getTopicDeletion(ctx, tc.Name); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected deletion marker to be cleared after resumed GC, got %v", err)
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
