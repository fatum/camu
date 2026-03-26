package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()

	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.Server.InstanceID = "n1"
	cfg.WAL.Directory = filepath.Join(t.TempDir(), "wal")
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Storage.Bucket = "test"

	s, err := NewWithS3Client(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewWithS3Client() error = %v", err)
	}
	s.registry = coordination.NewRegistry(s3Client, cfg.Server.InstanceID, "127.0.0.1:8080", time.Minute)
	return s
}

func TestInitPartitionAsLeader_RF1SkipsReplicaState(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}
	ps.nextOffset = 7

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1"},
		Leader:      "n1",
		LeaderEpoch: 1,
	})

	if ps.replicaState != nil {
		t.Fatal("expected nil replicaState for rf=1 leader")
	}
}

func TestHandleProduceLowLevel_FencesStaleLeaderAfterReassignment(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 1,
	})

	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 2},
	}
	s.assignmentsMu.Unlock()

	if err := s.assignmentStore.Write(context.Background(), "topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {
				Replicas:    []string{"n1", "n2", "n3"},
				Leader:      "n2",
				LeaderEpoch: 2,
			},
		},
		Version: 2,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}

	body := bytes.NewBufferString(`{"key":"k","value":"v"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/topic/partitions/0/messages", body)
	req.SetPathValue("topic", "topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()

	s.handleProduceLowLevel(rec, req)

	if rec.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusMisdirectedRequest, rec.Body.String())
	}
}

func TestApplyAssignmentsForTopic_ReadErrorRevokesOwnership(t *testing.T) {
	s := newTestServer(t)

	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 1},
		1: {Owned: true, LeaderEpoch: 1},
	}
	s.assignmentsMu.Unlock()

	s.readAssignments = func(ctx context.Context, topic string) (coordination.TopicAssignments, error) {
		return coordination.TopicAssignments{}, errors.New("temporary s3 read failure")
	}

	s.applyAssignmentsForTopic(context.Background(), "topic", 2)

	if s.isOwnedPartition("topic", 0) || s.isOwnedPartition("topic", 1) {
		t.Fatal("expected ownership to be revoked on assignment read error")
	}
}

func TestApplyAssignmentsForTopic_NotFoundFallsBackToSingleInstanceOwnership(t *testing.T) {
	s := newTestServer(t)

	s.readAssignments = func(ctx context.Context, topic string) (coordination.TopicAssignments, error) {
		return coordination.TopicAssignments{}, storage.ErrNotFound
	}

	s.applyAssignmentsForTopic(context.Background(), "topic", 2)

	if !s.isOwnedPartition("topic", 0) || !s.isOwnedPartition("topic", 1) {
		t.Fatal("expected single-instance fallback ownership on assignment not found")
	}
}

func TestVerifyOwnershipFromS3_ReadErrorFailsClosed(t *testing.T) {
	s := newTestServer(t)

	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 1},
	}
	s.assignmentsMu.Unlock()

	s.readAssignments = func(ctx context.Context, topic string) (coordination.TopicAssignments, error) {
		return coordination.TopicAssignments{}, errors.New("temporary s3 read failure")
	}

	if s.verifyOwnershipFromS3("topic", 0) {
		t.Fatal("verifyOwnershipFromS3() = true, want false on read error")
	}
	if s.isOwnedPartition("topic", 0) {
		t.Fatal("expected partition ownership to be revoked after read error")
	}
}

func TestInitPartitionAsLeader_SetsLeaderEpoch(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 7,
	})

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}
	if ps.epoch != 7 {
		t.Fatalf("ps.epoch = %d, want 7", ps.epoch)
	}
}

func TestGetRoutingMap_FallsBackToLeaderHostWhenRegistryMissing(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        2,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.assignmentStore.Write(context.Background(), "topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Replicas: []string{"n1", "n2", "n3"}, Leader: "n1", LeaderEpoch: 1},
			1: {Replicas: []string{"n2", "n3", "n4"}, Leader: "n4", LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}

	routing := s.getRoutingMap("topic")
	if got := routing.Partitions["0"].Address; got != "http://n1:8080" {
		t.Fatalf("partition 0 address = %q, want %q", got, "http://n1:8080")
	}
	if got := routing.Partitions["1"].Address; got != "http://n4:8080" {
		t.Fatalf("partition 1 address = %q, want %q", got, "http://n4:8080")
	}
	if got := routing.Partitions["0"].Replicas; !reflect.DeepEqual(got, []routingReplicaInfo{
		{InstanceID: "n1", Address: "http://n1:8080"},
		{InstanceID: "n2", Address: "http://n2:8080"},
		{InstanceID: "n3", Address: "http://n3:8080"},
	}) {
		t.Fatalf("partition 0 replicas = %#v", got)
	}
	if got := routing.Partitions["1"].Replicas; !reflect.DeepEqual(got, []routingReplicaInfo{
		{InstanceID: "n2", Address: "http://n2:8080"},
		{InstanceID: "n3", Address: "http://n3:8080"},
		{InstanceID: "n4", Address: "http://n4:8080"},
	}) {
		t.Fatalf("partition 1 replicas = %#v", got)
	}
}

func TestHandleRouting_DoesNotFillMissingPartitionsWithSelf(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/topic/routing", nil)
	req.SetPathValue("topic", "topic")
	rec := httptest.NewRecorder()

	s.handleRouting(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want %q", got, "no-store")
	}

	var resp routingResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Partitions) != 0 {
		t.Fatalf("partitions = %#v, want empty", resp.Partitions)
	}
}

func TestHandleConsumeLowLevel_ReturnsCommittedWALSuffixForOwnedPartition(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 1,
	})
	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 1},
	}
	s.assignmentsMu.Unlock()

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}
	if err := ps.wal.AppendBatch([]log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
	}); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}
	ps.nextOffset = 3
	ps.replicaState = replication.NewReplicaState("n1", 3, 2, 1000)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/topic/partitions/0/messages?offset=0&limit=10", nil)
	req.SetPathValue("topic", "topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()

	s.handleConsumeLowLevel(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("X-High-Watermark"); got != "3" {
		t.Fatalf("X-High-Watermark = %q, want %q", got, "3")
	}

	var resp consumeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("len(messages) = %d, want 3", len(resp.Messages))
	}
	if resp.NextOffset != 3 {
		t.Fatalf("next_offset = %d, want 3", resp.NextOffset)
	}
	for i, msg := range resp.Messages {
		if msg.Offset != uint64(i) {
			t.Fatalf("message[%d].offset = %d, want %d", i, msg.Offset, i)
		}
		if msg.Key != "k"+strconv.Itoa(i) {
			t.Fatalf("message[%d].key = %q, want %q", i, msg.Key, "k"+strconv.Itoa(i))
		}
		if msg.Value != "v"+strconv.Itoa(i) {
			t.Fatalf("message[%d].value = %q, want %q", i, msg.Value, "v"+strconv.Itoa(i))
		}
	}
}

func TestHandleConsumeLowLevel_MergesOverlappingSegmentAndWALData(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 1,
	})
	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 1},
	}
	s.assignmentsMu.Unlock()

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	segmentMsgs := make([]log.Message, 20)
	for i := range segmentMsgs {
		segmentMsgs[i] = log.Message{
			Offset: uint64(i),
			Key:    []byte("seg-k" + strconv.Itoa(i)),
			Value:  []byte("seg-v" + strconv.Itoa(i)),
		}
	}
	var segBuf bytes.Buffer
	if err := log.WriteSegment(&segBuf, segmentMsgs, log.CompressionNone, 16*1024); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	segKey := "topic/0/0-1.segment"
	if err := s.partitionManager.GetDiskCache().Put(segKey, segBuf.Bytes()); err != nil {
		t.Fatalf("diskCache.Put() error = %v", err)
	}
	ps.index.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  19,
		Epoch:      1,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	walMsgs := make([]log.Message, 18)
	for i := range walMsgs {
		offset := uint64(i + 9)
		walMsgs[i] = log.Message{
			Offset: offset,
			Key:    []byte("wal-k" + strconv.Itoa(int(offset))),
			Value:  []byte("wal-v" + strconv.Itoa(int(offset))),
		}
	}
	if err := ps.wal.AppendBatch(walMsgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}
	ps.nextOffset = 27
	ps.replicaState = replication.NewReplicaState("n1", 27, 2, 1000)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/topic/partitions/0/messages?offset=0&limit=1000", nil)
	req.SetPathValue("topic", "topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()

	s.handleConsumeLowLevel(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp consumeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Messages) != 27 {
		t.Fatalf("len(messages) = %d, want 27", len(resp.Messages))
	}
	if resp.NextOffset != 27 {
		t.Fatalf("next_offset = %d, want 27", resp.NextOffset)
	}
	for i, msg := range resp.Messages {
		if msg.Offset != uint64(i) {
			t.Fatalf("message[%d].offset = %d, want %d", i, msg.Offset, i)
		}
	}
	if resp.Messages[9].Key != "wal-k9" {
		t.Fatalf("message[9].key = %q, want %q", resp.Messages[9].Key, "wal-k9")
	}
	if resp.Messages[26].Key != "wal-k26" {
		t.Fatalf("message[26].key = %q, want %q", resp.Messages[26].Key, "wal-k26")
	}
}

func TestHandleConsumeLowLevel_MergesWALBeforeApplyingLimit(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 1,
	})
	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 1},
	}
	s.assignmentsMu.Unlock()

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	segmentMsgs := make([]log.Message, 10)
	for i := range segmentMsgs {
		segmentMsgs[i] = log.Message{
			Offset: uint64(i),
			Key:    []byte("seg-k" + strconv.Itoa(i)),
			Value:  []byte("seg-v" + strconv.Itoa(i)),
		}
	}
	var segBuf bytes.Buffer
	if err := log.WriteSegment(&segBuf, segmentMsgs, log.CompressionNone, 16*1024); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	segKey := "topic/0/0-1.segment"
	if err := s.partitionManager.GetDiskCache().Put(segKey, segBuf.Bytes()); err != nil {
		t.Fatalf("diskCache.Put() error = %v", err)
	}
	ps.index.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  9,
		Epoch:      1,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	walMsgs := make([]log.Message, 10)
	for i := range walMsgs {
		offset := uint64(i)
		walMsgs[i] = log.Message{
			Offset: offset,
			Key:    []byte("wal-k" + strconv.Itoa(i)),
			Value:  []byte("wal-v" + strconv.Itoa(i)),
		}
	}
	if err := ps.wal.AppendBatch(walMsgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}
	ps.nextOffset = 10
	ps.replicaState = replication.NewReplicaState("n1", 10, 2, 1000)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/topic/partitions/0/messages?offset=0&limit=10", nil)
	req.SetPathValue("topic", "topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()

	s.handleConsumeLowLevel(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp consumeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Messages) != 10 {
		t.Fatalf("len(messages) = %d, want 10", len(resp.Messages))
	}
	for i, msg := range resp.Messages {
		if msg.Offset != uint64(i) {
			t.Fatalf("message[%d].offset = %d, want %d", i, msg.Offset, i)
		}
		if msg.Key != "wal-k"+strconv.Itoa(i) {
			t.Fatalf("message[%d].key = %q, want %q", i, msg.Key, "wal-k"+strconv.Itoa(i))
		}
	}
}

func TestHandleConsumeLowLevel_ReturnsReadableFollowerWALSuffix(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{}
	s.assignmentsMu.Unlock()

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	segmentMsgs := make([]log.Message, 17)
	for i := range segmentMsgs {
		segmentMsgs[i] = log.Message{
			Offset: uint64(i),
			Key:    []byte("seg-k" + strconv.Itoa(i)),
			Value:  []byte("seg-v" + strconv.Itoa(i)),
		}
	}
	var segBuf bytes.Buffer
	if err := log.WriteSegment(&segBuf, segmentMsgs, log.CompressionNone, 16*1024); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	segKey := "topic/0/0-1.segment"
	if err := s.partitionManager.GetDiskCache().Put(segKey, segBuf.Bytes()); err != nil {
		t.Fatalf("diskCache.Put() error = %v", err)
	}
	ps.index.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  16,
		Epoch:      1,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	walMsgs := make([]log.Message, 5)
	for i := range walMsgs {
		offset := uint64(i + 17)
		walMsgs[i] = log.Message{
			Offset: offset,
			Key:    []byte("wal-k" + strconv.Itoa(int(offset))),
			Value:  []byte("wal-v" + strconv.Itoa(int(offset))),
		}
	}
	if err := ps.wal.AppendBatch(walMsgs); err != nil {
		t.Fatalf("wal.AppendBatch() error = %v", err)
	}
	ps.nextOffset = 22
	ps.followerHW = 22

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/topic/partitions/0/messages?offset=0&limit=1000", nil)
	req.SetPathValue("topic", "topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()

	s.handleConsumeLowLevel(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("X-High-Watermark"); got != "22" {
		t.Fatalf("X-High-Watermark = %q, want %q", got, "22")
	}

	var resp consumeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Messages) != 22 {
		t.Fatalf("len(messages) = %d, want 22", len(resp.Messages))
	}
	if resp.NextOffset != 22 {
		t.Fatalf("next_offset = %d, want 22", resp.NextOffset)
	}
	if resp.Messages[17].Key != "wal-k17" {
		t.Fatalf("message[17].key = %q, want %q", resp.Messages[17].Key, "wal-k17")
	}
	if resp.Messages[21].Key != "wal-k21" {
		t.Fatalf("message[21].key = %q, want %q", resp.Messages[21].Key, "wal-k21")
	}
}

func TestRoutableHTTPAddress(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		rawAddr    string
		want       string
	}{
		{
			name:       "empty falls back to instance hostname",
			instanceID: "n2",
			rawAddr:    "",
			want:       "http://n2:8080",
		},
		{
			name:       "ipv6 wildcard rewrites to instance hostname",
			instanceID: "n3",
			rawAddr:    "[::]:8080",
			want:       "http://n3:8080",
		},
		{
			name:       "ipv4 wildcard rewrites to instance hostname",
			instanceID: "n4",
			rawAddr:    "0.0.0.0:9090",
			want:       "http://n4:9090",
		},
		{
			name:       "real host is preserved",
			instanceID: "n5",
			rawAddr:    "n5:8081",
			want:       "http://n5:8081",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := routableHTTPAddress(tt.instanceID, tt.rawAddr); got != tt.want {
				t.Fatalf("routableHTTPAddress(%q, %q) = %q, want %q", tt.instanceID, tt.rawAddr, got, tt.want)
			}
		})
	}
}

func TestGCStaleInstances(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Register a live instance via the server's own registry.
	if err := s.registry.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Write a stale instance registration with an old heartbeat.
	staleInfo := coordination.InstanceInfo{
		InstanceID:  "dead-node",
		Address:     "10.0.0.99:8080",
		HeartbeatAt: time.Now().Add(-24 * time.Hour),
	}
	staleData, _ := json.Marshal(staleInfo)
	if err := s.s3Client.Put(ctx, "_coordination/instances/dead-node.json", staleData, storage.PutOpts{}); err != nil {
		t.Fatalf("Put stale instance: %v", err)
	}

	// Verify both registrations exist.
	keys, _ := s.s3Client.List(ctx, "_coordination/instances/")
	if len(keys) != 2 {
		t.Fatalf("expected 2 instance files, got %d", len(keys))
	}

	// Run GC — should remove the stale one and keep the live one.
	s.gcStaleInstances(ctx)

	keys, _ = s.s3Client.List(ctx, "_coordination/instances/")
	if len(keys) != 1 {
		t.Fatalf("expected 1 instance file after GC, got %d", len(keys))
	}
}

func TestGCStaleISR(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Create a topic with 2 partitions.
	tc := meta.TopicConfig{
		Name:              "mytopic",
		Partitions:        2,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create: %v", err)
	}

	// Write ISR files: valid (mytopic/0, mytopic/1), stale (mytopic/5, deleted-topic/0).
	for _, key := range []string{
		"_coordination/isr/mytopic/0.json",
		"_coordination/isr/mytopic/1.json",
		"_coordination/isr/mytopic/5.json",       // partition beyond count
		"_coordination/isr/deleted-topic/0.json", // topic doesn't exist
	} {
		if err := s.s3Client.Put(ctx, key, []byte(`{}`), storage.PutOpts{}); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	keys, _ := s.s3Client.List(ctx, "_coordination/isr/")
	if len(keys) != 4 {
		t.Fatalf("expected 4 ISR files, got %d", len(keys))
	}

	// Run GC.
	topics, _ := s.topicStore.List(ctx)
	s.gcStaleISR(ctx, topics)

	keys, _ = s.s3Client.List(ctx, "_coordination/isr/")
	if len(keys) != 2 {
		t.Fatalf("expected 2 ISR files after GC, got %d: %v", len(keys), keys)
	}
}
