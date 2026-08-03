package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newTestServer(t testing.TB) *Server {
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
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "sql-cache")
	cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "sql-tmp")
	cfg.Storage.Bucket = "test"

	s, err := NewWithS3Client(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewWithS3Client() error = %v", err)
	}
	s.registry = coordination.NewRegistry(s3Client, cfg.Server.InstanceID, "127.0.0.1:8080", "127.0.0.1:8081", "", time.Minute)
	return s
}

func putConsumeTestSegment(t *testing.T, s *Server, key string, batch []byte, firstOffset, lastOffset uint64) {
	t.Helper()
	if err := s.s3Client.Put(context.Background(), key, batch, storage.PutOpts{}); err != nil {
		t.Fatalf("put segment: %v", err)
	}
	var sidecar bytes.Buffer
	if err := log.WriteSidecar(&sidecar, []log.IndexEntry{{
		BaseOffset: int64(firstOffset),
		LastOffset: int64(lastOffset),
		Position:   0,
		BatchSize:  int32(len(batch)),
	}}, nil); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	if err := s.s3Client.Put(context.Background(), log.SegmentOffsetIndexKey(key), sidecar.Bytes(), storage.PutOpts{}); err != nil {
		t.Fatalf("put sidecar: %v", err)
	}
}

func newQueryTestServer(t *testing.T) *Server {
	t.Helper()

	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.Server.InstanceID = "query-1"
	cfg.Server.Mode = config.ServerModeQuery
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "sql-cache")
	cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "sql-tmp")
	cfg.Storage.Bucket = "test"

	s, err := NewWithS3Client(cfg, s3Client)
	if err != nil {
		t.Fatalf("NewWithS3Client() error = %v", err)
	}
	return s
}

type rejectedStartupListener struct{ closed bool }

func (l *rejectedStartupListener) Accept() (net.Conn, error) {
	return nil, errors.New("should not accept")
}
func (l *rejectedStartupListener) Close() error   { l.closed = true; return nil }
func (l *rejectedStartupListener) Addr() net.Addr { return &net.TCPAddr{} }

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

func TestFollowerFetchMatchesAssignment_UsesConfiguredAssignmentEpoch(t *testing.T) {
	ps := &partitionState{
		leaderID:             "n2",
		fetchCancel:          func() {},
		fetchAssignmentEpoch: 4,
		epoch:                3,
	}

	if !followerFetchMatchesAssignment(ps, "n2", 4) {
		t.Fatal("expected fetcher configured for assignment epoch 4 to be reused")
	}
	if followerFetchMatchesAssignment(ps, "n2", 5) {
		t.Fatal("expected newer assignment epoch to reconfigure the fetcher")
	}
}

func TestHandleKafkaListOffsets_ReplicatedPartitionNotReady(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 2,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	_, err := s.handleKafkaListOffsets(context.Background(), "topic", 0, -2)
	if !errors.Is(err, errKafkaLeaderNotAvailable) {
		t.Fatalf("handleKafkaListOffsets() error = %v, want %v", err, errKafkaLeaderNotAvailable)
	}
}

func TestHandleKafkaListOffsets_ByTimestampFromWAL(t *testing.T) {
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
	if _, err := s.partitionManager.appendBatchToPS(ps, "topic", 0, []log.Message{
		{Timestamp: 1000, Value: []byte("a")},
		{Timestamp: 2000, Value: []byte("b")},
		{Timestamp: 3000, Value: []byte("c")},
	}); err != nil {
		t.Fatalf("appendBatchToPS() error = %v", err)
	}

	resp, err := s.handleKafkaListOffsets(context.Background(), "topic", 0, 1500)
	if err != nil {
		t.Fatalf("handleKafkaListOffsets() error = %v", err)
	}
	if resp.Offset != 1 {
		t.Fatalf("handleKafkaListOffsets() offset = %d, want 1", resp.Offset)
	}
	if resp.Timestamp != 1500 {
		t.Fatalf("handleKafkaListOffsets() timestamp = %d, want 1500", resp.Timestamp)
	}
}

func TestHandleConsumeLowLevel_DisklessHonorsMessageLimit(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{})
	defer s.disklessEngine.Close()

	tc := meta.TopicConfig{
		Name:              "diskless-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "diskless",
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	s.markTopicDiskless("diskless-topic")

	largeValue := strings.Repeat("x", 1500)
	for i := 0; i < 3; i++ {
		_, err := s.disklessEngine.Produce(context.Background(), "diskless-topic", 0, log.EncodeRecordBatch(0, []log.Message{
			{Key: []byte("k" + strconv.Itoa(i+1)), Value: []byte(largeValue + "-" + strconv.Itoa(i+1))},
		}))
		if err != nil {
			t.Fatalf("disklessEngine.Produce() error = %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/diskless-topic/partitions/0/messages?offset=0&limit=2", nil)
	req.SetPathValue("topic", "diskless-topic")
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
	if len(resp.Messages) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(resp.Messages))
	}
	if resp.Messages[0].Offset != 0 {
		t.Fatalf("message[0].offset = %d, want 0", resp.Messages[0].Offset)
	}
	if resp.Messages[1].Offset != 1 {
		t.Fatalf("message[1].offset = %d, want 1", resp.Messages[1].Offset)
	}
	if resp.NextOffset != 2 {
		t.Fatalf("next_offset = %d, want 2", resp.NextOffset)
	}
}

func TestHandleConsumeLowLevel_DisklessBeyondEndReturnsRequestedOffset(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{})
	defer s.disklessEngine.Close()

	tc := meta.TopicConfig{
		Name:              "diskless-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "diskless",
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	s.markTopicDiskless("diskless-topic")

	for i := 0; i < 2; i++ {
		_, err := s.disklessEngine.Produce(context.Background(), "diskless-topic", 0, log.EncodeRecordBatch(0, []log.Message{
			{Key: []byte("k" + strconv.Itoa(i+1)), Value: []byte("v" + strconv.Itoa(i+1))},
		}))
		if err != nil {
			t.Fatalf("disklessEngine.Produce() error = %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/diskless-topic/partitions/0/messages?offset=10&limit=2", nil)
	req.SetPathValue("topic", "diskless-topic")
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
	if len(resp.Messages) != 0 {
		t.Fatalf("len(messages) = %d, want 0", len(resp.Messages))
	}
	if resp.NextOffset != 10 {
		t.Fatalf("next_offset = %d, want 10", resp.NextOffset)
	}
}

func TestPublicAPIHandler_QueryModeDisablesStreamingEndpoints(t *testing.T) {
	s := newQueryTestServer(t)
	s.ready.Store(true)

	handler := s.publicAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/ready status = %d, want %d", rec.Code, http.StatusOK)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/cluster/status status = %d, want %d", rec.Code, http.StatusOK)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/cluster/ready", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /v1/cluster/ready status = %d, want %d in query mode", rec.Code, http.StatusServiceUnavailable)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /v1/sql status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/topics", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("POST /v1/topics status = %d, want %d in query mode", rec.Code, http.StatusNotFound)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /v1/topics status = %d, want %d in query mode", rec.Code, http.StatusNotFound)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/topics/topic/partitions/0/messages?offset=0", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET consume status = %d, want %d in query mode", rec.Code, http.StatusNotFound)
	}
}

func TestInternalRoutes_QueryModeOnlyReady(t *testing.T) {
	s := newTestServer(t)
	s.cfg.Server.Mode = config.ServerModeQuery
	s.ready.Store(true)

	handler := s.internalRoutes()

	req := httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/ready status = %d, want %d", rec.Code, http.StatusOK)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/internal/replicate/topic/0", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET internal replicate status = %d, want %d in query mode", rec.Code, http.StatusNotFound)
	}
}

func TestHandleReplicaFetch_DivergenceReturnsEpochAtTruncate(t *testing.T) {
	s := newTestServer(t)
	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 2,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}
	ps := s.partitionManager.GetPartitionState(tc.Name, 0)
	ps.mu.Lock()
	ps.epoch = 3
	ps.epochHistory = &replication.EpochHistory{Entries: []replication.EpochEntry{
		{Epoch: 1, StartOffset: 0},
		{Epoch: 2, StartOffset: 10},
		{Epoch: 3, StartOffset: 20},
	}}
	ps.replicaState = replication.NewReplicaState("n1", 20, 1, 1000)
	ps.replicaState.SetEpochHistory(ps.epochHistory)
	ps.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/internal/replicate/topic/0?from_offset=15", nil)
	req.SetPathValue("topic", tc.Name)
	req.SetPathValue("pid", "0")
	req.Header.Set("X-Replica-ID", "n2")
	req.Header.Set("X-Replica-Offset", "15")
	req.Header.Set("X-Replica-Epoch", "1")
	rec := httptest.NewRecorder()
	s.handleReplicaFetch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("X-Truncate-To"); got != "10" {
		t.Errorf("X-Truncate-To = %q, want 10", got)
	}
	if got := rec.Header().Get("X-Leader-Epoch"); got != "2" {
		t.Errorf("X-Leader-Epoch = %q, want 2", got)
	}
}

func TestHandleReplicaFetch_DuplicateEpochBoundaryServesRealTail(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 2,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}
	if err := s.partitionManager.ensureActiveSegment(tc.Name, 0); err != nil {
		t.Fatalf("ensureActiveSegment: %v", err)
	}

	// This is the epoch history recovered from S3 after the old restart bug:
	// two boundaries claim to be epoch 1. The second is not a real leadership
	// transition, so an epoch-1 follower must be allowed to fetch the tail.
	ps := s.partitionManager.GetPartitionState(tc.Name, 0)
	ps.mu.Lock()
	ps.epoch = 1
	ps.epochHistory = &replication.EpochHistory{Entries: []replication.EpochEntry{
		{Epoch: 1, StartOffset: 0},
		{Epoch: 1, StartOffset: 10},
	}}
	ps.replicaState = replication.NewReplicaState("n1", 10, 1, 1000)
	ps.replicaState.SetEpochHistory(ps.epochHistory)
	ps.mu.Unlock()

	raw := log.EncodeRecordBatch(10, []log.Message{{Offset: 10, Value: []byte("recovered-tail")}})
	if err := s.partitionManager.AppendReplicatedRawBatch(ctx, tc.Name, 0, raw); err != nil {
		t.Fatalf("AppendReplicatedRawBatch: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/internal/replicate/topic/0?from_offset=10", nil)
	req.SetPathValue("topic", tc.Name)
	req.SetPathValue("pid", "0")
	req.Header.Set("X-Replica-ID", "n2")
	req.Header.Set("X-Replica-Offset", "11")
	req.Header.Set("X-Replica-Epoch", "1")
	rec := httptest.NewRecorder()
	s.handleReplicaFetch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Truncate-To"); got != "" {
		t.Fatalf("X-Truncate-To = %q, want no truncation", got)
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, raw) {
		t.Fatalf("replica response = %x, want raw RecordBatch %x", got, raw)
	}
}

func TestInternalReadinessReportsInitializedPartitions(t *testing.T) {
	s := newTestServer(t)
	s.ready.Store(true)
	tc := meta.TopicConfig{Name: "ready-topic", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{0: 7}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/readiness", nil)
	rec := httptest.NewRecorder()
	s.handleInternalReadiness(rec, req)
	var got localReadinessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode readiness: %v", err)
	}
	if !got.Ready || len(got.Partitions) != 1 || got.Partitions[0].Topic != tc.Name || got.Partitions[0].Epoch != 7 {
		t.Fatalf("readiness = %+v, want ready partition epoch 7", got)
	}
}

func TestStartWithListener_QueryModeSkipsClusterStartup(t *testing.T) {
	s := newQueryTestServer(t)
	// An invalid internal address proves query mode neither binds nor serves h2c.
	s.cfg.Server.InternalAddress = "not-a-listening-address"
	s.cfg.Server.KafkaPort = 9092
	s.registry = nil

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer ln.Close()

	if err := s.startWithListener(ln); err != nil {
		t.Fatalf("startWithListener() error = %v", err)
	}
	defer func() {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}()

	if !s.ready.Load() {
		t.Fatal("ready = false, want true")
	}
	if s.registry != nil {
		t.Fatalf("registry = %#v, want nil in query mode", s.registry)
	}
	if s.kafkaServer != nil {
		t.Fatalf("kafkaServer = %#v, want nil in query mode", s.kafkaServer)
	}
	if s.disklessEngine != nil {
		t.Fatalf("disklessEngine = %#v, want nil in query mode", s.disklessEngine)
	}
	if s.internalListener != nil {
		t.Fatalf("internalListener = %#v, want nil in query mode", s.internalListener)
	}
	if s.partitionManager != nil {
		t.Fatalf("partitionManager = %#v, want nil in query mode", s.partitionManager)
	}
	if s.leaderElection != nil || s.assignmentStore != nil || s.isrStore != nil {
		t.Fatal("query mode constructed coordination services")
	}
	if s.fetcher != nil || s.offsetStore != nil || s.aclStore != nil || s.groupCoord != nil || s.idempotencyManager != nil {
		t.Fatal("query mode constructed stream-only services")
	}
}

func TestHandleSQLQueryInvalidBody(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader("{"))
	rec := httptest.NewRecorder()

	s.handleSQLQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSQLAuthTokenEnforced(t *testing.T) {
	s := newQueryTestServer(t)
	s.cfg.Server.AuthToken = "secret"
	h := s.PublicHandler()
	body := strings.NewReader(`{"sql":"select 1"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1"}`))
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid auth status = %d", rec.Code)
	}
}

func TestSQLAuthTokenAllowsValidCredential(t *testing.T) {
	s := newQueryTestServer(t)
	s.cfg.Server.AuthToken = "secret"
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1"}`))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	s.PublicHandler().ServeHTTP(rec, req)
	if rec.Code == http.StatusUnauthorized {
		t.Fatal("valid auth was rejected")
	}
}

func TestHeapProfileRequiresAuth(t *testing.T) {
	s := newTestServer(t)
	s.cfg.Server.AuthToken = "secret"
	s.cfg.Server.HeapProfileEnabled = true
	req := httptest.NewRequest(http.MethodGet, "/v1/debug/heap", nil)
	rec := httptest.NewRecorder()
	s.PublicHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d", rec.Code)
	}
}

func TestHandleSQLQueryAdmitsScopeResolutionThroughSQLLimiter(t *testing.T) {
	s := newTestServer(t)
	s.sqlLimiter = make(chan struct{}, 1)
	s.sqlLimiter <- struct{}{}
	if err := s.topicStore.Create(context.Background(), meta.TopicConfig{
		Name: "events", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(),
		ReplicationFactor: 1, MinInsyncReplicas: 1,
	}); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		s.handleSQLQuery(rec, httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1","topics":["events"]}`)))
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("SQL request resolved scope without acquiring the limiter")
	case <-time.After(20 * time.Millisecond):
	}
	<-s.sqlLimiter
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SQL request did not continue after limiter release")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 after scope resolution finds no parquet; body=%s", rec.Code, rec.Body.String())
	}
}

func TestSQLRequestContextCancelledByShutdown(t *testing.T) {
	s := newTestServer(t)
	ctx, cancel, err := s.sqlRequestContext(context.Background())
	if err != nil {
		t.Fatalf("sqlRequestContext() error = %v", err)
	}
	defer cancel()

	s.sqlCtxCancel()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("context error = %v, want context.Canceled", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("SQL request context was not cancelled by shutdown context")
	}
}

func TestResolveSQLQueryScopeUsesManifests(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tc := meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if _, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic:         "events",
		Partition:     0,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
		Entries: []ParquetManifestEntry{
			{ObjectKey: parquetExportObjectKey("events", 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC), 0, 9, 1, "events/0/0-9-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1},
		},
	}); err != nil {
		t.Fatalf("publishParquetManifest() error = %v", err)
	}

	scope, err := s.resolveSQLQueryScope(ctx, sqlQueryRequest{
		SQL:    "select 1",
		Topics: []string{"events"},
		TimeRange: &sqlTimeRange{
			From: "2026-04-11T00:00:00Z",
			To:   "2026-04-11T23:59:59Z",
		},
	})
	if err != nil {
		t.Fatalf("resolveSQLQueryScope() error = %v", err)
	}
	if len(scope.Topics) != 1 || scope.Topics[0] != "events" {
		t.Fatalf("scope.Topics = %v, want [events]", scope.Topics)
	}
	if len(scope.Manifests["events"]) != 1 {
		t.Fatalf("len(scope.Manifests[events]) = %d, want 1", len(scope.Manifests["events"]))
	}
}

func TestHandleSQLQueryUnknownTopic(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1","topics":["missing"]}`))
	rec := httptest.NewRecorder()

	s.handleSQLQuery(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestHandleSQLQueryReturnsNotImplementedAfterScopeResolution(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tc := meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1","topics":["events"]}`))
	rec := httptest.NewRecorder()

	s.handleSQLQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestPublicAPIHandler_SQLRouteDisabledByDefaultInStreamMode(t *testing.T) {
	s := newTestServer(t)
	// Stream mode is the default, and with SQL.Enabled unset the endpoint
	// should not be registered on the public mux — analytical SQL load
	// must not land on hot streaming nodes by default.
	handler := s.publicAPIHandler()
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1","topics":["events"]}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 404/405 (route not registered); body=%s", rec.Code, rec.Body.String())
	}
}

func TestPublicAPIHandler_SQLRouteEnabledExplicitly(t *testing.T) {
	s := newTestServer(t)
	enabled := true
	s.cfg.SQL.Enabled = &enabled
	if err := s.topicStore.Create(context.Background(), meta.TopicConfig{
		Name: "events", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(),
		ReplicationFactor: 1, MinInsyncReplicas: 1,
	}); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	handler := s.publicAPIHandler()
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1","topics":["events"]}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// The handler runs through the pipeline (topic exists, no parquet ⇒
	// 400). What matters is that the route is reachable, not 404.
	if rec.Code == http.StatusNotFound || rec.Code == http.StatusMethodNotAllowed {
		t.Fatalf("SQL route not registered when explicitly enabled: status=%d", rec.Code)
	}
}

func TestHandleSQLQueryRejectsUnsafeTopicName(t *testing.T) {
	s := newTestServer(t)
	for _, bad := range []string{"../etc/passwd", `events" or "1`, "events space", ""} {
		body := fmt.Sprintf(`{"sql":"select 1","topics":[%q]}`, bad)
		req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(body))
		rec := httptest.NewRecorder()
		s.handleSQLQuery(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("topic=%q status=%d, want 400; body=%s", bad, rec.Code, rec.Body.String())
		}
	}
}

func TestPrepareSQLConnectionEnforcesScanBudget(t *testing.T) {
	s := newTestServer(t)
	s.cfg.SQL.MaxScanFiles = 2
	ctx := context.Background()
	tc := meta.TopicConfig{
		Name: "events", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(),
		ReplicationFactor: 1, MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create: %v", err)
	}

	// Plant stub parquet files so ensureLocalParquetObject succeeds.
	planted := []string{}
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		key := parquetExportObjectKey("events", 0, ts, int64(i*10), int64(i*10+9), 1, fmt.Sprintf("events/0/%d-%d-1.segment|epoch=1", i*10, i*10+9))
		if err := s.s3Client.Put(ctx, key, []byte("stub"), storage.PutOpts{}); err != nil {
			t.Fatalf("plant: %v", err)
		}
		planted = append(planted, key)
	}
	entries := make([]ParquetManifestEntry, len(planted))
	for i, k := range planted {
		entries[i] = ParquetManifestEntry{ObjectKey: k, BaseOffset: int64(i * 10), EndOffset: int64(i*10 + 9), SchemaVersion: 1, SourceKey: fmt.Sprintf("events/0/%d-%d-1.segment", i*10, i*10+9), SourceEpoch: 1}
	}
	if _, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1, Entries: entries,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	scope, err := s.resolveSQLQueryScope(ctx, sqlQueryRequest{
		SQL: "select 1", Topics: []string{"events"},
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	db, err := s.sqlDBHandle()
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	_, err = s.prepareSQLConnection(ctx, conn, scope)
	if !errors.Is(err, errSQLScanBudgetExceeded) {
		t.Fatalf("prepareSQLConnection err = %v, want errSQLScanBudgetExceeded", err)
	}
	cacheEntries, err := os.ReadDir(s.cfg.SQL.CacheDirectory)
	if err != nil {
		t.Fatalf("ReadDir(cache): %v", err)
	}
	if len(cacheEntries) != 0 {
		t.Fatalf("cache directory should stay empty when scan budget is exceeded: %v", cacheEntries)
	}
	tempEntries, err := os.ReadDir(s.cfg.SQL.TempDirectory)
	if err != nil {
		t.Fatalf("ReadDir(temp): %v", err)
	}
	for _, entry := range tempEntries {
		if strings.HasPrefix(entry.Name(), "camu-query-") && strings.HasSuffix(entry.Name(), ".parquet") {
			t.Fatalf("scan budget exceeded should not leave query temp files behind: %s", entry.Name())
		}
	}
}

func TestEnsureLocalParquetObjectCoalescesConcurrentFetches(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Plant an object whose key is deterministic.
	key := parquetExportObjectKey("events", 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC), 0, 9, 1, "events/0/0-9-1.segment|epoch=1")
	if err := s.s3Client.Put(ctx, key, []byte("content-bytes"), storage.PutOpts{}); err != nil {
		t.Fatalf("plant: %v", err)
	}

	// Scrub any in-process cache that a previous test might have left.
	cachePath := s.parquetCachePath(key)
	_ = os.Remove(cachePath)

	const workers = 16
	type result struct {
		file localParquetFile
		err  error
	}
	results := make(chan result, workers)
	for range workers {
		go func() {
			p, err := s.ensureLocalParquetObject(ctx, key)
			results <- result{file: p, err: err}
		}()
	}
	paths := map[string]int{}
	files := make([]localParquetFile, 0, workers)
	for range workers {
		r := <-results
		if r.err != nil {
			t.Fatalf("ensureLocalParquetObject: %v", r.err)
		}
		if r.file.Temporary {
			t.Fatal("concurrent cache acquisition unexpectedly used a temporary file")
		}
		paths[r.file.Path]++
		files = append(files, r.file)
	}
	if len(paths) != 1 {
		t.Fatalf("concurrent fetches returned %d distinct paths, want 1: %v", len(paths), paths)
	}
	// Exactly one canonical file — no leftover `.tmp` siblings from the
	// race condition the fix targets.
	dir := filepath.Dir(cachePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var parquetFiles, tmpFiles int
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".parquet.tmp"):
			tmpFiles++
		case strings.HasSuffix(name, ".parquet"):
			parquetFiles++
		}
	}
	if parquetFiles != 1 {
		t.Fatalf("parquet cache files = %d, want 1", parquetFiles)
	}
	if tmpFiles != 0 {
		t.Fatalf("leftover .tmp files = %d, want 0", tmpFiles)
	}
	s.parquetCacheMu.Lock()
	pins := s.parquetCachePins[cachePath]
	s.parquetCacheMu.Unlock()
	if pins != workers {
		t.Fatalf("cache pins = %d, want %d", pins, workers)
	}
	for _, file := range files {
		file.Release()
	}
	s.parquetCacheMu.Lock()
	pins = s.parquetCachePins[cachePath]
	s.parquetCacheMu.Unlock()
	if pins != 0 {
		t.Fatalf("cache pins after release = %d, want 0", pins)
	}
}

func TestEnsureLocalParquetObjectKeepsPinnedFileDuringCachePressure(t *testing.T) {
	s := newTestServer(t)
	s.cfg.SQL.CacheMaxSize = 6
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	firstKey := parquetExportObjectKey("events", 0, ts, 0, 0, 1, "events/0/0-0-1.segment|epoch=1")
	secondKey := parquetExportObjectKey("events", 0, ts, 1, 1, 1, "events/0/1-1-1.segment|epoch=1")
	if err := s.s3Client.Put(ctx, firstKey, []byte("1234"), storage.PutOpts{}); err != nil {
		t.Fatalf("plant first object: %v", err)
	}
	if err := s.s3Client.Put(ctx, secondKey, []byte("5678"), storage.PutOpts{}); err != nil {
		t.Fatalf("plant second object: %v", err)
	}

	first, err := s.ensureLocalParquetObject(ctx, firstKey)
	if err != nil || first.Temporary {
		t.Fatalf("first cache result = %+v, err=%v; want cached file", first, err)
	}
	defer first.Release()
	second, err := s.ensureLocalParquetObject(ctx, secondKey)
	if err != nil {
		t.Fatalf("second cache result: %v", err)
	}
	if !second.Temporary {
		t.Fatal("second cache result should fall back to a temporary file while first is pinned")
	}
	defer cleanupLocalParquetFiles([]localParquetFile{second})

	data, err := os.ReadFile(first.Path)
	if err != nil {
		t.Fatalf("read pinned cache file: %v", err)
	}
	if string(data) != "1234" {
		t.Fatalf("pinned cache file = %q, want %q", data, "1234")
	}
	if _, err := os.Stat(second.Path); err != nil {
		t.Fatalf("temporary fallback missing: %v", err)
	}
}

func TestEnsureLocalParquetObjectEvictsToAggregateCacheLimit(t *testing.T) {
	s := newTestServer(t)
	s.cfg.SQL.CacheMaxSize = 6
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	firstKey := parquetExportObjectKey("events", 0, ts, 0, 0, 1, "events/0/0-0-1.segment|epoch=1")
	secondKey := parquetExportObjectKey("events", 0, ts, 1, 1, 1, "events/0/1-1-1.segment|epoch=1")
	if err := s.s3Client.Put(ctx, firstKey, []byte("1234"), storage.PutOpts{}); err != nil {
		t.Fatalf("plant first object: %v", err)
	}
	if err := s.s3Client.Put(ctx, secondKey, []byte("5678"), storage.PutOpts{}); err != nil {
		t.Fatalf("plant second object: %v", err)
	}

	first, err := s.ensureLocalParquetObject(ctx, firstKey)
	if err != nil || first.Temporary {
		t.Fatalf("first cache result = %+v, err=%v; want cached file", first, err)
	}
	first.Release()
	second, err := s.ensureLocalParquetObject(ctx, secondKey)
	if err != nil || second.Temporary {
		t.Fatalf("second cache result = %+v, err=%v; want cached file", second, err)
	}

	if _, err := os.Stat(first.Path); !os.IsNotExist(err) {
		t.Fatalf("first cache entry still exists after eviction: err=%v", err)
	}
	if _, err := os.Stat(second.Path); err != nil {
		t.Fatalf("second cache entry missing after install: %v", err)
	}
	entries, err := os.ReadDir(s.cfg.SQL.CacheDirectory)
	if err != nil {
		t.Fatalf("ReadDir(cache): %v", err)
	}
	var total int64
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".parquet") {
			info, err := entry.Info()
			if err != nil {
				t.Fatalf("stat cache entry: %v", err)
			}
			total += info.Size()
		}
	}
	if total > s.cfg.SQL.CacheMaxSize {
		t.Fatalf("cache size = %d, limit = %d", total, s.cfg.SQL.CacheMaxSize)
	}
}

func TestEnsureLocalParquetObjectReconcilesPreexistingCacheOnHit(t *testing.T) {
	s := newTestServer(t)
	s.cfg.SQL.CacheMaxSize = 6
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	keptKey := parquetExportObjectKey("events", 0, ts, 0, 0, 1, "events/0/0-0-1.segment|epoch=1")
	staleKey := parquetExportObjectKey("events", 0, ts, 1, 1, 1, "events/0/1-1-1.segment|epoch=1")
	keptPath := s.parquetCachePath(keptKey)
	stalePath := s.parquetCachePath(staleKey)
	if err := os.MkdirAll(filepath.Dir(keptPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(cache): %v", err)
	}
	if err := os.WriteFile(keptPath, []byte("1234"), 0o644); err != nil {
		t.Fatalf("write kept cache entry: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("5678"), 0o644); err != nil {
		t.Fatalf("write stale cache entry: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(stalePath, old, old); err != nil {
		t.Fatalf("age stale cache entry: %v", err)
	}

	file, err := s.ensureLocalParquetObject(ctx, keptKey)
	if err != nil || file.Temporary {
		t.Fatalf("cache hit = %+v, err=%v; want cached file", file, err)
	}
	defer file.Release()
	if _, err := os.Stat(keptPath); err != nil {
		t.Fatalf("reused cache entry missing: %v", err)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale oversized cache entry remains: %v", err)
	}
	info, err := os.Stat(keptPath)
	if err != nil {
		t.Fatalf("stat kept cache entry: %v", err)
	}
	if info.Size() > s.cfg.SQL.CacheMaxSize {
		t.Fatalf("cache size = %d, limit = %d", info.Size(), s.cfg.SQL.CacheMaxSize)
	}
}

func TestLocalParquetFilesForObjectKeysCleansPartialAcquisition(t *testing.T) {
	s := newTestServer(t)
	s.cfg.SQL.CacheMaxSize = 6
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	cachedKey := parquetExportObjectKey("events", 0, ts, 0, 0, 1, "events/0/0-0-1.segment|epoch=1")
	temporaryKey := parquetExportObjectKey("events", 0, ts, 1, 1, 1, "events/0/1-1-1.segment|epoch=1")
	missingKey := parquetExportObjectKey("events", 0, ts, 2, 2, 1, "events/0/2-2-1.segment|epoch=1")
	if err := s.s3Client.Put(ctx, cachedKey, []byte("1234"), storage.PutOpts{}); err != nil {
		t.Fatalf("plant cached object: %v", err)
	}
	if err := s.s3Client.Put(ctx, temporaryKey, []byte("5678"), storage.PutOpts{}); err != nil {
		t.Fatalf("plant temporary object: %v", err)
	}

	_, err := s.localParquetFilesForObjectKeys(ctx, []string{cachedKey, temporaryKey, missingKey})
	if err == nil {
		t.Fatal("partial acquisition unexpectedly succeeded")
	}
	entries, err := os.ReadDir(s.cfg.SQL.TempDirectory)
	if err != nil {
		t.Fatalf("ReadDir(temp): %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "camu-query-") && strings.HasSuffix(entry.Name(), ".parquet") {
			t.Fatalf("partial acquisition left temporary parquet file behind: %s", entry.Name())
		}
	}
	cachePath := s.parquetCachePath(cachedKey)
	s.parquetCacheMu.Lock()
	pins := s.parquetCachePins[cachePath]
	s.parquetCacheMu.Unlock()
	if pins != 0 {
		t.Fatalf("cached file pins after partial acquisition = %d, want 0", pins)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("released cache file should remain available for eviction: %v", err)
	}
}

func TestHandleSQLQueryRemovesOversizedTempFiles(t *testing.T) {
	s := newTestServer(t)
	s.cfg.SQL.CacheMaxSize = 1
	ctx := context.Background()
	tc := meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	objectKey := parquetExportObjectKey("events", 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC), 0, 1, 1, "events/0/0-1-1.segment|epoch=1")
	writeTestParquetObject(t, s, objectKey, []string{
		`CREATE TABLE events AS SELECT 1::BIGINT AS id, 'alpha'::VARCHAR AS name UNION ALL SELECT 2::BIGINT, 'beta'::VARCHAR`,
	})
	if _, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic:         "events",
		Partition:     0,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
		Entries: []ParquetManifestEntry{
			{ObjectKey: objectKey, BaseOffset: 0, EndOffset: 1, SchemaVersion: 1, SourceKey: "events/0/0-1.segment", SourceEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("publishParquetManifest() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select id, name from events order by id","topics":["events"]}`))
	rec := httptest.NewRecorder()
	s.handleSQLQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	entries, err := os.ReadDir(s.cfg.SQL.TempDirectory)
	if err != nil {
		t.Fatalf("ReadDir(temp): %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "camu-query-") && strings.HasSuffix(entry.Name(), ".parquet") {
			t.Fatalf("oversized parquet temp file leaked after query: %s", entry.Name())
		}
	}
}

func TestShutdownCancelsSQLContext(t *testing.T) {
	s := newTestServer(t)
	if s.sqlCtx == nil {
		t.Fatal("sqlCtx not initialized")
	}
	if err := s.sqlCtx.Err(); err != nil {
		t.Fatalf("sqlCtx already cancelled before Shutdown: %v", err)
	}
	// Don't actually bind and serve — just exercise the Shutdown codepath
	// that cancels in-flight SQL work and closes the DuckDB handle.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if s.sqlCtx.Err() == nil {
		t.Fatal("sqlCtx still live after Shutdown, want cancelled")
	}
}

func TestPublicAPIHandler_QueryModeExposesSQLRoute(t *testing.T) {
	s := newTestServer(t)
	s.cfg.Server.Mode = config.ServerModeQuery
	s.ready.Store(true)
	if err := s.topicStore.Create(context.Background(), meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	handler := s.publicAPIHandler()
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select 1","topics":["events"]}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /v1/sql status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleSQLQueryExecutesManifestScopedQuery(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tc := meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	objectKey := parquetExportObjectKey("events", 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC), 0, 1, 1, "events/0/0-1-1.segment|epoch=1")
	writeTestParquetObject(t, s, objectKey, []string{
		`CREATE TABLE events AS SELECT 1::BIGINT AS id, 'alpha'::VARCHAR AS name UNION ALL SELECT 2::BIGINT, 'beta'::VARCHAR`,
	})
	if _, err := s.publishParquetManifest(ctx, ParquetManifest{
		Topic:         "events",
		Partition:     0,
		Date:          "2026-04-11",
		Hour:          "13",
		SchemaVersion: 1,
		Entries: []ParquetManifestEntry{
			{ObjectKey: objectKey, BaseOffset: 0, EndOffset: 1, SchemaVersion: 1, SourceKey: "events/0/0-1.segment", SourceEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("publishParquetManifest() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"select id, name from events order by id","topics":["events"]}`))
	rec := httptest.NewRecorder()

	s.handleSQLQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp sqlQueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Columns) != 2 {
		t.Fatalf("len(columns) = %d, want 2", len(resp.Columns))
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(resp.Rows))
	}
	if got := resp.Rows[0][0]; got != float64(1) {
		t.Fatalf("row[0][0] = %#v, want 1", got)
	}
	if got := resp.Rows[1][1]; got != "beta" {
		t.Fatalf("row[1][1] = %#v, want beta", got)
	}
}

func TestHandleSQLQueryRejectsMutatingStatement(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.topicStore.Create(ctx, meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/sql", strings.NewReader(`{"sql":"delete from events","topics":["events"]}`))
	rec := httptest.NewRecorder()

	s.handleSQLQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func writeTestParquetObject(t *testing.T, s *Server, objectKey string, setup []string) {
	t.Helper()

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test.parquet")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open(duckdb) error = %v", err)
	}
	defer db.Close()

	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("duckdb exec %q error = %v", stmt, err)
		}
	}
	if _, err := db.Exec(`COPY events TO '` + strings.ReplaceAll(parquetPath, `'`, `''`) + `' (FORMAT PARQUET)`); err != nil {
		t.Fatalf("duckdb COPY TO parquet error = %v", err)
	}

	data, err := os.ReadFile(parquetPath)
	if err != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", parquetPath, err)
	}
	if err := s.s3Client.Put(context.Background(), objectKey, data, storage.PutOpts{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("s3Client.Put(%s) error = %v", objectKey, err)
	}
}

func TestHandleKafkaMetadataIncludesUnknownRequestedTopic(t *testing.T) {
	s := newTestServer(t)

	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: strPtr("missing-topic")}}

	resp, err := s.handleKafkaMetadata(context.Background(), req)
	if err != nil {
		t.Fatalf("handleKafkaMetadata() error = %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("handleKafkaMetadata() topics = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != "missing-topic" {
		t.Fatalf("handleKafkaMetadata() topic = %v, want missing-topic", resp.Topics[0].Topic)
	}
	if resp.Topics[0].ErrorCode != kafkaErrorUnknownTopicPartition {
		t.Fatalf("handleKafkaMetadata() error = %d, want %d", resp.Topics[0].ErrorCode, kafkaErrorUnknownTopicPartition)
	}
}

func TestKafkaCreateTopicRequestRejectsRetentionBytes(t *testing.T) {
	retentionBytes := "1024"
	_, code, msg := newTestServer(t).kafkaCreateTopicRequest(kmsg.CreateTopicsRequestTopic{
		Topic:             "topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "retention.bytes",
			Value: &retentionBytes,
		}},
	})
	if code != kafkaErrorInvalidConfig {
		t.Fatalf("kafkaCreateTopicRequest() code = %d, want %d", code, kafkaErrorInvalidConfig)
	}
	if !strings.Contains(msg, "time-based retention only") {
		t.Fatalf("kafkaCreateTopicRequest() message = %q, want time-based retention guidance", msg)
	}
}

func TestApplyKafkaTopicConfigsRejectsRetentionBytes(t *testing.T) {
	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}

	retentionBytes := "1024"
	_, err := applyKafkaTopicConfigs(tc, map[string]*string{
		"retention.bytes": &retentionBytes,
	}, false)
	if err == nil {
		t.Fatal("applyKafkaTopicConfigs() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "time-based retention only") {
		t.Fatalf("applyKafkaTopicConfigs() error = %q, want time-based retention guidance", err)
	}
}

func TestKafkaCreateTopicRequestAcceptsDisklessStorageMode(t *testing.T) {
	storageMode := "diskless"
	reqBody, code, msg := newTestServer(t).kafkaCreateTopicRequest(kmsg.CreateTopicsRequestTopic{
		Topic:             "topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "camu.storage.mode",
			Value: &storageMode,
		}},
	})
	if code != 0 {
		t.Fatalf("kafkaCreateTopicRequest() code = %d, want 0; msg=%q", code, msg)
	}
	if reqBody.StorageMode != "diskless" {
		t.Fatalf("kafkaCreateTopicRequest() storage mode = %q, want %q", reqBody.StorageMode, "diskless")
	}
}

func TestParquetExportRejectsUncleanLeaderElectionOnHTTPTopicCreate(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", strings.NewReader(`{
"name":"events","partitions":1,"unclean_leader_election":true,"export_enabled":true
}`))
	rec := httptest.NewRecorder()
	s.PublicHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /v1/topics status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unclean_leader_election") {
		t.Fatalf("POST /v1/topics body = %q, want parquet compatibility error", rec.Body.String())
	}
}

func TestParquetExportDisabledAllowsUncleanLeaderElectionOnHTTPTopicCreate(t *testing.T) {
	s := newTestServer(t)
	if err := s.registry.Register(context.Background()); err != nil {
		t.Fatalf("register test instance: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/topics", strings.NewReader(`{
"name":"events","partitions":1,"unclean_leader_election":true,"storage_mode":"diskless"
}`))
	rec := httptest.NewRecorder()
	s.PublicHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/topics status = %d, want %d; body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func TestParquetExportRejectsStartupForPersistedUncleanClassicTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.topicStore.Create(ctx, meta.TopicConfig{
		Name:                  "unclean-events",
		Partitions:            1,
		Retention:             time.Hour,
		CreatedAt:             time.Now(),
		ReplicationFactor:     1,
		MinInsyncReplicas:     1,
		UncleanLeaderElection: true,
		StorageMode:           meta.StorageModeClassic,
		ExportEnabled:         true,
	}); err != nil {
		t.Fatalf("seed topic: %v", err)
	}

	listener := &rejectedStartupListener{}
	err := s.startWithListener(listener)
	if err == nil || !strings.Contains(err.Error(), "unclean-events") {
		t.Fatalf("startWithListener() error = %v, want incompatible topic", err)
	}
	if !listener.closed {
		t.Fatal("startup rejection did not close supplied listener")
	}
	instances, err := s.registry.ActiveInstances(ctx)
	if err != nil {
		t.Fatalf("list registry: %v", err)
	}
	if len(instances) != 0 {
		t.Fatalf("startup rejection registered stream instance: %v", instances)
	}
	if s.ready.Load() || s.partitionManager != nil && len(s.partitionManager.partitions) != 0 {
		t.Fatal("startup rejection initialized serving or partitions")
	}
}

func TestParquetExportGuardsKafkaUncleanElectionMutation(t *testing.T) {
	for _, test := range []struct {
		name    string
		enabled bool
		invoke  func(*Server, context.Context, string, string) (int16, error)
	}{
		{
			name: "alter_configs",
			invoke: func(s *Server, ctx context.Context, value, export string) (int16, error) {
				req := kmsg.NewPtrAlterConfigsRequest()
				req.Resources = []kmsg.AlterConfigsRequestResource{{
					ResourceType: kmsg.ConfigResourceTypeTopic,
					ResourceName: "events",
					Configs:      []kmsg.AlterConfigsRequestResourceConfig{{Name: "unclean.leader.election.enable", Value: &value}, {Name: "camu.export.enabled", Value: &export}},
				}}
				resp, err := s.handleKafkaAlterConfigs(ctx, req)
				if err != nil {
					return 0, err
				}
				return resp.Resources[0].ErrorCode, nil
			},
		},
		{
			name: "incremental_alter_configs",
			invoke: func(s *Server, ctx context.Context, value, export string) (int16, error) {
				req := kmsg.NewPtrIncrementalAlterConfigsRequest()
				req.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
					ResourceType: kmsg.ConfigResourceTypeTopic,
					ResourceName: "events",
					Configs:      []kmsg.IncrementalAlterConfigsRequestResourceConfig{{Name: "unclean.leader.election.enable", Op: kmsg.IncrementalAlterConfigOpSet, Value: &value}, {Name: "camu.export.enabled", Op: kmsg.IncrementalAlterConfigOpSet, Value: &export}},
				}}
				resp, err := s.handleKafkaIncrementalAlterConfigs(ctx, req)
				if err != nil {
					return 0, err
				}
				return resp.Resources[0].ErrorCode, nil
			},
		},
	} {
		for _, enabled := range []bool{true, false} {
			t.Run(test.name+"/export_"+strconv.FormatBool(enabled), func(t *testing.T) {
				s := newTestServer(t)
				s.leaderLease = coordination.LeaderLease{InstanceID: s.instanceID, ExpiresAt: time.Now().Add(time.Minute)}
				ctx := context.Background()
				if err := s.topicStore.Create(ctx, meta.TopicConfig{Name: "events", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeClassic, ExportEnabled: enabled}); err != nil {
					t.Fatalf("seed topic: %v", err)
				}

				export := strconv.FormatBool(enabled)
				code, err := test.invoke(s, ctx, "true", export)
				if err != nil {
					t.Fatalf("mutation: %v", err)
				}
				want := int16(0)
				if enabled {
					want = kafkaErrorInvalidConfig
				}
				if code != want {
					t.Fatalf("mutation error code = %d, want %d", code, want)
				}
				got, err := s.topicStore.Get(ctx, "events")
				if err != nil {
					t.Fatalf("load topic: %v", err)
				}
				if got.UncleanLeaderElection != !enabled {
					t.Fatalf("persisted unclean_leader_election = %v, want %v", got.UncleanLeaderElection, !enabled)
				}
			})
		}
	}
}

func TestParquetExportRejectsUncleanLeaderElectionOnKafkaTopicCreate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		want    int16
	}{
		{name: "enabled", enabled: true, want: kafkaErrorInvalidConfig},
		{name: "disabled", enabled: false, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t)
			s.leaderLease = coordination.LeaderLease{InstanceID: s.instanceID, ExpiresAt: time.Now().Add(time.Minute)}
			unclean := "true"
			exportEnabled := strconv.FormatBool(tc.enabled)
			req := kmsg.NewPtrCreateTopicsRequest()
			req.ValidateOnly = true
			req.Topics = []kmsg.CreateTopicsRequestTopic{{
				Topic:             "events",
				NumPartitions:     1,
				ReplicationFactor: 1,
				Configs: []kmsg.CreateTopicsRequestTopicConfig{{
					Name:  "unclean.leader.election.enable",
					Value: &unclean,
				}, {Name: "camu.export.enabled", Value: &exportEnabled}},
			}}

			resp, err := s.handleKafkaCreateTopics(context.Background(), req)
			if err != nil {
				t.Fatalf("handleKafkaCreateTopics() error = %v", err)
			}
			if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != tc.want {
				t.Fatalf("CreateTopics error = %d, want %d", resp.Topics[0].ErrorCode, tc.want)
			}
		})
	}
}

func TestApplyKafkaTopicConfigsRejectsStorageModeMutation(t *testing.T) {
	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "diskless",
	}

	classicMode := "classic"
	_, err := applyKafkaTopicConfigs(tc, map[string]*string{
		"camu.storage.mode": &classicMode,
	}, false)
	if err == nil {
		t.Fatal("applyKafkaTopicConfigs() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("applyKafkaTopicConfigs() error = %q, want immutable guidance", err)
	}
}

func TestHandleKafkaCreateTopicsRequiresController(t *testing.T) {
	s := newTestServer(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             "topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}}

	resp, err := s.handleKafkaCreateTopics(context.Background(), req)
	if err != nil {
		t.Fatalf("handleKafkaCreateTopics() error = %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("handleKafkaCreateTopics() topics = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != kafkaErrorNotController {
		t.Fatalf("handleKafkaCreateTopics() error = %d, want %d", resp.Topics[0].ErrorCode, kafkaErrorNotController)
	}
	if _, err := s.topicStore.Get(context.Background(), "topic"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("topicStore.Get() error = %v, want not found", err)
	}
}

func TestHandleKafkaDeleteTopicsRequiresController(t *testing.T) {
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

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.SetVersion(5)
	req.TopicNames = []string{"topic"}

	resp, err := s.handleKafkaDeleteTopics(context.Background(), req)
	if err != nil {
		t.Fatalf("handleKafkaDeleteTopics() error = %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("handleKafkaDeleteTopics() topics = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != kafkaErrorNotController {
		t.Fatalf("handleKafkaDeleteTopics() error = %d, want %d", resp.Topics[0].ErrorCode, kafkaErrorNotController)
	}
	if _, err := s.topicStore.Get(context.Background(), "topic"); err != nil {
		t.Fatalf("topicStore.Get() after non-controller delete error = %v, want topic to remain", err)
	}
}

func TestHandleKafkaDeleteTopicsEnqueuesDisklessCleanup(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	s.leaderLease = coordination.LeaderLease{
		InstanceID: s.instanceID,
		ExpiresAt:  time.Now().Add(time.Minute),
	}

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

	_, err := s.disklessMeta.AllocateOffsets(ctx, []diskless.OffsetAllocation{{
		Topic:     "diskless-topic",
		Partition: 0,
		Count:     5,
	}})
	if err != nil {
		t.Fatalf("AllocateOffsets() error = %v", err)
	}
	if err := s.disklessMeta.RegisterSegment(ctx, diskless.SegmentRecord{
		FileKey: "seg-001.dat",
		Batches: []diskless.BatchRef{{
			Topic:      "diskless-topic",
			Partition:  0,
			BaseOffset: 0,
			EndOffset:  5,
			ByteOffset: 0,
			ByteLength: 500,
		}},
	}); err != nil {
		t.Fatalf("RegisterSegment() error = %v", err)
	}

	head, err := s.disklessMeta.GetPartitionHead(ctx, "diskless-topic", 0)
	if err != nil {
		t.Fatalf("GetPartitionHead(before) error = %v", err)
	}
	if head != 5 {
		t.Fatalf("GetPartitionHead(before) = %d, want 5", head)
	}
	refs, err := s.disklessMeta.QuerySegments(ctx, "diskless-topic", 0, 0, 10000)
	if err != nil {
		t.Fatalf("QuerySegments(before) error = %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("QuerySegments(before) = %d refs, want 1", len(refs))
	}

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.SetVersion(5)
	req.TopicNames = []string{"diskless-topic"}
	resp, err := s.handleKafkaDeleteTopics(ctx, req)
	if err != nil {
		t.Fatalf("handleKafkaDeleteTopics() error = %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("handleKafkaDeleteTopics() topics = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("handleKafkaDeleteTopics() error = %d, want 0", resp.Topics[0].ErrorCode)
	}

	if _, err := s.topicStore.Get(ctx, "diskless-topic"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("topicStore.Get(after) error = %v, want ErrNotFound", err)
	}
	if _, err := s.getTopicDeletion(ctx, "diskless-topic"); err != nil {
		t.Fatalf("getTopicDeletion() error = %v, want marker to remain", err)
	}
	head, err = s.disklessMeta.GetPartitionHead(ctx, "diskless-topic", 0)
	if err != nil {
		t.Fatalf("GetPartitionHead(after) error = %v", err)
	}
	if head != 5 {
		t.Fatalf("GetPartitionHead(after enqueue) = %d, want 5", head)
	}
	refs, err = s.disklessMeta.QuerySegments(ctx, "diskless-topic", 0, 0, 10000)
	if err != nil {
		t.Fatalf("QuerySegments(after) error = %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("QuerySegments(after enqueue) = %d refs, want 1", len(refs))
	}
}

func TestHandleKafkaListOffsets_DisklessTimestampLookupReturnsInvalidRequest(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{})
	defer s.disklessEngine.Close()

	tc := meta.TopicConfig{
		Name:              "diskless-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "diskless",
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	_, err := s.handleKafkaListOffsets(context.Background(), "diskless-topic", 0, 1234)
	if !errors.Is(err, errKafkaInvalidRequest) {
		t.Fatalf("handleKafkaListOffsets() error = %v, want %v", err, errKafkaInvalidRequest)
	}
}

func TestDisklessRetentionCleanupDeletesExpiredDataAndAdvancesEarliestOffset(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{})
	defer s.disklessEngine.Close()

	tc := meta.TopicConfig{
		Name:              "diskless-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		StorageMode:       "diskless",
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	s.markTopicDiskless("diskless-topic")

	oldBatch := log.EncodeRecordBatch(0, []log.Message{{Offset: 0, Value: []byte("old")}})
	freshBatch := log.EncodeRecordBatch(1, []log.Message{{Offset: 1, Value: []byte("fresh")}})

	oldFileKey := "_diskless/test-node/old.data"
	freshFileKey := "_diskless/test-node/fresh.data"
	if err := s.s3Client.Put(context.Background(), oldFileKey, oldBatch, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put old batch error = %v", err)
	}
	if err := s.s3Client.Put(context.Background(), freshFileKey, freshBatch, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put fresh batch error = %v", err)
	}

	_, err := s.disklessMeta.AllocateOffsets(context.Background(), []diskless.OffsetAllocation{
		{Topic: "diskless-topic", Partition: 0, Count: 2},
	})
	if err != nil {
		t.Fatalf("AllocateOffsets() error = %v", err)
	}

	if err := s.disklessMeta.RegisterSegment(context.Background(), diskless.SegmentRecord{
		FileKey:   oldFileKey,
		CreatedAt: time.Now().Add(-2 * time.Hour),
		Batches: []diskless.BatchRef{{
			Topic:      "diskless-topic",
			Partition:  0,
			BaseOffset: 0,
			EndOffset:  1,
			ByteOffset: 0,
			ByteLength: int64(len(oldBatch)),
		}},
	}); err != nil {
		t.Fatalf("RegisterSegment(old) error = %v", err)
	}
	if err := s.disklessMeta.RegisterSegment(context.Background(), diskless.SegmentRecord{
		FileKey:   freshFileKey,
		CreatedAt: time.Now(),
		Batches: []diskless.BatchRef{{
			Topic:      "diskless-topic",
			Partition:  0,
			BaseOffset: 1,
			EndOffset:  2,
			ByteOffset: 0,
			ByteLength: int64(len(freshBatch)),
		}},
	}); err != nil {
		t.Fatalf("RegisterSegment(fresh) error = %v", err)
	}

	topics, err := s.topicStore.List(context.Background())
	if err != nil {
		t.Fatalf("topicStore.List() error = %v", err)
	}
	if err := s.assignmentStore.Write(context.Background(), "diskless-topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 9},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["diskless-topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 9},
	}
	s.assignmentsMu.Unlock()
	s.runPartitionMaintenance(context.Background(), topics)

	if _, err := s.s3Client.Get(context.Background(), oldFileKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected expired diskless file to be deleted by owner maintenance, got %v", err)
	}
	refsBefore, err := s.disklessMeta.QuerySegments(context.Background(), "diskless-topic", 0, 0, 1000)
	if err != nil {
		t.Fatalf("QuerySegments(after maintenance) error = %v", err)
	}
	if len(refsBefore) == 0 || refsBefore[0].FileKey != freshFileKey {
		t.Fatalf("QuerySegments(after maintenance) = %+v, want only fresh ref to remain visible", refsBefore)
	}

	if _, err := s.s3Client.Get(context.Background(), oldFileKey); err == nil {
		t.Fatal("expected expired diskless file to be deleted")
	}
	if _, err := s.s3Client.Get(context.Background(), freshFileKey); err != nil {
		t.Fatalf("expected fresh diskless file to remain: %v", err)
	}

	resp, err := s.handleKafkaListOffsets(context.Background(), "diskless-topic", 0, -2)
	if err != nil {
		t.Fatalf("handleKafkaListOffsets() error = %v", err)
	}
	if resp.Offset != 1 {
		t.Fatalf("handleKafkaListOffsets() earliest offset = %d, want 1", resp.Offset)
	}
}

func TestKafkaControllerBrokerUsesLeaderLease(t *testing.T) {
	s := newTestServer(t)
	s.registry = coordination.NewRegistry(s.s3Client, "n1", "127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:19092", time.Minute)
	if err := s.registry.Register(context.Background()); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}

	peerRegistry := coordination.NewRegistry(s.s3Client, "n2", "127.0.0.2:8080", "127.0.0.2:8081", "127.0.0.2:29092", time.Minute)
	if err := peerRegistry.Register(context.Background()); err != nil {
		t.Fatalf("peer registry.Register() error = %v", err)
	}

	lease, acquired, err := s.leaderElection.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("expected n1 to acquire controller lease")
	}
	s.leaderLease = lease

	brokerID, host, port, err := s.kafkaControllerBroker(context.Background())
	if err != nil {
		t.Fatalf("kafkaControllerBroker() error = %v", err)
	}
	wantHost, wantPort := splitKafkaBrokerAddr("127.0.0.1:19092")
	if brokerID != kafkaBrokerID("n1") || host != wantHost || port != wantPort {
		t.Fatalf("kafkaControllerBroker() = (%d,%s,%d), want (%d,%s,%d)", brokerID, host, port, kafkaBrokerID("n1"), wantHost, wantPort)
	}
}

func TestKafkaControllerBrokerPrefersCurrentController(t *testing.T) {
	s := newTestServer(t)
	s.registry = coordination.NewRegistry(s.s3Client, "n1", "127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:19092", time.Minute)
	if err := s.registry.Register(context.Background()); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}

	peerRegistry := coordination.NewRegistry(s.s3Client, "n2", "127.0.0.2:8080", "127.0.0.2:8081", "127.0.0.2:29092", time.Minute)
	if err := peerRegistry.Register(context.Background()); err != nil {
		t.Fatalf("peer registry.Register() error = %v", err)
	}

	peerElection := coordination.NewLeaderElection(s.s3Client, "n2", 30*time.Second)
	lease, acquired, err := peerElection.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("peer TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("expected n2 to acquire controller lease")
	}

	brokerID, host, port, err := s.kafkaControllerBroker(context.Background())
	if err != nil {
		t.Fatalf("kafkaControllerBroker() error = %v", err)
	}
	wantHost, wantPort := splitKafkaBrokerAddr("127.0.0.2:29092")
	if brokerID != kafkaBrokerID("n2") || host != wantHost || port != wantPort {
		t.Fatalf("kafkaControllerBroker() = (%d,%s,%d), want (%d,%s,%d)", brokerID, host, port, kafkaBrokerID("n2"), wantHost, wantPort)
	}
	_ = lease
}

func TestIsLocalKafkaCoordinatorFollowsControllerLease(t *testing.T) {
	s := newTestServer(t)
	s.instanceID = "n1"
	s.registry = coordination.NewRegistry(s.s3Client, "n1", "127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:19092", time.Minute)
	if err := s.registry.Register(context.Background()); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}

	peerRegistry := coordination.NewRegistry(s.s3Client, "n2", "127.0.0.2:8080", "127.0.0.2:8081", "127.0.0.2:29092", time.Minute)
	if err := peerRegistry.Register(context.Background()); err != nil {
		t.Fatalf("peer registry.Register() error = %v", err)
	}

	lease, acquired, err := s.leaderElection.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("expected n1 to acquire controller lease")
	}
	s.leaderLease = lease
	if got := s.isLocalKafkaCoordinator(context.Background(), "any-group"); !got {
		t.Fatalf("isLocalKafkaCoordinator() = %v, want true", got)
	}

	peerElection := coordination.NewLeaderElection(s.s3Client, "n2", 30*time.Second)
	err = s.s3Client.Delete(context.Background(), "_coordination/leader.json")
	if err != nil {
		t.Fatalf("Delete leader lease error = %v", err)
	}
	_, acquired, err = peerElection.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("peer TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("expected n2 to acquire controller lease")
	}
	if got := s.isLocalKafkaCoordinator(context.Background(), "any-group"); got {
		t.Fatalf("isLocalKafkaCoordinator() = %v, want false", got)
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

	body := bytes.NewBufferString(`[{"key":"k","value":"v"}]`)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/topic/partitions/0/messages", body)
	req.SetPathValue("topic", "topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()

	s.handleProduceLowLevel(rec, req)

	if rec.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusMisdirectedRequest, rec.Body.String())
	}
	forwarded, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read forwarded body: %v", err)
	}
	if got, want := string(forwarded), `[{"key":"k","value":"v"}]`; got != want {
		t.Fatalf("forwarded body = %q, want %q", got, want)
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

func TestPublishAssignmentsForTopics_DoesNotShrinkReplicaSetsOnActiveLoss(t *testing.T) {
	s := newTestServer(t)

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 3,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.registry.Register(context.Background()); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}
	initial := coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {
				Replicas:    []string{"n1", "n2", "n3"},
				Leader:      "n1",
				LeaderEpoch: 5,
			},
		},
		Version: 1,
	}
	if err := s.assignmentStore.Write(context.Background(), "topic", initial, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}

	s.publishAssignmentsForTopics(context.Background(), []meta.TopicConfig{tc})

	got, err := s.assignmentStore.Read(context.Background(), "topic")
	if err != nil {
		t.Fatalf("assignmentStore.Read() error = %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("version = %d, want 1", got.Version)
	}
	partition := got.Partitions[0]
	if !reflect.DeepEqual(partition.Replicas, []string{"n1", "n2", "n3"}) {
		t.Fatalf("replicas = %v, want [n1 n2 n3]", partition.Replicas)
	}
	if partition.Leader != "n1" {
		t.Fatalf("leader = %q, want %q", partition.Leader, "n1")
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

func TestInitPartitionAsLeader_RefreshesIndexFromS3BeforeRecoveringTail(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	// Simulate a follower that started before any segments existed locally,
	// then later gets promoted after the old leader has already flushed a
	// committed prefix to S3.
	oldMsgs := []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 3, Key: []byte("k3"), Value: []byte("v3")},
		{Offset: 4, Key: []byte("k4"), Value: []byte("v4")},
	}
	oldSegData := log.EncodeRecordBatch(int64(oldMsgs[0].Offset), oldMsgs)
	oldSegKey := log.FormatSegmentKey("topic", 0, 0, 4, 1)
	if err := s.s3Client.Put(ctx, oldSegKey, oldSegData, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(segment) error = %v", err)
	}
	partState := &log.PartitionState{HighWatermark: 5}
	stateData, err := partState.Marshal()
	if err != nil {
		t.Fatalf("PartitionState.Marshal() error = %v", err)
	}
	if err := s.s3Client.Put(ctx, log.StateKey("topic", 0), stateData, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put(state) error = %v", err)
	}

	seg, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "topic-0-active"), 5)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	if err := seg.Append(log.EncodeRecordBatch(5, []log.Message{
		{Offset: 5, Key: []byte("k5"), Value: []byte("v5"), Timestamp: 1},
		{Offset: 6, Key: []byte("k6"), Value: []byte("v6"), Timestamp: 2},
		{Offset: 7, Key: []byte("k7"), Value: []byte("v7"), Timestamp: 3},
		{Offset: 8, Key: []byte("k8"), Value: []byte("v8"), Timestamp: 4},
		{Offset: 9, Key: []byte("k9"), Value: []byte("v9"), Timestamp: 5},
	})); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
	}
	ps.activeSegment = seg
	ps.nextOffset = 10
	ps.flushedOffset = 4
	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 2},
	}
	s.assignmentsMu.Unlock()

	s.initPartitionAsLeader(ctx, "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1", "n2", "n3"},
		Leader:      "n1",
		LeaderEpoch: 2,
	})

	if got := ps.index.HighWatermark(); got != 10 {
		t.Fatalf("index.HighWatermark() = %d, want 10", got)
	}
	if got := ps.index.NextOffset(); got != 10 {
		t.Fatalf("index.NextOffset() = %d, want 10", got)
	}
	if _, ok := ps.index.Lookup(0); !ok {
		t.Fatal("expected flushed S3 prefix to remain in index after promotion")
	}
	if _, ok := ps.index.Lookup(9); !ok {
		t.Fatal("expected recovered local tail to be flushed into index after promotion")
	}
	if got := ps.flushedOffset; got != 9 {
		t.Fatalf("ps.flushedOffset = %d, want 9", got)
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

func TestLeaderInternalAddr_UsesRegisteredInternalAddress(t *testing.T) {
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
	if err := s.assignmentStore.Write(context.Background(), "topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Replicas: []string{"n2"}, Leader: "n2", LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
	data, err := json.Marshal(coordination.InstanceInfo{
		InstanceID:      "n2",
		Address:         "127.0.0.1:8080",
		InternalAddress: "127.0.0.1:18081",
		HeartbeatAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := s.s3Client.Put(context.Background(), "_coordination/instances/n2.json", data, storage.PutOpts{}); err != nil {
		t.Fatalf("s3Client.Put() error = %v", err)
	}

	if got := s.leaderInternalAddr("topic", 0); got != "127.0.0.1:18081" {
		t.Fatalf("leaderInternalAddr() = %q, want %q", got, "127.0.0.1:18081")
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

func TestHandleConsumeLowLevel_ReturnsCommittedActiveSuffixForOwnedPartition(t *testing.T) {
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
	as, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "topic-0-active"), 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.activeSegment = as
	if err := as.Append(log.EncodeRecordBatch(0, []log.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
	})); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
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

func TestHandleConsumeLowLevel_MergesOverlappingSegmentAndActiveData(t *testing.T) {
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
	segData := log.EncodeRecordBatch(int64(segmentMsgs[0].Offset), segmentMsgs)
	segKey := "topic/0/0-1.segment"
	if err := s.partitionManager.GetDiskCache().Put(segKey, segData); err != nil {
		t.Fatalf("diskCache.Put() error = %v", err)
	}
	putConsumeTestSegment(t, s, segKey, segData, 0, 19)
	ps.index.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  19,
		Epoch:      1,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	activeMsgs := make([]log.Message, 18)
	for i := range activeMsgs {
		offset := uint64(i + 9)
		activeMsgs[i] = log.Message{
			Offset: offset,
			Key:    []byte("active-k" + strconv.Itoa(int(offset))),
			Value:  []byte("active-v" + strconv.Itoa(int(offset))),
		}
	}
	as, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "topic-0-active-overlap"), 9)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.activeSegment = as
	if err := as.Append(log.EncodeRecordBatch(9, activeMsgs)); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
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
	if resp.Messages[9].Key != "active-k9" {
		t.Fatalf("message[9].key = %q, want %q", resp.Messages[9].Key, "active-k9")
	}
	if resp.Messages[26].Key != "active-k26" {
		t.Fatalf("message[26].key = %q, want %q", resp.Messages[26].Key, "active-k26")
	}
}

func TestHandleConsumeLowLevel_MergesActiveDataBeforeApplyingLimit(t *testing.T) {
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
	segData := log.EncodeRecordBatch(int64(segmentMsgs[0].Offset), segmentMsgs)
	segKey := "topic/0/0-1.segment"
	if err := s.partitionManager.GetDiskCache().Put(segKey, segData); err != nil {
		t.Fatalf("diskCache.Put() error = %v", err)
	}
	putConsumeTestSegment(t, s, segKey, segData, 0, 9)
	ps.index.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  9,
		Epoch:      1,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	activeMsgs := make([]log.Message, 10)
	for i := range activeMsgs {
		offset := uint64(i)
		activeMsgs[i] = log.Message{
			Offset: offset,
			Key:    []byte("active-k" + strconv.Itoa(i)),
			Value:  []byte("active-v" + strconv.Itoa(i)),
		}
	}
	as, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "topic-0-active-limit"), 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.activeSegment = as
	if err := as.Append(log.EncodeRecordBatch(0, activeMsgs)); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
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
		if msg.Key != "active-k"+strconv.Itoa(i) {
			t.Fatalf("message[%d].key = %q, want %q", i, msg.Key, "active-k"+strconv.Itoa(i))
		}
	}
}

func TestHandleConsumeLowLevel_ReturnsReadableFollowerActiveSuffix(t *testing.T) {
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
	segData := log.EncodeRecordBatch(int64(segmentMsgs[0].Offset), segmentMsgs)
	segKey := log.FormatSegmentKey("topic", 0, 0, 16, 1)
	if err := s.partitionManager.GetDiskCache().Put(segKey, segData); err != nil {
		t.Fatalf("diskCache.Put() error = %v", err)
	}
	putConsumeTestSegment(t, s, segKey, segData, 0, 16)
	ps.index.Add(log.SegmentRef{
		BaseOffset: 0,
		EndOffset:  16,
		Epoch:      1,
		Key:        segKey,
		CreatedAt:  time.Now(),
	})

	activeMsgs := make([]log.Message, 5)
	for i := range activeMsgs {
		offset := uint64(i + 17)
		activeMsgs[i] = log.Message{
			Offset: offset,
			Key:    []byte("active-k" + strconv.Itoa(int(offset))),
			Value:  []byte("active-v" + strconv.Itoa(int(offset))),
		}
	}
	as, err := log.OpenActiveSegment(filepath.Join(t.TempDir(), "topic-0-active-follower"), 17)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.activeSegment = as
	if err := as.Append(log.EncodeRecordBatch(17, activeMsgs)); err != nil {
		t.Fatalf("activeSegment.Append() error = %v", err)
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
	if resp.Messages[17].Key != "active-k17" {
		t.Fatalf("message[17].key = %q, want %q", resp.Messages[17].Key, "active-k17")
	}
	if resp.Messages[21].Key != "active-k21" {
		t.Fatalf("message[21].key = %q, want %q", resp.Messages[21].Key, "active-k21")
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

func TestInitProducer(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()

	req := httptest.NewRequest(http.MethodPost, "/v1/producers/init", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp struct {
		ProducerID uint64 `json:"producer_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if resp.ProducerID == 0 {
		t.Fatal("expected non-zero producer_id")
	}

	// Second call should return a different ID.
	req2 := httptest.NewRequest(http.MethodPost, "/v1/producers/init", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	var resp2 struct {
		ProducerID uint64 `json:"producer_id"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if resp2.ProducerID == resp.ProducerID {
		t.Fatalf("expected different producer_id, got %d both times", resp.ProducerID)
	}
}

func setupTestTopicAndOwnership(t testing.TB, s *Server) {
	t.Helper()
	tc := meta.TopicConfig{
		Name: "test-topic", Partitions: 1, Retention: time.Hour,
		CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions["test-topic"] = map[int]localPartitionAssignment{0: {Owned: true}}
	s.assignmentsMu.Unlock()
}

func TestProduceIdempotent_Dedup(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()
	setupTestTopicAndOwnership(t, s)

	// Init producer.
	initReq := httptest.NewRequest(http.MethodPost, "/v1/producers/init", nil)
	initRec := httptest.NewRecorder()
	handler.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusCreated {
		t.Fatalf("init status = %d, want %d", initRec.Code, http.StatusCreated)
	}
	var initResp struct {
		ProducerID uint64 `json:"producer_id"`
	}
	if err := json.Unmarshal(initRec.Body.Bytes(), &initResp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	pid := initResp.ProducerID

	// Produce with seq=0 via partition-specific endpoint.
	body := []byte(`{"producer_id":` + strconv.FormatUint(pid, 10) + `,"sequence":0,"messages":[{"key":"k1","value":"v1"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/partitions/0/messages", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("first produce status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var firstResp struct {
		Offsets []struct {
			Partition int    `json:"partition"`
			Offset    uint64 `json:"offset"`
		} `json:"offsets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &firstResp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	// Retry same seq=0 — should get 200 with duplicate flag.
	body2 := []byte(`{"producer_id":` + strconv.FormatUint(pid, 10) + `,"sequence":0,"messages":[{"key":"k1","value":"v1"}]}`)
	req2 := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/partitions/0/messages", bytes.NewReader(body2))
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("retry produce status = %d, want %d; body=%s", rec2.Code, http.StatusOK, rec2.Body.String())
	}

	var retryResp struct {
		Duplicate bool `json:"duplicate"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &retryResp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !retryResp.Duplicate {
		t.Fatal("expected duplicate=true on retry")
	}
}

func TestProduceIdempotent_SequenceGap(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()
	setupTestTopicAndOwnership(t, s)

	// Init producer.
	initReq := httptest.NewRequest(http.MethodPost, "/v1/producers/init", nil)
	initRec := httptest.NewRecorder()
	handler.ServeHTTP(initRec, initReq)
	var initResp struct {
		ProducerID uint64 `json:"producer_id"`
	}
	if err := json.Unmarshal(initRec.Body.Bytes(), &initResp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	pid := initResp.ProducerID

	// Send seq=5 (skip 0) via partition-specific endpoint — should get 422.
	body := []byte(`{"producer_id":` + strconv.FormatUint(pid, 10) + `,"sequence":5,"messages":[{"key":"k1","value":"v1"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/partitions/0/messages", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != 422 {
		t.Fatalf("status = %d, want 422; body=%s", rec.Code, rec.Body.String())
	}
}

func TestProduceWithoutProducerID_BackwardsCompatible(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()
	setupTestTopicAndOwnership(t, s)

	// Produce without producer_id — should succeed as before.
	body := []byte(`[{"key":"k1","value":"v1"}]`)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/messages", bytes.NewReader(body))
	req.SetPathValue("topic", "test-topic")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Offsets []struct {
			Partition int    `json:"partition"`
			Offset    uint64 `json:"offset"`
		} `json:"offsets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Offsets) != 1 {
		t.Fatalf("expected 1 offset, got %d", len(resp.Offsets))
	}
}

func TestProduceHighLevelRejectsSingleObjectBody(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()
	setupTestTopicAndOwnership(t, s)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/messages", bytes.NewBufferString(`{"key":"k1","value":"v1"}`))
	req.SetPathValue("topic", "test-topic")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "expected array") {
		t.Fatalf("body = %q, want error mentioning expected array", rec.Body.String())
	}
}

func TestCommitConsumerOffsetsRejectsInvalidBody(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/topic/offsets/consumer-a", bytes.NewBufferString("{"))
	req.SetPathValue("topic", "topic")
	req.SetPathValue("consumer_id", "consumer-a")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid request body") {
		t.Fatalf("body = %q, want invalid request body", rec.Body.String())
	}
}

func TestCommitGroupOffsetsRejectsInvalidBody(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()

	req := httptest.NewRequest(http.MethodPost, "/v1/groups/group-a/commit", bytes.NewBufferString("{"))
	req.SetPathValue("group_id", "group-a")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid request body") {
		t.Fatalf("body = %q, want invalid request body", rec.Body.String())
	}
}

func TestProduceLowLevelRejectsSingleObjectBody(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()
	setupTestTopicAndOwnership(t, s)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/partitions/0/messages", bytes.NewBufferString(`{"key":"k1","value":"v1"}`))
	req.SetPathValue("topic", "test-topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "expected batch or array") {
		t.Fatalf("body = %q, want error mentioning expected batch or array", rec.Body.String())
	}
}

func TestIdempotency_EndToEnd(t *testing.T) {
	s := newTestServer(t)
	handler := s.publicRoutes()
	setupTestTopicAndOwnership(t, s)

	// Helper: do a produce request via the partition-specific endpoint.
	produce := func(body string) (int, []uint64) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/partitions/0/messages", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == http.StatusOK {
			var resp struct {
				Offsets []struct {
					Offset uint64 `json:"offset"`
				} `json:"offsets"`
			}
			json.Unmarshal(rec.Body.Bytes(), &resp)
			offsets := make([]uint64, len(resp.Offsets))
			for i, o := range resp.Offsets {
				offsets[i] = o.Offset
			}
			return rec.Code, offsets
		}
		return rec.Code, nil
	}

	// 1. Init producer.
	initReq := httptest.NewRequest(http.MethodPost, "/v1/producers/init", nil)
	initRec := httptest.NewRecorder()
	handler.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusCreated {
		t.Fatalf("init status = %d, want 201", initRec.Code)
	}
	var initResp struct {
		ProducerID uint64 `json:"producer_id"`
	}
	json.Unmarshal(initRec.Body.Bytes(), &initResp)
	pid := strconv.FormatUint(initResp.ProducerID, 10)

	// 2. Produce batch of 3 msgs with seq=0 → offsets [0,1,2].
	code, offsets1 := produce(`{"producer_id":` + pid + `,"sequence":0,"messages":[{"value":"a"},{"value":"b"},{"value":"c"}]}`)
	if code != 200 {
		t.Fatalf("step 2: status=%d, want 200", code)
	}
	if len(offsets1) != 3 || offsets1[0] != 0 || offsets1[1] != 1 || offsets1[2] != 2 {
		t.Fatalf("step 2: offsets=%v, want [0 1 2]", offsets1)
	}

	// 3. Produce next batch of 2 msgs with seq=3 → offsets [3,4].
	code, offsets2 := produce(`{"producer_id":` + pid + `,"sequence":3,"messages":[{"value":"d"},{"value":"e"}]}`)
	if code != 200 {
		t.Fatalf("step 3: status=%d, want 200", code)
	}
	if len(offsets2) != 2 || offsets2[0] != 3 || offsets2[1] != 4 {
		t.Fatalf("step 3: offsets=%v, want [3 4]", offsets2)
	}

	// 4. Retry batch with seq=0 → 200 duplicate (no new data written).
	code, _ = produce(`{"producer_id":` + pid + `,"sequence":0,"messages":[{"value":"a"},{"value":"b"},{"value":"c"}]}`)
	if code != 200 {
		t.Fatalf("step 4: status=%d, want 200 (duplicate)", code)
	}

	// 5. Produce with seq=10 (gap) → 422.
	code, _ = produce(`{"producer_id":` + pid + `,"sequence":10,"messages":[{"value":"f"}]}`)
	if code != 422 {
		t.Fatalf("step 5: status=%d, want 422", code)
	}

	// 6. Produce without producer_id via regular batch body → backwards compat, offset 5.
	code, offsets4 := produce(`[{"value":"no-idem"}]`)
	if code != 200 {
		t.Fatalf("step 6: status=%d, want 200", code)
	}
	if len(offsets4) != 1 || offsets4[0] != 5 {
		t.Fatalf("step 6: offsets=%v, want [5]", offsets4)
	}

	// 7. Verify next valid sequence after the gap rejection still works (seq=5).
	code, offsets5 := produce(`{"producer_id":` + pid + `,"sequence":5,"messages":[{"value":"f"}]}`)
	if code != 200 {
		t.Fatalf("step 7: status=%d, want 200", code)
	}
	if len(offsets5) != 1 || offsets5[0] != 6 {
		t.Fatalf("step 7: offsets=%v, want [6]", offsets5)
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

func TestHandleDeleteTopicEnqueuesAsyncCleanup(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Create topic.
	tc := meta.TopicConfig{
		Name:              "doomed",
		Partitions:        2,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("topicStore.Create() error = %v", err)
	}

	// Seed S3 with partition data, assignment, and epoch files.
	seedKeys := []string{
		"doomed/0/state.json",
		"doomed/0/00000000000000000000.seg",
		"doomed/0/00000000000000000000.idx",
		"doomed/0/00000000000000000000.meta.json",
		"doomed/0/producers.checkpoint",
		"doomed/1/state.json",
		"doomed/1/00000000000000000000.seg",
		"_coordination/assignments/doomed.json",
		"_coordination/epochs/doomed/0.json",
		"_coordination/epochs/doomed/1.json",
	}
	for _, key := range seedKeys {
		if err := s.s3Client.Put(ctx, key, []byte(`{}`), storage.PutOpts{}); err != nil {
			t.Fatalf("s3Client.Put(%s) error = %v", key, err)
		}
	}

	// Send DELETE request.
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/doomed", nil)
	req.SetPathValue("topic", "doomed")
	rec := httptest.NewRecorder()
	s.handleDeleteTopic(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify topic metadata is gone.
	if _, err := s.topicStore.Get(ctx, "doomed"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted topic, got %v", err)
	}
	if _, err := s.getTopicDeletion(ctx, "doomed"); err != nil {
		t.Fatalf("expected deletion marker for doomed topic, got %v", err)
	}

	// Verify partition data is still present until GC finalizes cleanup.
	keys, _ := s.s3Client.List(ctx, "doomed/")
	if len(keys) == 0 {
		t.Fatal("expected doomed/ objects to remain until GC")
	}
}

func TestHandleDeleteTopic_NotFound(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/nonexistent", nil)
	req.SetPathValue("topic", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleDeleteTopic(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}
