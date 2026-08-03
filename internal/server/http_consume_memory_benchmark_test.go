//go:build benchmark

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
)

// BenchmarkHTTPConsumeMemory exercises the real low-level HTTP consume
// handler for the maximum atomic response of 1,000 records on a populated
// classic topic. It deliberately excludes HTTP transport so the profile shows
// handler, record decoding, and JSON response allocations.
//
// Example:
//
//	go test -tags benchmark ./internal/server -run '^$' -bench BenchmarkHTTPConsumeMemory \
//	  -benchmem -benchtime=3x -memprofile /tmp/camu-http-consume.mem
//	go tool pprof -top -alloc_space /tmp/camu-http-consume.mem
type httpConsumeMemoryFixture struct {
	server        *Server
	request       *http.Request
	responseBytes int
}

var (
	httpConsumeFixtureOnce sync.Once
	httpConsumeFixture     httpConsumeMemoryFixture
)

func BenchmarkHTTPConsumeMemory(b *testing.B) {
	httpConsumeFixtureOnce.Do(func() {
		httpConsumeFixture = newHTTPConsumeMemoryFixture(b)
	})
	fixture := httpConsumeFixture
	b.SetBytes(int64(fixture.responseBytes))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response := httptest.NewRecorder()
		fixture.server.handleConsumeLowLevel(response, fixture.request)
		if response.Code != http.StatusOK || response.Body.Len() == 0 {
			b.Fatalf("consume status=%d bytes=%d", response.Code, response.Body.Len())
		}
	}
}

func newHTTPConsumeMemoryFixture(b *testing.B) httpConsumeMemoryFixture {
	const (
		topic       = "benchmark-http-consume"
		records     = maxAtomicConsumeLimit
		payloadSize = 1024
	)
	s := newTestServer(b)
	topicConfig := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	ctx := context.Background()
	if err := s.topicStore.Create(ctx, topicConfig); err != nil {
		b.Fatalf("create topic: %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, topicConfig, map[int]uint64{}); err != nil {
		b.Fatalf("initialize topic: %v", err)
	}
	s.initPartitionAsLeader(ctx, topic, 0, coordination.PartitionAssignment{Replicas: []string{"n1"}, Leader: "n1", LeaderEpoch: 1})
	s.assignmentsMu.Lock()
	s.myPartitions[topic] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	ps := s.partitionManager.GetPartitionState(topic, 0)
	if ps == nil || ps.activeSegment == nil {
		b.Fatal("active partition segment was not initialized")
	}
	payload := strings.Repeat("x", payloadSize)
	messages := make([]log.Message, records)
	for index := range messages {
		messages[index] = log.Message{Offset: uint64(index), Timestamp: time.Now().UnixMilli(), Key: []byte(fmt.Sprintf("key-%04d", index)), Value: []byte(payload)}
	}
	if err := ps.activeSegment.Append(log.EncodeRecordBatch(0, messages)); err != nil {
		b.Fatalf("append active segment: %v", err)
	}
	ps.mu.Lock()
	ps.nextOffset = records
	ps.replicaState = replication.NewReplicaState("n1", records, 1, 1000)
	ps.mu.Unlock()

	request := httptest.NewRequest(http.MethodGet, "/v1/topics/benchmark-http-consume/partitions/0/messages?offset=0&limit=1000", nil)
	request.SetPathValue("topic", topic)
	request.SetPathValue("id", "0")
	response := httptest.NewRecorder()
	s.handleConsumeLowLevel(response, request)
	if response.Code != http.StatusOK {
		b.Fatalf("warm consume status = %d: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("X-High-Watermark"); got != "1000" {
		b.Fatalf("warm consume high watermark = %q, want 1000", got)
	}
	var decoded consumeResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		b.Fatalf("decode warm consume response: %v", err)
	}
	if len(decoded.Messages) != records || decoded.NextOffset != records {
		b.Fatalf("warm consume response = messages=%d next_offset=%d, want %d and %d", len(decoded.Messages), decoded.NextOffset, records, records)
	}
	return httpConsumeMemoryFixture{server: s, request: request, responseBytes: response.Body.Len()}
}
