package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/meta"
)

// TestProduceLowLevel_DisklessIdempotentSequenceValidation verifies that an
// idempotent diskless produce rejects gap and out-of-order sequences with 422
// and confirms the duplicate flag on exact retries.
func TestProduceLowLevel_DisklessIdempotentSequenceValidation(t *testing.T) {
	s := newTestServer(t)
	s.disklessMeta = diskless.NewMemoryMetaStore()
	s.disklessEngine = diskless.NewEngine(s.s3Client, s.disklessMeta, s.instanceID, diskless.EngineConfig{LingerMs: 1})
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
	s.assignmentsMu.Lock()
	s.myPartitions["diskless-topic"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()

	produce := func(seq int) (int, string) {
		body := fmt.Sprintf(`{"producer_id":7,"sequence":%d,"messages":[{"key":"k","value":"v"}]}`, seq)
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/diskless-topic/partitions/0/messages", bytes.NewBufferString(body))
		req.SetPathValue("topic", "diskless-topic")
		req.SetPathValue("id", "0")
		rec := httptest.NewRecorder()
		s.handleProduceLowLevel(rec, req)
		return rec.Code, rec.Body.String()
	}

	if code, _ := produce(0); code != http.StatusOK {
		t.Fatalf("first produce status = %d, want 200", code)
	}
	// Sequence 2 after a 1-record batch at sequence 0 is a gap.
	if code, body := produce(2); code != http.StatusUnprocessableEntity {
		t.Fatalf("gap produce status = %d, want 422; body=%s", code, body)
	}
	// Sequence 1 is the exact next contiguous batch.
	if code, _ := produce(1); code != http.StatusOK {
		t.Fatalf("contiguous produce status = %d, want 200", code)
	}
	// Delayed retries remain valid within the retained producer history.
	if code, _ := produce(0); code != http.StatusOK {
		t.Fatalf("delayed retry status = %d, want 200", code)
	}
	// An exact retry of the latest batch (11) is deduplicated and flagged.
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/diskless-topic/partitions/0/messages",
		bytes.NewBufferString(`{"producer_id":7,"sequence":1,"messages":[{"key":"k","value":"v"}]}`))
	req.SetPathValue("topic", "diskless-topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()
	s.handleProduceLowLevel(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("exact retry status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"duplicate":true`)) {
		t.Fatalf("exact retry response missing duplicate flag: %s", rec.Body.String())
	}
}
