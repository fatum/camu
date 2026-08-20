package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

func newControllerServer(t *testing.T) (*Server, *coordination.ControllerState) {
	t.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	cs := coordination.NewControllerState()
	cs.SetPartition("orders", 0, &coordination.PartitionMeta{
		Leader:   "node-A",
		Epoch:    1,
		Replicas: []string{"node-A", "node-B"},
		ISR:      []string{"node-A", "node-B"},
		HW:       100,
	})
	s := &Server{
		instanceID:      "controller-node",
		s3Client:        s3Client,
		isrStore:        replication.NewISRStore(s3Client),
		topicStore:      meta.NewTopicStore(s3Client),
		assignmentStore: coordination.NewAssignmentStore(s3Client),
		registry:        coordination.NewRegistry(s3Client, "controller-node", "localhost:8080", "localhost:8081", "", "", 30*time.Second),
		internalClient:  &http.Client{Timeout: 5 * time.Second},
	}
	s.controllerState.Store(cs)

	// Seed the topic so handleReportFailure can read unclean-election config.
	if err := s.topicStore.Create(context.Background(), meta.TopicConfig{
		Name:              "orders",
		Partitions:        1,
		ReplicationFactor: 2,
		MinInsyncReplicas: 1,
		Retention:         time.Hour,
	}); err != nil {
		t.Fatalf("seed topic: %v", err)
	}
	// Seed assignments.
	if err := s.assignmentStore.Write(context.Background(), "orders", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: "node-A", LeaderEpoch: 1, Replicas: []string{"node-A", "node-B"}},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("seed assignments: %v", err)
	}
	// Register the two nodes as active so election candidates are visible.
	registerInstance(t, s3Client, "node-A", "node-a:8081")
	registerInstance(t, s3Client, "node-B", "node-b:8081")
	// Seed the authoritative ISR store so handleReportFailure elects from it
	// rather than the controller's in-memory snapshot.
	if err := s.isrStore.Update(context.Background(), "orders", 0, 1, func(_ replication.ISRState) (replication.ISRState, error) {
		return replication.ISRState{ISR: []string{"node-A", "node-B"}, Leader: "node-A", HighWatermark: 100}, nil
	}); err != nil {
		t.Fatalf("seed ISR: %v", err)
	}
	return s, cs
}

func TestHandleReportFailure(t *testing.T) {
	s, cs := newControllerServer(t)

	body, _ := json.Marshal(reportFailureRequest{
		Topic:        "orders",
		Partition:    0,
		FailedLeader: "node-A",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/report-failure", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleReportFailure(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["new_leader"] != "node-B" {
		t.Fatalf("expected new_leader=node-B, got %v", resp["new_leader"])
	}
	if resp["epoch"].(float64) != 2 {
		t.Fatalf("expected epoch=2, got %v", resp["epoch"])
	}

	// Verify state was mutated.
	meta := cs.GetPartition("orders", 0)
	if meta.Leader != "node-B" {
		t.Fatalf("expected leader=node-B in state, got %s", meta.Leader)
	}
}

func TestHandleReportFailure_NotController(t *testing.T) {
	s := &Server{} // controllerState is nil

	body, _ := json.Marshal(reportFailureRequest{
		Topic:        "orders",
		Partition:    0,
		FailedLeader: "node-A",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/report-failure", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleReportFailure(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleReportFailure_NoEligible(t *testing.T) {
	s, cs := newControllerServer(t)

	// Overwrite the ISR state so only the failed leader remains in ISR.
	if err := s.isrStore.Update(context.Background(), "orders", 0, 1, func(_ replication.ISRState) (replication.ISRState, error) {
		return replication.ISRState{ISR: []string{"node-A"}, Leader: "node-A", HighWatermark: 100}, nil
	}); err != nil {
		t.Fatalf("seed ISR: %v", err)
	}
	_ = cs

	body, _ := json.Marshal(reportFailureRequest{
		Topic:        "orders",
		Partition:    0,
		FailedLeader: "node-A",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/report-failure", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleReportFailure(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleReportISR(t *testing.T) {
	s, cs := newControllerServer(t)

	body, _ := json.Marshal(reportISRRequest{
		Topic:     "orders",
		Partition: 0,
		ISR:       []string{"node-A", "node-B", "node-C"},
		Leader:    "node-A",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/report-isr", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleReportISR(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	meta := cs.GetPartition("orders", 0)
	if len(meta.ISR) != 3 {
		t.Fatalf("expected ISR length 3, got %d", len(meta.ISR))
	}
}

func TestHandleReportHW(t *testing.T) {
	s, cs := newControllerServer(t)

	body, _ := json.Marshal(reportHWRequest{
		Topic:     "orders",
		Partition: 0,
		HW:        12345,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/report-hw", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleReportHW(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	meta := cs.GetPartition("orders", 0)
	if meta.HW != 12345 {
		t.Fatalf("expected HW=12345, got %d", meta.HW)
	}
}

func TestHandleGetAssignments(t *testing.T) {
	s, _ := newControllerServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/internal/assignments", nil)
	rec := httptest.NewRecorder()

	s.handleGetAssignments(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]map[string]*coordination.PartitionMeta
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	orders, ok := resp["orders"]
	if !ok {
		t.Fatal("expected 'orders' topic in response")
	}
	meta, ok := orders["0"]
	if !ok {
		t.Fatal("expected partition 0 in orders")
	}
	if meta.Leader != "node-A" {
		t.Fatalf("expected leader=node-A, got %s", meta.Leader)
	}
}
