package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maksim/camu/internal/coordination"
)

func newControllerServer() (*Server, *coordination.ControllerState) {
	cs := coordination.NewControllerState()
	cs.SetPartition("orders", 0, &coordination.PartitionMeta{
		Leader:   "node-A",
		Epoch:    1,
		Replicas: []string{"node-A", "node-B"},
		ISR:      []string{"node-A", "node-B"},
		HW:       100,
	})
	s := &Server{}
	s.controllerState.Store(cs)
	return s, cs
}

func TestHandleReportFailure(t *testing.T) {
	s, cs := newControllerServer()

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
	cs := coordination.NewControllerState()
	cs.SetPartition("orders", 0, &coordination.PartitionMeta{
		Leader:   "node-A",
		Epoch:    1,
		Replicas: []string{"node-A"},
		ISR:      []string{"node-A"}, // only the failed leader in ISR
		HW:       100,
	})
	s := &Server{}
	s.controllerState.Store(cs)

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
	s, cs := newControllerServer()

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
	s, cs := newControllerServer()

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
	s, _ := newControllerServer()

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
