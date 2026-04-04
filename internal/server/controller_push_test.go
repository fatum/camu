package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAssignmentPusher_Push(t *testing.T) {
	received := make(chan pushAssignmentRequest, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req pushAssignmentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
			w.WriteHeader(500)
			return
		}
		received <- req
		w.WriteHeader(200)
	}))
	defer srv.Close()

	pusher := NewAssignmentPusher(&http.Client{Timeout: 2 * time.Second})
	// srv.URL is "http://host:port", but Push prepends "http://" — strip it
	addr := strings.TrimPrefix(srv.URL, "http://")
	err := pusher.Push(context.Background(), addr, pushAssignmentRequest{
		Topic: "orders", Partition: 0, Leader: "node-B", Epoch: 2,
		Replicas: []string{"A", "B", "C"}, ISR: []string{"B"}, HW: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-received:
		if req.Leader != "node-B" || req.Epoch != 2 || req.HW != 100 {
			t.Fatalf("unexpected: %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestAssignmentPusher_Push_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	pusher := NewAssignmentPusher(&http.Client{Timeout: 2 * time.Second})
	addr := strings.TrimPrefix(srv.URL, "http://")
	err := pusher.Push(context.Background(), addr, pushAssignmentRequest{
		Topic: "t", Partition: 0, Leader: "A", Epoch: 1,
	})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestHandlePushAssignment(t *testing.T) {
	s := &Server{}
	body := `{"topic":"orders","partition":0,"leader":"node-B","epoch":2}`
	req := httptest.NewRequest("POST", "/v1/internal/push-assignments", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handlePushAssignment(w, req)
	if w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}
