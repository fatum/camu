package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
)

// TestHandleLeaderDown_SelfPromotesWithoutControllerReport verifies that the
// primary failover path is follower self-promotion: when a caught-up replica
// detects the leader is down, it CAS-promotes itself and never contacts the
// controller.
func TestHandleLeaderDown_SelfPromotesWithoutControllerReport(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	var reportHits atomic.Int32
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/internal/report-failure" {
			reportHits.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ctrl.Close()

	// The S3 lease points at a controller node registered at the recording
	// endpoint, so any reportFailureToController call would hit it.
	acquireLeaseAs(t, s.s3Client, "controller")
	registerInstance(t, s.s3Client, "controller", strings.TrimPrefix(ctrl.URL, "http://"))

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	// Current assignment: another instance leads, this node is a replica.
	if err := s.assignmentStore.Write(ctx, "topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Replicas: []string{"other", s.instanceID}, Leader: "other", LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("Write assignment: %v", err)
	}

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}
	ps.mu.Lock()
	ps.isLeader = false
	ps.leaderID = "other"
	ps.mu.Unlock()

	s.partitionFollower().handleLeaderDown("topic", 0)

	if got := reportHits.Load(); got != 0 {
		t.Fatalf("controller report-failure hit %d times, want 0 (self-promotion is primary)", got)
	}
	assignments, err := s.assignmentStore.Read(ctx, "topic")
	if err != nil {
		t.Fatalf("Read assignment: %v", err)
	}
	pa := assignments.Partitions[0]
	if pa.Leader != s.instanceID {
		t.Fatalf("leader = %q, want self %q after self-promotion", pa.Leader, s.instanceID)
	}
	if pa.LeaderEpoch <= 1 {
		t.Fatalf("leader epoch = %d, want > 1 after self-promotion", pa.LeaderEpoch)
	}
	ps.mu.RLock()
	isLeader := ps.isLeader
	ps.mu.RUnlock()
	if !isLeader {
		t.Fatal("partition state did not transition to leader")
	}
}

// TestHandleLeaderDown_ReportsToControllerWhenSelfPromotionFails verifies the
// controller backstop: when self-promotion is not possible, the controller is
// notified.
func TestHandleLeaderDown_ReportsToControllerWhenSelfPromotionFails(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	var reportHits atomic.Int32
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/internal/report-failure" {
			reportHits.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ctrl.Close()

	acquireLeaseAs(t, s.s3Client, "controller")
	registerInstance(t, s.s3Client, "controller", strings.TrimPrefix(ctrl.URL, "http://"))
	// reportFailureToController uses the internal h2c client; point it at a
	// plain HTTP client so the recording httptest server can serve it.
	s.internalClient = &http.Client{Timeout: 3 * time.Second}

	tc := meta.TopicConfig{
		Name:              "topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 3,
		MinInsyncReplicas: 2,
	}
	if err := s.topicStore.Create(ctx, tc); err != nil {
		t.Fatalf("Create topic: %v", err)
	}
	if err := s.partitionManager.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic: %v", err)
	}

	// This node IS a replica but is NOT in the ISR, and unclean leader
	// election is disabled, so self-promotion must refuse and fall back to the
	// controller.
	if err := s.assignmentStore.Write(ctx, "topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Replicas: []string{"other", s.instanceID, "third"}, Leader: "other", LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("Write assignment: %v", err)
	}
	if err := s.isrStore.Write(ctx, "topic", replication.ISRState{
		Partition:     0,
		ISR:           []string{"other", "third"},
		Leader:        "other",
		LeaderEpoch:   1,
		HighWatermark: 0,
	}, ""); err != nil {
		t.Fatalf("Write ISR: %v", err)
	}

	ps := s.partitionManager.GetPartitionState("topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}
	ps.mu.Lock()
	ps.isLeader = false
	ps.leaderID = "other"
	ps.mu.Unlock()

	s.partitionFollower().handleLeaderDown("topic", 0)

	if got := reportHits.Load(); got == 0 {
		t.Fatal("expected self-promotion failure to fall back to the controller")
	}
}
