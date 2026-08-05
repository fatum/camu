package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
)

func TestEvaluateClusterReadiness(t *testing.T) {
	tests := []struct {
		name                                           string
		active, ready, assigned, initialized, expected int
		unavailable                                    bool
		reasons                                        []string
		status                                         string
		wantReady                                      bool
	}{
		{"all-ready", 3, 3, 12, 12, 12, false, nil, "ready", true},
		{"missing-assignment", 3, 3, 11, 11, 12, false, []string{"assignment not initialized"}, "rebalancing", false},
		{"uninitialized-replica", 3, 3, 12, 11, 12, false, []string{"replica initializing"}, "rebalancing", false},
		{"epoch-mismatch", 3, 3, 12, 11, 12, false, []string{"epoch mismatch"}, "rebalancing", false},
		{"unreachable-node", 3, 2, 12, 10, 12, true, []string{"instance unreachable"}, "unavailable", false},
		{"leader-failover", 3, 3, 12, 10, 12, false, []string{"partition initializing after reassignment"}, "rebalancing", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evaluateClusterReadiness(tt.active, tt.ready, tt.assigned, tt.initialized, tt.expected, tt.unavailable, tt.reasons)
			if got.Status != tt.status || got.Ready != tt.wantReady {
				t.Fatalf("status=%q ready=%v, want %q/%v (response=%+v)", got.Status, got.Ready, tt.status, tt.wantReady, got)
			}
		})
	}
}

// readinessTransport stubs the per-instance /v1/internal/readiness fan-out that
// clusterStatus performs, so the readiness loop can run without real nodes.
type readinessTransport struct{ ready bool }

func (t readinessTransport) RoundTrip(*http.Request) (*http.Response, error) {
	body, err := json.Marshal(localReadinessResponse{Ready: t.ready})
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, nil
}

// TestClusterStatusExcludesDisklessTopics verifies that diskless topics neither
// contribute to the classic partition counts nor produce replica-initialization
// reasons, so they never block readiness for classic topics.
func TestClusterStatusExcludesDisklessTopics(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.registry.Register(ctx); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}
	s.internalClient = &http.Client{Transport: readinessTransport{ready: true}}

	now := time.Now()
	classic := meta.TopicConfig{Name: "classic-topic", Partitions: 2, Retention: time.Hour, CreatedAt: now, ReplicationFactor: 1, MinInsyncReplicas: 1}
	if err := s.topicStore.Create(ctx, classic); err != nil {
		t.Fatalf("create classic topic: %v", err)
	}
	if err := s.assignmentStore.Write(ctx, "classic-topic", coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
			1: {Leader: s.instanceID, Replicas: []string{s.instanceID}, LeaderEpoch: 1},
		},
		Version: 1,
	}, ""); err != nil {
		t.Fatalf("assign classic topic: %v", err)
	}
	disklessTopic := meta.TopicConfig{Name: "diskless-topic", Partitions: 4, Retention: time.Hour, CreatedAt: now, ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, disklessTopic); err != nil {
		t.Fatalf("create diskless topic: %v", err)
	}

	status := s.clusterStatus(ctx)
	if status.ExpectedPartitions != 2 {
		t.Fatalf("ExpectedPartitions = %d, want 2 (diskless partitions excluded)", status.ExpectedPartitions)
	}
	for _, reason := range status.Reasons {
		if strings.Contains(reason, "diskless-topic") {
			t.Fatalf("diskless topic blocked cluster readiness: %q", reason)
		}
	}
}

// TestClusterStatusReadyWithOnlyDisklessTopics verifies that a cluster serving
// only diskless topics reports ready instead of waiting on replica
// initialization that diskless topics never have.
func TestClusterStatusReadyWithOnlyDisklessTopics(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.registry.Register(ctx); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}
	s.internalClient = &http.Client{Transport: readinessTransport{ready: true}}

	disklessTopic := meta.TopicConfig{Name: "diskless-topic", Partitions: 4, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeDiskless}
	if err := s.topicStore.Create(ctx, disklessTopic); err != nil {
		t.Fatalf("create diskless topic: %v", err)
	}

	status := s.clusterStatus(ctx)
	if !status.Ready {
		t.Fatalf("cluster with only diskless topics not ready: %s (reasons=%v)", status.Status, status.Reasons)
	}
}
