package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

func testServerWithRf1Partition(t *testing.T) *Server {
	t.Helper()
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
	s.initPartitionAsLeader(context.Background(), "topic", 0, coordination.PartitionAssignment{
		Replicas:    []string{"n1"},
		Leader:      "n1",
		LeaderEpoch: 1,
	})

	s.assignmentsMu.Lock()
	s.myPartitions["topic"] = map[int]localPartitionAssignment{
		0: {Owned: true, LeaderEpoch: 1},
	}
	s.assignmentsMu.Unlock()
	return s
}

func writeAssignment(t *testing.T, s *Server, leader string, epoch uint64) {
	t.Helper()
	assignments := coordination.TopicAssignments{
		Partitions: map[int]coordination.PartitionAssignment{
			0: {
				Replicas:    []string{"n1", "n2"},
				Leader:      leader,
				LeaderEpoch: epoch,
			},
		},
		Version: epoch,
	}
	etag := ""
	if existing, err := s.assignmentStore.Read(context.Background(), "topic"); err == nil {
		etag = existing.ETag
		assignments.Version = existing.Version + 1
	}
	if err := s.assignmentStore.Write(context.Background(), "topic", assignments, etag); err != nil {
		t.Fatalf("assignmentStore.Write() error = %v", err)
	}
}

func TestVerifyPartitionFence_ValidOwnership(t *testing.T) {
	s := testServerWithRf1Partition(t)
	writeAssignment(t, s, "n1", 1)

	if !s.verifyPartitionFence(context.Background(), "topic", 0, 1) {
		t.Fatal("verifyPartitionFence() = false, want true for valid ownership")
	}
}

func TestVerifyPartitionFence_RevokesOnLostLeadership(t *testing.T) {
	s := testServerWithRf1Partition(t)
	writeAssignment(t, s, "n2", 2)

	if s.verifyPartitionFence(context.Background(), "topic", 0, 1) {
		t.Fatal("verifyPartitionFence() = true, want false when leadership moved")
	}
	if s.isOwnedPartition("topic", 0) {
		t.Fatal("expected ownership to be revoked after fence loss")
	}
}

func TestVerifyPartitionFence_RevokesOnEpochMismatch(t *testing.T) {
	s := testServerWithRf1Partition(t)
	writeAssignment(t, s, "n1", 3)

	if s.verifyPartitionFence(context.Background(), "topic", 0, 1) {
		t.Fatal("verifyPartitionFence() = true, want false on epoch mismatch")
	}
	if s.isOwnedPartition("topic", 0) {
		t.Fatal("expected ownership to be revoked after epoch mismatch")
	}
}

func TestVerifyPartitionFence_NotFoundFallsBackToLocal(t *testing.T) {
	s := testServerWithRf1Partition(t)
	s.readAssignments = func(ctx context.Context, topic string) (coordination.TopicAssignments, error) {
		return coordination.TopicAssignments{}, storage.ErrNotFound
	}

	if !s.verifyPartitionFence(context.Background(), "topic", 0, 1) {
		t.Fatal("verifyPartitionFence() = false, want fallback to local cache on ErrNotFound")
	}
}

func TestVerifyPartitionFence_ReadErrorFailsClosed(t *testing.T) {
	s := testServerWithRf1Partition(t)
	s.readAssignments = func(ctx context.Context, topic string) (coordination.TopicAssignments, error) {
		return coordination.TopicAssignments{}, errors.New("temporary s3 read failure")
	}

	if s.verifyPartitionFence(context.Background(), "topic", 0, 1) {
		t.Fatal("verifyPartitionFence() = true, want false on read error")
	}
}

func TestVerifyPartitionFence_Amortized(t *testing.T) {
	s := testServerWithRf1Partition(t)
	writeAssignment(t, s, "n1", 1)
	s.fenceInterval = time.Hour

	if !s.verifyPartitionFence(context.Background(), "topic", 0, 1) {
		t.Fatal("first verifyPartitionFence() = false")
	}

	// Move the assignment; the amortized cache should still trust ownership
	// within fenceInterval and avoid an S3 read.
	writeAssignment(t, s, "n2", 2)
	if !s.verifyPartitionFence(context.Background(), "topic", 0, 1) {
		t.Fatal("amortized verifyPartitionFence() = false, want cache hit")
	}
	if !s.isOwnedPartition("topic", 0) {
		t.Fatal("expected ownership to remain during the amortized window")
	}
}

func TestOnISRWriteError_StaleEpochRevokes(t *testing.T) {
	s := testServerWithRf1Partition(t)

	s.onISRWriteError("topic", 0, replication.ErrISRStaleEpoch)
	if s.isOwnedPartition("topic", 0) {
		t.Fatal("expected stale-epoch ISR error to revoke ownership")
	}
}

func TestOnISRWriteError_TransientKeepsOwnership(t *testing.T) {
	s := testServerWithRf1Partition(t)

	s.onISRWriteError("topic", 0, errors.New("temporary s3 failure"))
	if !s.isOwnedPartition("topic", 0) {
		t.Fatal("transient ISR write failure should not revoke ownership")
	}
}

func TestProduceRf1RejectsWhenFenced(t *testing.T) {
	s := testServerWithRf1Partition(t)
	// Leadership has moved to another instance.
	writeAssignment(t, s, "n2", 2)

	body := bytes.NewBufferString(`[{"key":"k","value":"v"}]`)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/topic/partitions/0/messages", body)
	req.SetPathValue("topic", "topic")
	req.SetPathValue("id", "0")
	rec := httptest.NewRecorder()

	s.handleProduceLowLevel(rec, req)

	if rec.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusMisdirectedRequest, rec.Body.String())
	}
	if s.isOwnedPartition("topic", 0) {
		t.Fatal("expected produce to revoke fenced rf=1 partition")
	}
	forwarded, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read forwarded body: %v", err)
	}
	if got, want := string(forwarded), `[{"key":"k","value":"v"}]`; got != want {
		t.Fatalf("forwarded body = %q, want %q", got, want)
	}
}
