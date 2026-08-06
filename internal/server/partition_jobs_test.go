package server

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/jobs"
)

// TestPartitionJobQueueRoundTrip proves a job can be put, listed, and
// deleted — the basic contract of the S3-backed per-topic-leader queue.
func TestPartitionJobQueueRoundTrip(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	job := PartitionJob{
		ID:            partitionJobID(jobs.TypeRetention, "events-0-0-9"),
		Topic:         "events",
		Partition:     0,
		Type:          jobs.TypeRetention,
		ExpectedOwner: "n1",
		ExpectedEpoch: 4,
		State:         PartitionJobStatePending,
		Phase:         PartitionJobPhasePublishData,
		Payload:       json.RawMessage(`{"source_offset_start":0,"source_offset_end":9}`),
		StartedAt:     time.Now().UTC(),
	}
	if err := s.putPartitionJob(ctx, job); err != nil {
		t.Fatalf("putPartitionJob: %v", err)
	}
	jobs, err := s.listPartitionJobs(ctx, "events", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("listPartitionJobs returned %d jobs, want 1", len(jobs))
	}
	got := jobs[0]
	if got.ID != job.ID || got.Type != job.Type || got.ExpectedOwner != job.ExpectedOwner || got.ExpectedEpoch != job.ExpectedEpoch {
		t.Fatalf("job round-trip mismatch: got %+v want %+v", got, job)
	}
	if err := s.deletePartitionJob(ctx, "events", 0, job.ID); err != nil {
		t.Fatalf("deletePartitionJob: %v", err)
	}
	after, err := s.listPartitionJobs(ctx, "events", 0)
	if err != nil {
		t.Fatalf("listPartitionJobs after delete: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("after delete len = %d, want 0", len(after))
	}
}

// TestPartitionJobQueueIsolatedPerTopicAndPartition proves that each
// topic-partition has an independent queue — a leader that claims jobs for
// (events, 0) must not see or interfere with jobs for (events, 1) or
// (orders, 0). This is the per-topic-leader guarantee.
func TestPartitionJobQueueIsolatedPerTopicAndPartition(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tasks := []PartitionJob{
		{ID: "retention/events-0-a", Topic: "events", Partition: 0, Type: jobs.TypeRetention, State: PartitionJobStatePending, StartedAt: time.Now()},
		{ID: "retention/events-0-b", Topic: "events", Partition: 0, Type: jobs.TypeRetention, State: PartitionJobStatePending, StartedAt: time.Now()},
		{ID: "retention/events-1-a", Topic: "events", Partition: 1, Type: jobs.TypeRetention, State: PartitionJobStatePending, StartedAt: time.Now()},
		{ID: "retention/orders-0-a", Topic: "orders", Partition: 0, Type: jobs.TypeRetention, State: PartitionJobStatePending, StartedAt: time.Now()},
	}
	for _, j := range tasks {
		if err := s.putPartitionJob(ctx, j); err != nil {
			t.Fatalf("put %s: %v", j.ID, err)
		}
	}

	mustList := func(topic string, part int, want int) []PartitionJob {
		t.Helper()
		jobs, err := s.listPartitionJobs(ctx, topic, part)
		if err != nil {
			t.Fatalf("list %s/%d: %v", topic, part, err)
		}
		if len(jobs) != want {
			t.Fatalf("list %s/%d returned %d jobs, want %d", topic, part, len(jobs), want)
		}
		return jobs
	}
	mustList("events", 0, 2)
	mustList("events", 1, 1)
	mustList("orders", 0, 1)
	mustList("orders", 1, 0)      // no jobs planted here
	mustList("nonexistent", 0, 0) // unknown topic

	// Deleting a job on (events, 0) must not affect (events, 1).
	if err := s.deletePartitionJob(ctx, "events", 0, tasks[0].ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	mustList("events", 0, 1)
	mustList("events", 1, 1)
}

// TestPartitionJobQueueOrderedByUpdatedAt proves listPartitionJobs sorts by
// UpdatedAt ascending — the partition leader picks the oldest pending job
// first, which is how the S3-backed queue preserves FIFO across restarts.
func TestPartitionJobQueueOrderedByUpdatedAt(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	topic, part := "events", 0

	// Put jobs in reverse order of desired UpdatedAt so we know the sort
	// is not just insertion order.
	ids := []string{"c", "a", "b"}
	for i, id := range ids {
		j := PartitionJob{
			ID:        id,
			Topic:     topic,
			Partition: part,
			Type:      jobs.TypeRetention,
			State:     PartitionJobStatePending,
			StartedAt: time.Now().UTC(),
		}
		if err := s.putPartitionJob(ctx, j); err != nil {
			t.Fatalf("put: %v", err)
		}
		// Give each put a distinct UpdatedAt stamp.
		if i < len(ids)-1 {
			time.Sleep(2 * time.Millisecond)
		}
	}

	jobs, err := s.listPartitionJobs(ctx, topic, part)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("len = %d, want 3", len(jobs))
	}
	// Expected order: "c" was put first → smallest UpdatedAt → index 0.
	want := []string{"c", "a", "b"}
	for i, w := range want {
		if jobs[i].ID != w {
			t.Fatalf("jobs[%d].ID = %q, want %q (full order: %v)", i, jobs[i].ID, w, jobIDs(jobs))
		}
	}
}

// TestPartitionJobQueueOverwriteIsIdempotent proves that re-putting the
// same job (same key) updates it in place rather than creating a duplicate.
// This is what lets a maintenance job re-run a phase safely — the second Put
// overwrites rather than accumulating. Idempotent retry is the whole point.
func TestPartitionJobQueueOverwriteIsIdempotent(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	j := PartitionJob{
		ID: "retention/events-0-0-9", Topic: "events", Partition: 0,
		Type: jobs.TypeRetention, State: PartitionJobStatePending,
		StartedAt: time.Now().UTC(),
	}
	for range 5 {
		if err := s.putPartitionJob(ctx, j); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	jobs, err := s.listPartitionJobs(ctx, "events", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len = %d, want 1 (idempotent overwrite)", len(jobs))
	}
	// Progressing the job's state also overwrites cleanly.
	j.State = PartitionJobStateRunning
	j.Phase = PartitionJobPhasePublishData
	if err := s.putPartitionJob(ctx, j); err != nil {
		t.Fatalf("put progressed: %v", err)
	}
	jobs, err = s.listPartitionJobs(ctx, "events", 0)
	if err != nil {
		t.Fatalf("list 2: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != PartitionJobStateRunning || jobs[0].Phase != PartitionJobPhasePublishData {
		t.Fatalf("state transition not reflected: %+v", jobs)
	}
}

// TestPartitionJobQueueConcurrentPutsOnDistinctKeys proves that concurrent
// writes to different job keys within the same topic-partition do not
// corrupt the queue or drop entries. This mimics a partition leader that
// enqueues several jobs from different goroutines.
func TestPartitionJobQueueConcurrentPutsOnDistinctKeys(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j := PartitionJob{
				ID:        partitionJobID(PartitionJobTypeRetention, "events-0-bucket-"+string(rune('a'+i))),
				Topic:     "events",
				Partition: 0,
				Type:      PartitionJobTypeRetention,
				State:     PartitionJobStatePending,
				StartedAt: time.Now().UTC(),
			}
			if err := s.putPartitionJob(ctx, j); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent put: %v", err)
		}
	}
	jobs, err := s.listPartitionJobs(ctx, "events", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(jobs) != n {
		t.Fatalf("len = %d, want %d", len(jobs), n)
	}
}

func jobIDs(jobs []PartitionJob) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}
