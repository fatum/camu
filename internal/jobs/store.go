package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/maksim/camu/internal/jobqueue"
)

type Type string

const (
	TypeRetention    Type = "retention"
	TypeSegmentMerge Type = "segment_merge"
)

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
)

type Phase string

const (
	PhasePublishData Phase = "publish_data"
	PhasePublishMeta Phase = "publish_metadata"
	PhaseDeleteData  Phase = "delete_data"
	PhaseDeleteMeta  Phase = "delete_metadata"
)

// Record is the generic durable job representation used by partition-
// scoped and future non-partition job runners. Scope fields stay explicit
// on the record so the storage/runtime layer remains domain-agnostic.
type Record struct {
	ID            string          `json:"id"`
	Topic         string          `json:"topic"`
	Partition     int             `json:"partition"`
	Type          Type            `json:"type"`
	ExpectedOwner string          `json:"expected_owner"`
	ExpectedEpoch uint64          `json:"expected_epoch"`
	State         State           `json:"state"`
	Phase         Phase           `json:"phase"`
	Payload       json.RawMessage `json:"payload"`
	StartedAt     time.Time       `json:"started_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// ID builds a stable persisted job id from a type + caller-defined key.
func ID(kind Type, key string) string {
	return string(kind) + "/" + strings.TrimSuffix(url.PathEscape(key), ".json")
}

// Store persists job records under a queue root. Ordering semantics live
// here so domain packages do not each re-implement queue decoding/sorting.
type Store struct {
	queue *jobqueue.Queue
}

func NewStore(objects jobqueue.ObjectStore, root string) *Store {
	return &Store{queue: jobqueue.NewQueue(objects, root)}
}

func (s *Store) Put(ctx context.Context, job Record) error {
	job.UpdatedAt = time.Now()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job %q: %w", job.ID, err)
	}
	return s.queue.Put(ctx, job.Topic, job.Partition, job.ID, data)
}

func (s *Store) Delete(ctx context.Context, topic string, partition int, jobID string) error {
	return s.queue.Delete(ctx, topic, partition, jobID)
}

func (s *Store) List(ctx context.Context, topic string, partition int) ([]Record, error) {
	entries, err := s.queue.List(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	jobs := make([]Record, 0, len(entries))
	for _, e := range entries {
		var job Record
		if err := json.Unmarshal(e.Data, &job); err != nil {
			return nil, fmt.Errorf("unmarshal job %q: %w", e.Key, err)
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].UpdatedAt.Equal(jobs[j].UpdatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].UpdatedAt.Before(jobs[j].UpdatedAt)
	})
	return jobs, nil
}
