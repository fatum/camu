package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/maksim/camu/internal/storage"
)

// PartitionAssignment holds the replica set and leader for a single partition.
//
// Native records the pre-existing replicas of the partition: the nodes that
// held the committed prefix before any backfill. Leadership is restricted to
// native replicas, because a node backfilled into the replica set to maintain
// RF is a follower only until it catches up. Persisting the set (rather than
// reconstructing it from Replicas each cycle) keeps a backfill from being
// mistaken for a native survivor on a later cycle and promoted. Assignments
// written before this field existed (or fresh ones) carry no Native set and are
// treated as all-native by AssignReplicated.
type PartitionAssignment struct {
	Replicas    []string `json:"replicas"`
	Native      []string `json:"native,omitempty"`
	Leader      string   `json:"leader"`
	LeaderEpoch uint64   `json:"leader_epoch"`
}

// TopicAssignments maps partition IDs to PartitionAssignment for a single topic.
type TopicAssignments struct {
	Partitions map[int]PartitionAssignment `json:"partitions"`
	Version    uint64                      `json:"version"`
	ETag       string                      `json:"-"`
}

// AssignmentStore reads and writes partition assignments in S3.
type AssignmentStore struct {
	s3Client *storage.S3Client
}

// NewAssignmentStore creates a new AssignmentStore.
func NewAssignmentStore(s3 *storage.S3Client) *AssignmentStore {
	return &AssignmentStore{s3Client: s3}
}

func assignmentKey(topic string) string {
	return fmt.Sprintf("_coordination/assignments/%s.json", topic)
}

// Write writes partition assignments for a topic using ConditionalPut.
// Pass etag="" for initial creation, or the ETag from a previous Read
// to ensure no concurrent overwrite.
func (as *AssignmentStore) Write(ctx context.Context, topic string, assignments TopicAssignments, etag string) error {
	data, err := json.Marshal(assignments)
	if err != nil {
		return fmt.Errorf("assignments: marshal: %w", err)
	}
	_, err = as.s3Client.ConditionalPut(ctx, assignmentKey(topic), data, etag)
	if err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("assignments: conflict (concurrent write): %w", err)
		}
		return fmt.Errorf("assignments: put: %w", err)
	}
	return nil
}

// Read reads partition assignments for a topic, including the ETag for CAS writes.
func (as *AssignmentStore) Read(ctx context.Context, topic string) (TopicAssignments, error) {
	data, etag, err := as.s3Client.GetWithETag(ctx, assignmentKey(topic))
	if err != nil {
		return TopicAssignments{}, fmt.Errorf("assignments: get: %w", err)
	}
	var assignments TopicAssignments
	if err := json.Unmarshal(data, &assignments); err != nil {
		return TopicAssignments{}, fmt.Errorf("assignments: unmarshal: %w", err)
	}
	assignments.ETag = etag
	return assignments, nil
}
