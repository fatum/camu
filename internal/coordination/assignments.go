package coordination

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/maksim/camu/internal/storage"
)

// TopicAssignments maps partition IDs to instance IDs for a single topic.
type TopicAssignments struct {
	Partitions map[int]string `json:"partitions"` // partitionID -> instanceID
	Version    uint64         `json:"version"`
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

// Write writes partition assignments for a topic (leader only).
func (as *AssignmentStore) Write(ctx context.Context, topic string, assignments TopicAssignments) error {
	data, err := json.Marshal(assignments)
	if err != nil {
		return fmt.Errorf("assignments: marshal: %w", err)
	}
	return as.s3Client.Put(ctx, assignmentKey(topic), data, storage.PutOpts{})
}

// Read reads partition assignments for a topic.
func (as *AssignmentStore) Read(ctx context.Context, topic string) (TopicAssignments, error) {
	data, err := as.s3Client.Get(ctx, assignmentKey(topic))
	if err != nil {
		return TopicAssignments{}, fmt.Errorf("assignments: get: %w", err)
	}
	var assignments TopicAssignments
	if err := json.Unmarshal(data, &assignments); err != nil {
		return TopicAssignments{}, fmt.Errorf("assignments: unmarshal: %w", err)
	}
	return assignments, nil
}
