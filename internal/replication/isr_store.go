package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// ISRState holds the in-sync replica state for a single partition.
type ISRState struct {
	Partition     int       `json:"partition"`
	ISR           []string  `json:"isr"`
	Leader        string    `json:"leader"`
	LeaderEpoch   uint64    `json:"leader_epoch"`
	HighWatermark uint64    `json:"high_watermark"`
	UpdatedAt     time.Time `json:"updated_at"`
	ETag          string    `json:"-"`
}

// ISRStore reads and writes ISR state in S3.
type ISRStore struct {
	s3Client *storage.S3Client
}

// NewISRStore creates a new ISRStore.
func NewISRStore(s3 *storage.S3Client) *ISRStore {
	return &ISRStore{s3Client: s3}
}

func isrKey(topic string, pid int) string {
	return fmt.Sprintf("_coordination/isr/%s/%d.json", topic, pid)
}

// Write writes ISR state for a topic partition.
// Pass etag="" for initial creation (unconditional write), or the ETag from a
// previous Read to guard against concurrent overwrites (CAS write).
func (s *ISRStore) Write(ctx context.Context, topic string, state ISRState, etag string) error {
	state.UpdatedAt = time.Now()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("isr: marshal: %w", err)
	}
	if etag == "" {
		if err := s.s3Client.Put(ctx, isrKey(topic, state.Partition), data, storage.PutOpts{}); err != nil {
			return fmt.Errorf("isr: put: %w", err)
		}
		return nil
	}
	_, err = s.s3Client.ConditionalPut(ctx, isrKey(topic, state.Partition), data, etag)
	if err != nil {
		return fmt.Errorf("isr: conditional put: %w", err)
	}
	return nil
}

func epochHistoryKey(topic string, pid int) string {
	return fmt.Sprintf("_coordination/epochs/%s/%d.json", topic, pid)
}

// WriteEpochHistory persists the epoch history for a partition to S3.
func (s *ISRStore) WriteEpochHistory(ctx context.Context, topic string, pid int, eh *EpochHistory) error {
	data, err := json.Marshal(eh.Entries)
	if err != nil {
		return fmt.Errorf("epoch history: marshal: %w", err)
	}
	if err := s.s3Client.Put(ctx, epochHistoryKey(topic, pid), data, storage.PutOpts{}); err != nil {
		return fmt.Errorf("epoch history: put: %w", err)
	}
	return nil
}

// ReadEpochHistory loads the epoch history for a partition from S3.
// Returns an empty EpochHistory if not found.
func (s *ISRStore) ReadEpochHistory(ctx context.Context, topic string, pid int) (*EpochHistory, error) {
	data, err := s.s3Client.Get(ctx, epochHistoryKey(topic, pid))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &EpochHistory{}, nil
		}
		return nil, fmt.Errorf("epoch history: get: %w", err)
	}
	eh := &EpochHistory{}
	if err := json.Unmarshal(data, &eh.Entries); err != nil {
		return nil, fmt.Errorf("epoch history: unmarshal: %w", err)
	}
	return eh, nil
}

// Read reads ISR state for a topic partition, including the ETag for CAS writes.
func (s *ISRStore) Read(ctx context.Context, topic string, pid int) (ISRState, error) {
	data, etag, err := s.s3Client.GetWithETag(ctx, isrKey(topic, pid))
	if err != nil {
		return ISRState{}, fmt.Errorf("isr: get: %w", err)
	}
	var state ISRState
	if err := json.Unmarshal(data, &state); err != nil {
		return ISRState{}, fmt.Errorf("isr: unmarshal: %w", err)
	}
	state.ETag = etag
	return state, nil
}
