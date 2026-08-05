package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// maxISRUpdateAttempts bounds the read-modify-write retries in Update before
// giving up on a persistently contended ISR object.
const maxISRUpdateAttempts = 8

// ErrISRStaleEpoch is returned by Update when a caller attempts to write ISR
// state with a leader epoch below the currently persisted epoch. The caller is
// a stale leader and must stop acting as the partition leader.
var ErrISRStaleEpoch = errors.New("isr: stale leader epoch")

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
// Pass etag="" for create-if-absent (the object must not exist yet), or the
// ETag from a previous Read for a guarded CAS update. An unconditional
// last-writer-wins overwrite is intentionally not supported: a stale leader
// must never clobber a newer leader's ISR state.
func (s *ISRStore) Write(ctx context.Context, topic string, state ISRState, etag string) error {
	state.UpdatedAt = time.Now()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("isr: marshal: %w", err)
	}
	if _, err := s.s3Client.ConditionalPut(ctx, isrKey(topic, state.Partition), data, etag); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("isr: conditional put %s/%d: %w", topic, state.Partition, err)
		}
		return fmt.Errorf("isr: conditional put: %w", err)
	}
	return nil
}

// Update performs a read-modify-write of the ISR state for a partition. The
// mutator receives the current persisted state (empty on first creation) and
// returns the state to persist. The write is guarded by a conditional PUT on
// the read ETag and retried on conflict. A caller whose wantEpoch is lower than
// the persisted epoch is rejected: it is a stale writer and must not clobber a
// newer leader's state.
func (s *ISRStore) Update(ctx context.Context, topic string, pid int, wantEpoch uint64, mut func(cur ISRState) (ISRState, error)) error {
	for attempt := 0; attempt < maxISRUpdateAttempts; attempt++ {
		cur, err := s.Read(ctx, topic, pid)
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			cur = ISRState{Partition: pid}
		}
		if cur.LeaderEpoch > wantEpoch {
			return fmt.Errorf("%w: %s/%d has epoch %d", ErrISRStaleEpoch, topic, pid, cur.LeaderEpoch)
		}
		next, err := mut(cur)
		if err != nil {
			return err
		}
		next.Partition = pid
		next.LeaderEpoch = wantEpoch
		if err := s.Write(ctx, topic, next, cur.ETag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("isr: update %s/%d after %d attempts: %w", topic, pid, maxISRUpdateAttempts, storage.ErrConflict)
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
