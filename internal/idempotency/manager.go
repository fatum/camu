package idempotency

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// Sentinel errors returned by CheckAndAdvance.
var (
	ErrDuplicateSequence = errors.New("duplicate sequence")
	ErrSequenceGap       = errors.New("sequence gap")
	ErrUnknownProducer   = errors.New("unknown producer")
)

// PartitionKey identifies a topic-partition pair.
type PartitionKey struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// BatchInfo describes a single batch for WAL rebuild.
type BatchInfo struct {
	ProducerID uint64
	Sequence   uint64
	BatchSize  int
	Key        PartitionKey
}

// sequenceState tracks the next expected sequence for a single (producer, partition) pair.
type sequenceState struct {
	NextSeq    uint64 `json:"next_seq"`
	LastOffset uint64 `json:"last_offset"` // last offset of the most recent batch (for purgatory join on dedup)
}

// producerEntry holds all per-producer state.
type producerEntry struct {
	ProducerID   uint64                    `json:"producer_id"`
	Partitions   map[string]*sequenceState `json:"partitions"` // key: "topic:N"
	LastActiveAt time.Time                 `json:"last_active_at"`
}

const counterKey = "_coordination/producer_id_counter"

// Manager provides producer idempotency: S3-based ID allocation,
// per-partition sequence checking, and checkpoint/restore.
type Manager struct {
	mu        sync.RWMutex
	producers map[uint64]*producerEntry
	s3        *storage.S3Client
}

// NewManager creates a Manager backed by the given S3 client for ID allocation.
func NewManager(s3Client *storage.S3Client) *Manager {
	return &Manager{
		producers: make(map[uint64]*producerEntry),
		s3:        s3Client,
	}
}

// AllocateProducerID atomically increments the global counter in S3 and returns
// the new ID. This is called once per producer lifetime (not on the hot path).
func (m *Manager) AllocateProducerID(ctx context.Context) (uint64, error) {
	const maxRetries = 10
	for range maxRetries {
		data, etag, err := m.s3.GetWithETag(ctx, counterKey)
		var current uint64
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return 0, fmt.Errorf("idempotency: read counter: %w", err)
			}
			// First allocation — counter doesn't exist yet.
		} else {
			v, perr := strconv.ParseUint(string(bytes.TrimSpace(data)), 10, 64)
			if perr != nil {
				return 0, fmt.Errorf("idempotency: parse counter: %w", perr)
			}
			current = v
		}

		next := current + 1
		newData := []byte(strconv.FormatUint(next, 10))
		if _, err := m.s3.ConditionalPut(ctx, counterKey, newData, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue // retry
			}
			return 0, fmt.Errorf("idempotency: write counter: %w", err)
		}

		// Register the producer in memory.
		m.mu.Lock()
		m.producers[next] = &producerEntry{
			ProducerID:   next,
			Partitions:   make(map[string]*sequenceState),
			LastActiveAt: time.Now(),
		}
		m.mu.Unlock()
		return next, nil
	}
	return 0, fmt.Errorf("idempotency: allocate producer ID: conflict after %d retries", maxRetries)
}

// CheckAndAdvance validates that sequence is the next expected value for the
// given (producer, partition) pair. On success it advances the internal counter
// by batchSize. Special case: an unknown producer with seq=0 is auto-registered.
func (m *Manager) CheckAndAdvance(producerID uint64, key PartitionKey, sequence uint64, batchSize int) error {
	pk := partitionKeyStr(key)

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.producers[producerID]
	if !ok {
		if sequence == 0 {
			// Auto-register.
			entry = &producerEntry{
				ProducerID:   producerID,
				Partitions:   make(map[string]*sequenceState),
				LastActiveAt: time.Now(),
			}
			m.producers[producerID] = entry
		} else {
			return ErrUnknownProducer
		}
	}

	entry.LastActiveAt = time.Now()

	ss, exists := entry.Partitions[pk]
	if !exists {
		ss = &sequenceState{}
		entry.Partitions[pk] = ss
	}

	if sequence < ss.NextSeq {
		return ErrDuplicateSequence
	}
	if sequence > ss.NextSeq {
		return ErrSequenceGap
	}

	ss.NextSeq = sequence + uint64(batchSize)
	return nil
}

// RollbackSequence resets NextSeq back to the given sequence value.
// Called when a WAL write fails after CheckAndAdvance succeeded.
func (m *Manager) RollbackSequence(producerID uint64, key PartitionKey, sequence uint64) {
	pk := partitionKeyStr(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.producers[producerID]
	if !ok {
		return
	}
	if ss, ok := entry.Partitions[pk]; ok {
		ss.NextSeq = sequence
	}
}

// RecordLastOffset stores the last offset assigned to the most recent batch
// for a (producer, partition) pair. Used to join the replication purgatory
// on duplicate detection.
func (m *Manager) RecordLastOffset(producerID uint64, key PartitionKey, lastOffset uint64) {
	pk := partitionKeyStr(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.producers[producerID]
	if !ok {
		return
	}
	if ss, ok := entry.Partitions[pk]; ok {
		ss.LastOffset = lastOffset
	}
}

// GetLastOffset returns the last offset of the most recent batch for a
// (producer, partition) pair, for joining the replication purgatory on dedup.
func (m *Manager) GetLastOffset(producerID uint64, key PartitionKey) (uint64, bool) {
	pk := partitionKeyStr(key)
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.producers[producerID]
	if !ok {
		return 0, false
	}
	ss, ok := entry.Partitions[pk]
	if !ok {
		return 0, false
	}
	return ss.LastOffset, true
}

// EvictStale removes producers that have been idle for longer than ttl and
// returns how many were removed.
func (m *Manager) EvictStale(ttl time.Duration) int {
	cutoff := time.Now().Add(-ttl)

	m.mu.Lock()
	defer m.mu.Unlock()

	var n int
	for id, e := range m.producers {
		if e.LastActiveAt.Before(cutoff) {
			delete(m.producers, id)
			n++
		}
	}
	return n
}

// checkpointEntry is a single line in the NDJSON checkpoint.
type checkpointEntry struct {
	ProducerID uint64         `json:"producer_id"`
	PartKey    string         `json:"part_key"`
	State      *sequenceState `json:"state"`
}

// CheckpointPartition serialises all producer state that touches the given
// partition into NDJSON bytes (one JSON object per line).
func (m *Manager) CheckpointPartition(key PartitionKey) ([]byte, error) {
	pk := partitionKeyStr(key)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var buf bytes.Buffer
	for _, entry := range m.producers {
		ss, ok := entry.Partitions[pk]
		if !ok {
			continue
		}
		line, err := json.Marshal(checkpointEntry{
			ProducerID: entry.ProducerID,
			PartKey:    pk,
			State:      ss,
		})
		if err != nil {
			return nil, fmt.Errorf("idempotency: marshal checkpoint: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// LoadCheckpoint restores state from NDJSON bytes produced by
// CheckpointPartition. It merges into existing state rather than replacing it.
func (m *Manager) LoadCheckpoint(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var ce checkpointEntry
		if err := json.Unmarshal(scanner.Bytes(), &ce); err != nil {
			continue // skip malformed lines
		}
		entry, ok := m.producers[ce.ProducerID]
		if !ok {
			entry = &producerEntry{
				ProducerID: ce.ProducerID,
				Partitions: make(map[string]*sequenceState),
			}
			m.producers[ce.ProducerID] = entry
		}
		entry.Partitions[ce.PartKey] = ce.State
	}
}

// RebuildFromBatches replays WAL batch metadata to advance sequence counters
// past whatever the checkpoint contained. Batches whose sequence is below the
// current NextSeq for their (producer, partition) are silently skipped (already
// covered by the checkpoint).
func (m *Manager) RebuildFromBatches(batches []BatchInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, b := range batches {
		pk := partitionKeyStr(b.Key)
		entry, ok := m.producers[b.ProducerID]
		if !ok {
			entry = &producerEntry{
				ProducerID: b.ProducerID,
				Partitions: make(map[string]*sequenceState),
			}
			m.producers[b.ProducerID] = entry
		}
		ss, ok := entry.Partitions[pk]
		if !ok {
			ss = &sequenceState{}
			entry.Partitions[pk] = ss
		}
		end := b.Sequence + uint64(b.BatchSize)
		if end > ss.NextSeq {
			ss.NextSeq = end
		}
	}
}

// partitionKeyStr returns "topic:N" for the given key.
func partitionKeyStr(k PartitionKey) string {
	return k.Topic + ":" + strconv.Itoa(k.Partition)
}
