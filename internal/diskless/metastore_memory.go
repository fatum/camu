package diskless

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type segmentEntry struct {
	fileKey    string
	baseOffset int64
	endOffset  int64
	byteOffset int64
	byteLength int64
	createdAt  time.Time
}

// MemoryMetaStore is an in-memory implementation of MetaStore for testing and development.
type MemoryMetaStore struct {
	mu       sync.Mutex
	offsets  map[string]int64
	segments map[string][]segmentEntry
}

// NewMemoryMetaStore creates a new in-memory MetaStore.
func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		offsets:  make(map[string]int64),
		segments: make(map[string][]segmentEntry),
	}
}

func partitionKey(topic string, partition int) string {
	return fmt.Sprintf("%s#%d", topic, partition)
}

// AllocateOffsets atomically assigns offset ranges for one or more partition batches.
func (m *MemoryMetaStore) AllocateOffsets(_ context.Context, allocs []OffsetAllocation) ([]OffsetResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	results := make([]OffsetResult, len(allocs))
	for i, a := range allocs {
		key := partitionKey(a.Topic, a.Partition)
		base := m.offsets[key]
		results[i] = OffsetResult{BaseOffset: base}
		m.offsets[key] = base + int64(a.Count)
	}
	return results, nil
}

// RegisterSegment records a flushed data file in the segment catalog.
func (m *MemoryMetaStore) RegisterSegment(_ context.Context, seg SegmentRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, b := range seg.Batches {
		key := partitionKey(b.Topic, b.Partition)
		m.segments[key] = append(m.segments[key], segmentEntry{
			fileKey:    seg.FileKey,
			baseOffset: b.BaseOffset,
			endOffset:  b.EndOffset,
			byteOffset: b.ByteOffset,
			byteLength: b.ByteLength,
			createdAt:  seg.CreatedAt,
		})
	}
	return nil
}

// QuerySegments returns segment references covering [fromOffset, ...) for a
// given topic-partition, up to maxBytes of data.
func (m *MemoryMetaStore) QuerySegments(_ context.Context, topic string, partition int,
	fromOffset int64, maxBytes int) ([]SegmentRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := partitionKey(topic, partition)
	entries := m.segments[key]

	var refs []SegmentRef
	var totalBytes int64
	for _, e := range entries {
		if e.endOffset <= fromOffset {
			continue
		}
		if totalBytes+e.byteLength > int64(maxBytes) && len(refs) > 0 {
			break
		}
		refs = append(refs, SegmentRef{
			FileKey:    e.fileKey,
			ByteOffset: e.byteOffset,
			ByteLength: e.byteLength,
			BaseOffset: e.baseOffset,
			EndOffset:  e.endOffset,
		})
		totalBytes += e.byteLength
	}
	return refs, nil
}

// GetPartitionHead returns the next offset that will be allocated for a partition.
func (m *MemoryMetaStore) GetPartitionHead(_ context.Context, topic string, partition int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.offsets[partitionKey(topic, partition)], nil
}

// GetPartitionStart returns the first readable offset for a partition, or the
// current head if all prior segments have been expired.
func (m *MemoryMetaStore) GetPartitionStart(_ context.Context, topic string, partition int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := partitionKey(topic, partition)
	entries := m.segments[key]
	if len(entries) == 0 {
		return m.offsets[key], nil
	}
	return entries[0].baseOffset, nil
}

// PlanExpiredFileDeletes returns file keys whose refs for the given
// topic-partition are expired and whose remaining refs, if any, are also
// expired.
func (m *MemoryMetaStore) PlanExpiredFileDeletes(_ context.Context, topic string, partition int, cutoff time.Time) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := partitionKey(topic, partition)
	entries := m.segments[key]
	if len(entries) == 0 {
		return nil, nil
	}

	candidates := make(map[string]struct{})
	for _, e := range entries {
		if !e.createdAt.After(cutoff) {
			candidates[e.fileKey] = struct{}{}
		}
	}

	deletable := make([]string, 0, len(candidates))
	for fileKey := range candidates {
		if !m.fileHasFreshRefLocked(fileKey, cutoff) {
			deletable = append(deletable, fileKey)
		}
	}
	return deletable, nil
}

// DeleteFileRefs removes all segment refs pointing at fileKey.
func (m *MemoryMetaStore) DeleteFileRefs(_ context.Context, fileKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, entries := range m.segments {
		kept := entries[:0]
		for _, e := range entries {
			if e.fileKey != fileKey {
				kept = append(kept, e)
			}
		}
		if len(kept) == 0 {
			delete(m.segments, key)
		} else {
			m.segments[key] = kept
		}
	}
	return nil
}

// DeleteTopic removes all MetaStore state for a topic.
func (m *MemoryMetaStore) DeleteTopic(_ context.Context, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prefix := topic + "#"
	for k := range m.offsets {
		if strings.HasPrefix(k, prefix) {
			delete(m.offsets, k)
		}
	}
	for k := range m.segments {
		if strings.HasPrefix(k, prefix) {
			delete(m.segments, k)
		}
	}
	return nil
}

func (m *MemoryMetaStore) fileReferencedLocked(fileKey string) bool {
	for _, entries := range m.segments {
		for _, e := range entries {
			if e.fileKey == fileKey {
				return true
			}
		}
	}
	return false
}

func (m *MemoryMetaStore) fileHasFreshRefLocked(fileKey string, cutoff time.Time) bool {
	for _, entries := range m.segments {
		for _, e := range entries {
			if e.fileKey == fileKey && e.createdAt.After(cutoff) {
				return true
			}
		}
	}
	return false
}

// Close releases any resources held by the MetaStore.
func (m *MemoryMetaStore) Close() error {
	return nil
}
