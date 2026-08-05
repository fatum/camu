package diskless

import (
	"context"
	"fmt"
	"sort"
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

// producerAlloc records the last idempotent batch allocated by a producer
// within a partition. Only the latest batch is kept: sequences must advance
// contiguously, so any earlier batch is no longer retryable.
type producerAlloc struct {
	firstSequence int64
	baseOffset    int64
	count         int
}

// MemoryMetaStore is an in-memory implementation of MetaStore for testing and development.
type MemoryMetaStore struct {
	mu             sync.Mutex
	offsets        map[string]int64
	committed      map[string]int64
	segments       map[string][]segmentEntry
	producerAllocs map[string]map[int64]producerAlloc // partition key -> producerID -> last batch
}

// NewMemoryMetaStore creates a new in-memory MetaStore.
func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		offsets:        make(map[string]int64),
		committed:      make(map[string]int64),
		segments:       make(map[string][]segmentEntry),
		producerAllocs: make(map[string]map[int64]producerAlloc),
	}
}

func partitionKey(topic string, partition int) string {
	return fmt.Sprintf("%s#%d", topic, partition)
}

// AllocateOffsets atomically assigns offset ranges for one or more partition batches.
func (m *MemoryMetaStore) AllocateOffsets(_ context.Context, allocs []OffsetAllocation) ([]OffsetResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate the whole batch against working copies of the state before
	// mutating anything, so an invalid later allocation can never strand a
	// valid prefix by advancing the counter before the batch is rejected.
	heads := make(map[string]int64, len(m.offsets))
	for k, v := range m.offsets {
		heads[k] = v
	}
	records := make(map[string]map[int64]producerAlloc, len(m.producerAllocs))
	for k, byPID := range m.producerAllocs {
		cp := make(map[int64]producerAlloc, len(byPID))
		for pid, r := range byPID {
			cp[pid] = r
		}
		records[k] = cp
	}

	results := make([]OffsetResult, len(allocs))
	for i, a := range allocs {
		key := partitionKey(a.Topic, a.Partition)
		res, err := simulateMemoryAlloc(heads, records, key, a)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}

	// Apply every validated allocation in order.
	for i, a := range allocs {
		if results[i].Duplicate {
			continue // exact retry: offsets were already assigned
		}
		key := partitionKey(a.Topic, a.Partition)
		base := m.offsets[key]
		results[i].BaseOffset = base
		m.offsets[key] = base + int64(a.Count)
		if a.ProducerID != 0 {
			byProducer := m.producerAllocs[key]
			if byProducer == nil {
				byProducer = make(map[int64]producerAlloc)
				m.producerAllocs[key] = byProducer
			}
			byProducer[a.ProducerID] = producerAlloc{firstSequence: a.Sequence, baseOffset: base, count: a.Count}
		}
	}
	return results, nil
}

// simulateMemoryAlloc applies a single allocation to the in-memory working
// state for validation purposes, returning an error if the allocation is
// invalid. An exact retry does not advance the head, mirroring the apply path.
func simulateMemoryAlloc(heads map[string]int64, records map[string]map[int64]producerAlloc, key string, a OffsetAllocation) (OffsetResult, error) {
	if a.ProducerID != 0 {
		if prev, ok := records[key][a.ProducerID]; ok {
			exact, err := checkProducerSequence(a.ProducerID, a.Sequence, a.Count, prev.firstSequence, prev.count)
			if err != nil {
				return OffsetResult{}, err
			}
			if exact {
				if prev.count != a.Count {
					return OffsetResult{}, fmt.Errorf("producer %d partition %s retried sequence %d with %d records, want %d", a.ProducerID, key, a.Sequence, a.Count, prev.count)
				}
				return OffsetResult{BaseOffset: prev.baseOffset, Duplicate: true}, nil
			}
		}
	}
	base := heads[key]
	heads[key] = base + int64(a.Count)
	if a.ProducerID != 0 {
		if records[key] == nil {
			records[key] = make(map[int64]producerAlloc)
		}
		records[key][a.ProducerID] = producerAlloc{firstSequence: a.Sequence, baseOffset: base, count: a.Count}
	}
	return OffsetResult{BaseOffset: base}, nil
}

// RegisterSegment records a flushed data file in the segment catalog and
// advances the partition's committed head through the longest run of contiguous
// materialized ranges. Registering an already-registered offset range is a
// no-op so an idempotent produce retry that re-materializes a batch does not
// create duplicate refs.
func (m *MemoryMetaStore) RegisterSegment(_ context.Context, seg SegmentRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	affected := make(map[string]bool)
	for _, b := range seg.Batches {
		key := partitionKey(b.Topic, b.Partition)
		if m.rangeRegistered(key, b.BaseOffset, b.EndOffset) {
			continue
		}
		m.segments[key] = append(m.segments[key], segmentEntry{
			fileKey:    seg.FileKey,
			baseOffset: b.BaseOffset,
			endOffset:  b.EndOffset,
			byteOffset: b.ByteOffset,
			byteLength: b.ByteLength,
			createdAt:  seg.CreatedAt,
		})
		affected[key] = true
	}

	// Advance committed heads only through contiguous materialized ranges so an
	// out-of-order registration never exposes a gap to readers.
	for key := range affected {
		m.committed[key] = m.contiguousCommittedLocked(key)
	}
	return nil
}

// contiguousCommittedLocked returns the end of the longest run of segment refs
// for a partition that is contiguous with its current committed head.
func (m *MemoryMetaStore) contiguousCommittedLocked(key string) int64 {
	entries := m.segments[key]
	refs := make([]SegmentRef, 0, len(entries))
	for _, e := range entries {
		refs = append(refs, SegmentRef{BaseOffset: e.baseOffset, EndOffset: e.endOffset})
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].BaseOffset < refs[j].BaseOffset })
	return contiguousCommittedEnd(m.committed[key], refs)
}

// rangeRegistered reports whether a batch for the same offset range is already
// present in the partition's segment list.
func (m *MemoryMetaStore) rangeRegistered(key string, base, end int64) bool {
	for _, e := range m.segments[key] {
		if e.baseOffset == base && e.endOffset == end {
			return true
		}
	}
	return false
}

// ReplaceSegmentRefs atomically removes the refs identified by remove and
// inserts add into the partition's segment list, so readers never observe a gap
// or duplicate for the covered range. The committed watermark is not modified.
func (m *MemoryMetaStore) ReplaceSegmentRefs(_ context.Context, topic string, partition int, remove []RefKey, add []SegmentRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := partitionKey(topic, partition)
	removeSet := make(map[RefKey]bool, len(remove))
	for _, rk := range remove {
		removeSet[rk] = true
	}
	entries := m.segments[key]
	kept := make([]segmentEntry, 0, len(entries))
	changed := false
	for _, e := range entries {
		if removeSet[RefKey{BaseOffset: e.baseOffset, EndOffset: e.endOffset}] {
			changed = true
			continue
		}
		kept = append(kept, e)
	}
	for _, ref := range add {
		if m.rangeRegistered(key, ref.BaseOffset, ref.EndOffset) {
			continue
		}
		kept = append(kept, segmentEntry{
			fileKey:    ref.FileKey,
			baseOffset: ref.BaseOffset,
			endOffset:  ref.EndOffset,
			byteOffset: ref.ByteOffset,
			byteLength: ref.ByteLength,
			createdAt:  time.Now(),
		})
		changed = true
	}
	if !changed {
		return nil
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].baseOffset < kept[j].baseOffset })
	m.segments[key] = kept
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

	// Keep refs in offset order so compaction selection and the read path see a
	// consistent view regardless of registration order.
	sorted := append([]segmentEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].baseOffset < sorted[j].baseOffset })

	var refs []SegmentRef
	var totalBytes int64
	for _, e := range sorted {
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
			CreatedAt:  e.createdAt,
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

// GetCommittedHead returns the highest offset durably materialized for a partition.
func (m *MemoryMetaStore) GetCommittedHead(_ context.Context, topic string, partition int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.committed[partitionKey(topic, partition)], nil
}

// GetPartitionStart returns the first readable offset for a partition, or the
// current head if all prior segments have been expired.
func (m *MemoryMetaStore) GetPartitionStart(_ context.Context, topic string, partition int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := partitionKey(topic, partition)
	entries := m.segments[key]
	if len(entries) == 0 {
		return m.committed[key], nil
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

// PlanUnreferencedFileDeletes returns the subset of fileKeys that appear in no
// partition's segment list, so their data objects can be deleted.
func (m *MemoryMetaStore) PlanUnreferencedFileDeletes(_ context.Context, fileKeys []string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	referenced := make(map[string]bool, len(fileKeys))
	for _, entries := range m.segments {
		for _, e := range entries {
			referenced[e.fileKey] = true
		}
	}
	var deletable []string
	for _, fileKey := range fileKeys {
		if !referenced[fileKey] {
			deletable = append(deletable, fileKey)
		}
	}
	return deletable, nil
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
	for k := range m.committed {
		if strings.HasPrefix(k, prefix) {
			delete(m.committed, k)
		}
	}
	for k := range m.segments {
		if strings.HasPrefix(k, prefix) {
			delete(m.segments, k)
		}
	}
	for k := range m.producerAllocs {
		if strings.HasPrefix(k, prefix) {
			delete(m.producerAllocs, k)
		}
	}
	return nil
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
