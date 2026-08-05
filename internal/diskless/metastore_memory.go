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
	batchID    string
	fileKey    string
	baseOffset int64
	endOffset  int64
	byteOffset int64
	byteLength int64
	createdAt  time.Time
}

const uploadedProducerHistory = 5

type producerCommit struct {
	firstSequence, baseOffset int64
	count                     int
}

// MemoryMetaStore is an in-memory implementation of MetaStore for testing and development.
type MemoryMetaStore struct {
	mu              sync.Mutex
	offsets         map[string]int64
	committed       map[string]int64
	segments        map[string][]segmentEntry
	producerCommits map[string]map[int64][]producerCommit
	batchCommits    map[string]OffsetResult // batch id -> durable outcome
}

// NewMemoryMetaStore creates a new in-memory MetaStore.
func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		offsets:         make(map[string]int64),
		committed:       make(map[string]int64),
		segments:        make(map[string][]segmentEntry),
		producerCommits: make(map[string]map[int64][]producerCommit),
		batchCommits:    make(map[string]OffsetResult),
	}
}

// CommitUploadedBatches makes an uploaded object visible without ever
// reserving an offset before the ref is durable. It intentionally accepts
// several partitions, but each partition is committed independently.
func (m *MemoryMetaStore) CommitUploadedBatches(_ context.Context, batches []UploadedBatch) ([]OffsetResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// A commit invocation is intentionally restricted to one partition. Writer
	// commits each uploaded batch separately, so this preserves an all-or-nothing
	// validation boundary and avoids a cross-partition partial prefix.
	if len(batches) > 1 {
		return nil, fmt.Errorf("commit uploaded batches accepts one batch per invocation")
	}
	results := make([]OffsetResult, len(batches))
	for i, b := range batches {
		if b.BatchID == "" || b.Count <= 0 {
			return nil, fmt.Errorf("uploaded batch %s has invalid count %d", b.FileKey, b.Count)
		}
		if old, ok := m.batchCommits[b.BatchID]; ok {
			old.Duplicate = true
			results[i] = old
			continue
		}
		key := partitionKey(b.Topic, b.Partition)
		var duplicate *producerCommit
		if b.ProducerID != 0 {
			history := m.producerCommits[key][b.ProducerID]
			for j := len(history) - 1; j >= 0; j-- {
				if history[j].firstSequence == b.Sequence {
					duplicate = &history[j]
					break
				}
			}
			if duplicate != nil {
				if duplicate.count != b.Count {
					return nil, fmt.Errorf("producer %d retried sequence %d with %d records, want %d", b.ProducerID, b.Sequence, b.Count, duplicate.count)
				}
				results[i] = OffsetResult{BaseOffset: duplicate.baseOffset, Duplicate: true}
				continue
			}
			if len(history) > 0 {
				last := history[len(history)-1]
				if _, err := checkProducerSequence(b.ProducerID, b.Sequence, b.Count, last.firstSequence, last.count); err != nil {
					return nil, err
				}
			} else if err := checkInitialProducerSequence(b.ProducerID, b.Sequence); err != nil {
				return nil, err
			}
		}
		base := m.offsets[key]
		end := base + int64(b.Count)
		m.segments[key] = append(m.segments[key], segmentEntry{batchID: b.BatchID, fileKey: b.FileKey, baseOffset: base, endOffset: end, byteOffset: b.ByteOffset, byteLength: b.ByteLength, createdAt: b.CreatedAt})
		m.offsets[key], m.committed[key] = end, end
		if b.ProducerID != 0 {
			if m.producerCommits[key] == nil {
				m.producerCommits[key] = map[int64][]producerCommit{}
			}
			h := append(m.producerCommits[key][b.ProducerID], producerCommit{b.Sequence, base, b.Count})
			if len(h) > uploadedProducerHistory {
				h = h[len(h)-uploadedProducerHistory:]
			}
			m.producerCommits[key][b.ProducerID] = h
		}
		results[i] = OffsetResult{BaseOffset: base}
		m.batchCommits[b.BatchID] = results[i]
	}
	return results, nil
}

func partitionKey(topic string, partition int) string {
	return fmt.Sprintf("%s#%d", topic, partition)
}

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

// ListFileRefs returns every segment reference across all partitions that
// points at fileKey.
func (m *MemoryMetaStore) ListFileRefs(_ context.Context, fileKey string) ([]FileRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var refs []FileRef
	for key, entries := range m.segments {
		topic, partition, err := parsePartitionKey(key)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.fileKey != fileKey {
				continue
			}
			refs = append(refs, FileRef{Topic: topic, Partition: partition, Ref: SegmentRef{
				FileKey:    e.fileKey,
				ByteOffset: e.byteOffset,
				ByteLength: e.byteLength,
				BaseOffset: e.baseOffset,
				EndOffset:  e.endOffset,
				CreatedAt:  e.createdAt,
			}})
		}
	}
	return refs, nil
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
