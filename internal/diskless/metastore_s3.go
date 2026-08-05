package diskless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// S3MetaStore is a MetaStore backed entirely by the S3-compatible object store
// used for the data plane, so diskless topics do not require DynamoDB.
//
// Offset allocation uses a per-partition head object with a conditional-write
// CAS loop (read ETag -> If-Match increment). Concurrent allocations are safe:
// a lost CAS retries with a fresh head. Segment references live in a single
// per-partition catalog object that is read-modify-written with a CAS, so
// compaction can atomically replace a run of refs without exposing a gap or a
// duplicate to readers.
type S3MetaStore struct {
	s3 *storage.S3Client
}

const (
	s3MetaPrefix       = "_diskless_meta/"
	s3HeadPrefix       = s3MetaPrefix + "head/"
	s3CommittedPrefix  = s3MetaPrefix + "committed/"
	s3CatalogPrefix    = s3MetaPrefix + "catalog/"
	s3SegmentPrefix    = s3MetaPrefix + "seg/"
	s3HeadFile         = ".json"
	s3HeadStateVersion = 1
)

type s3HeadState struct {
	Version    int                       `json:"version"`
	NextOffset int64                     `json:"next_offset"`
	Producers  map[string]s3ProducerBatch `json:"producers,omitempty"`
}

// s3ProducerBatch records the most recent idempotent allocation for a producer
// in a partition, keyed by producer ID. Idempotency follows the Kafka contract:
// a retried batch is deduplicated only when it is the producer's latest batch.
type s3ProducerBatch struct {
	FirstSequence int64 `json:"first_sequence"`
	BaseOffset    int64 `json:"base_offset"`
	Count         int   `json:"count"`
}

type s3CommittedState struct {
	Version         int   `json:"version"`
	CommittedOffset int64 `json:"committed_offset"`
}

// s3CatalogRef is one materialized segment reference within a partition catalog.
type s3CatalogRef struct {
	FileKey    string    `json:"file_key"`
	ByteOffset int64     `json:"byte_offset"`
	ByteLength int64     `json:"byte_length"`
	BaseOffset int64     `json:"base_offset"`
	EndOffset  int64     `json:"end_offset"`
	CreatedAt  time.Time `json:"created_at"`
}

// s3Catalog is the per-partition source of truth for segment references, kept
// sorted by base offset. Refs are immutable once written; compaction replaces a
// contiguous run with a single merged ref via a read-modify-write CAS.
type s3Catalog struct {
	Version int64          `json:"version"`
	Refs    []s3CatalogRef `json:"refs"`
}

// NewS3MetaStore creates a MetaStore backed by s3.
func NewS3MetaStore(s3 *storage.S3Client) *S3MetaStore {
	return &S3MetaStore{s3: s3}
}

func s3HeadKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d%s", s3HeadPrefix, topic, partition, s3HeadFile)
}

func s3CommittedKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d%s", s3CommittedPrefix, topic, partition, s3HeadFile)
}

func s3CatalogKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d%s", s3CatalogPrefix, topic, partition, s3HeadFile)
}

func s3CatalogPrefixForTopic(topic string) string {
	return s3CatalogPrefix + topic + "/"
}

func s3SegPrefixForTopic(topic string) string {
	return s3SegmentPrefix + topic + "/"
}

func s3HeadPrefixForTopic(topic string) string {
	return s3HeadPrefix + topic + "/"
}

// hasRef reports whether the catalog already holds a ref with the given range.
func (c *s3Catalog) hasRef(baseOffset, endOffset int64) bool {
	for _, r := range c.Refs {
		if r.BaseOffset == baseOffset && r.EndOffset == endOffset {
			return true
		}
	}
	return false
}

// sortRefs keeps the catalog ordered by base offset (stable for equal bases).
func (c *s3Catalog) sortRefs() {
	sort.SliceStable(c.Refs, func(i, j int) bool { return c.Refs[i].BaseOffset < c.Refs[j].BaseOffset })
}

// readCatalog returns the partition catalog and its current etag.
func (m *S3MetaStore) readCatalog(ctx context.Context, topic string, partition int) (*s3Catalog, string, error) {
	data, etag, err := m.s3.GetWithETag(ctx, s3CatalogKey(topic, partition))
	switch {
	case err == nil:
		var c s3Catalog
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, "", fmt.Errorf("parse catalog %s/%d: %w", topic, partition, err)
		}
		return &c, etag, nil
	case errors.Is(err, storage.ErrNotFound):
		return &s3Catalog{}, "", nil
	default:
		return nil, "", fmt.Errorf("read catalog %s/%d: %w", topic, partition, err)
	}
}

// writeCatalog CAS-writes the catalog, returning storage.ErrConflict when a
// concurrent writer changed it first.
func (m *S3MetaStore) writeCatalog(ctx context.Context, topic string, partition int, cat *s3Catalog, etag string) error {
	cat.Version++
	data, err := json.Marshal(cat)
	if err != nil {
		return err
	}
	if _, err := m.s3.ConditionalPut(ctx, s3CatalogKey(topic, partition), data, etag); err != nil {
		return fmt.Errorf("update catalog %s/%d: %w", topic, partition, err)
	}
	return nil
}

// AllocateOffsets atomically assigns offset ranges for one or more partition
// batches via per-partition CAS on the head object. The whole batch is
// validated before any offset state is mutated, so an invalid later allocation
// can never strand a valid prefix as a permanent gap in the log.
func (m *S3MetaStore) AllocateOffsets(ctx context.Context, allocs []OffsetAllocation) ([]OffsetResult, error) {
	if len(allocs) == 0 {
		return nil, nil
	}
	// A single allocation cannot strand a prefix (there is none), so skip the
	// extra validation read on the common produce path.
	if len(allocs) > 1 {
		if err := m.validateBatch(ctx, allocs); err != nil {
			return nil, err
		}
	}
	results := make([]OffsetResult, len(allocs))
	for i, alloc := range allocs {
		result, err := m.allocateOne(ctx, alloc)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}
	return results, nil
}

// validateBatch verifies that every allocation in a batch can be applied before
// any offset state is mutated. It simulates the sequential application —
// including producer-record updates made by earlier entries in the batch — so a
// mixed valid/invalid flush is rejected up front and a valid prefix is never
// abandoned. It performs reads only.
func (m *S3MetaStore) validateBatch(ctx context.Context, allocs []OffsetAllocation) error {
	heads := make(map[string]*s3HeadState)
	for _, alloc := range allocs {
		key := s3HeadKey(alloc.Topic, alloc.Partition)
		head := heads[key]
		if head == nil {
			head = &s3HeadState{}
			data, _, err := m.s3.GetWithETag(ctx, key)
			switch {
			case err == nil:
				if err := json.Unmarshal(data, head); err != nil {
					return fmt.Errorf("parse head %s: %w", key, err)
				}
			case errors.Is(err, storage.ErrNotFound):
				// fresh partition: start from an empty head.
			default:
				return fmt.Errorf("read head %s: %w", key, err)
			}
			heads[key] = head
		}
		if err := simulateS3Alloc(key, head, alloc); err != nil {
			return err
		}
	}
	return nil
}

// simulateS3Alloc applies a single allocation to the in-memory head state for
// validation purposes, returning an error if the allocation is invalid. An
// exact retry does not advance the head, mirroring allocateOne.
func simulateS3Alloc(key string, head *s3HeadState, alloc OffsetAllocation) error {
	if alloc.ProducerID != 0 {
		pidKey := strconv.FormatInt(alloc.ProducerID, 10)
		if prev, ok := head.Producers[pidKey]; ok {
			exact, err := checkProducerSequence(alloc.ProducerID, alloc.Sequence, alloc.Count, prev.FirstSequence, prev.Count)
			if err != nil {
				return err
			}
			if exact {
				if prev.Count != alloc.Count {
					return fmt.Errorf("producer %d partition %s retried sequence %d with %d records, want %d", alloc.ProducerID, key, alloc.Sequence, alloc.Count, prev.Count)
				}
				return nil // exact retry: offsets already assigned
			}
		}
	}
	if head.Producers == nil {
		head.Producers = make(map[string]s3ProducerBatch)
	}
	head.NextOffset += int64(alloc.Count)
	if alloc.ProducerID != 0 {
		pidKey := strconv.FormatInt(alloc.ProducerID, 10)
		head.Producers[pidKey] = s3ProducerBatch{
			FirstSequence: alloc.Sequence,
			BaseOffset:    head.NextOffset - int64(alloc.Count),
			Count:         alloc.Count,
		}
	}
	return nil
}

// allocateOne allocates a range for a single batch. For idempotent batches the
// head records the producer's latest allocation; an exact retry (same first
// sequence and record count) returns the prior base offset without advancing
// the counter.
func (m *S3MetaStore) allocateOne(ctx context.Context, alloc OffsetAllocation) (OffsetResult, error) {
	key := s3HeadKey(alloc.Topic, alloc.Partition)
	var pidKey string
	if alloc.ProducerID != 0 {
		pidKey = strconv.FormatInt(alloc.ProducerID, 10)
	}
	for {
		data, etag, err := m.s3.GetWithETag(ctx, key)
		var head s3HeadState
		var next int64
		switch {
		case err == nil:
			if err := json.Unmarshal(data, &head); err != nil {
				return OffsetResult{}, fmt.Errorf("parse head %s: %w", key, err)
			}
			next = head.NextOffset
		case errors.Is(err, storage.ErrNotFound):
			next = 0
		default:
			return OffsetResult{}, fmt.Errorf("read head %s: %w", key, err)
		}

		if pidKey != "" {
			if prev, ok := head.Producers[pidKey]; ok {
				exact, err := checkProducerSequence(alloc.ProducerID, alloc.Sequence, alloc.Count, prev.FirstSequence, prev.Count)
				if err != nil {
					return OffsetResult{}, err
				}
				if exact {
					if prev.Count == alloc.Count {
						return OffsetResult{BaseOffset: prev.BaseOffset, Duplicate: true}, nil
					}
					return OffsetResult{}, fmt.Errorf("producer %d partition %s retried sequence %d with %d records, want %d", alloc.ProducerID, key, alloc.Sequence, alloc.Count, prev.Count)
				}
			}
		}

		base := next
		if head.Producers == nil {
			head.Producers = make(map[string]s3ProducerBatch)
		}
		head.NextOffset = next + int64(alloc.Count)
		if pidKey != "" {
			head.Producers[pidKey] = s3ProducerBatch{
				FirstSequence: alloc.Sequence,
				BaseOffset:    base,
				Count:         alloc.Count,
			}
		}

		newHead, err := json.Marshal(head)
		if err != nil {
			return OffsetResult{}, err
		}
		if _, err := m.s3.ConditionalPut(ctx, key, newHead, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue // another writer advanced the head; retry with a fresh read.
			}
			return OffsetResult{}, fmt.Errorf("update head %s: %w", key, err)
		}
		return OffsetResult{BaseOffset: base}, nil
	}
}

// RegisterSegment records a flushed data file in the partition catalog as
// immutable per-batch references, advancing each partition's committed head.
func (m *S3MetaStore) RegisterSegment(ctx context.Context, seg SegmentRecord) error {
	// Group batch refs by partition so each catalog is updated once.
	type catalogBatch struct {
		topic     string
		partition int
		refs      []s3CatalogRef
	}
	groups := make(map[string]*catalogBatch)
	for _, b := range seg.Batches {
		key := partitionKey(b.Topic, b.Partition)
		g := groups[key]
		if g == nil {
			g = &catalogBatch{topic: b.Topic, partition: b.Partition}
			groups[key] = g
		}
		g.refs = append(g.refs, s3CatalogRef{
			FileKey:    seg.FileKey,
			ByteOffset: b.ByteOffset,
			ByteLength: b.ByteLength,
			BaseOffset: b.BaseOffset,
			EndOffset:  b.EndOffset,
			CreatedAt:  seg.CreatedAt,
		})
	}
	for _, g := range groups {
		if err := m.appendCatalogRefs(ctx, g.topic, g.partition, g.refs); err != nil {
			return err
		}
	}

	// Advance each partition's committed head so reads never report
	// allocated-but-unpersisted offsets as committed. The head only moves
	// through contiguous materialized ranges, so out-of-order registrations
	// from concurrent writers never expose a gap.
	seen := make(map[string]struct{})
	for _, b := range seg.Batches {
		key := partitionKey(b.Topic, b.Partition)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := m.advanceCommitted(ctx, b.Topic, b.Partition); err != nil {
			return err
		}
	}
	return nil
}

// appendCatalogRefs idempotently appends refs to a partition catalog via a
// read-modify-write CAS. Refs already present (by offset range) are skipped so
// a retried registration never creates duplicates.
func (m *S3MetaStore) appendCatalogRefs(ctx context.Context, topic string, partition int, refs []s3CatalogRef) error {
	for {
		cat, etag, err := m.readCatalog(ctx, topic, partition)
		if err != nil {
			return err
		}
		changed := false
		for _, ref := range refs {
			if cat.hasRef(ref.BaseOffset, ref.EndOffset) {
				continue
			}
			cat.Refs = append(cat.Refs, ref)
			changed = true
		}
		if !changed {
			return nil
		}
		cat.sortRefs()
		if err := m.writeCatalog(ctx, topic, partition, cat, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue
			}
			return fmt.Errorf("append catalog refs %s/%d: %w", topic, partition, err)
		}
		return nil
	}
}

// advanceCommitted raises a partition's committed offset to the end of the
// longest run of refs contiguous with the current head via a read-CAS loop.
// It only ever advances, so a concurrent lower update is a no-op.
func (m *S3MetaStore) advanceCommitted(ctx context.Context, topic string, partition int) error {
	key := s3CommittedKey(topic, partition)
	for {
		data, etag, err := m.s3.GetWithETag(ctx, key)
		var committed int64
		switch {
		case err == nil:
			var st s3CommittedState
			if err := json.Unmarshal(data, &st); err != nil {
				return fmt.Errorf("parse committed %s: %w", key, err)
			}
			committed = st.CommittedOffset
		case errors.Is(err, storage.ErrNotFound):
			committed = 0
		default:
			return fmt.Errorf("read committed %s: %w", key, err)
		}

		next, err := m.contiguousCommitted(ctx, topic, partition, committed)
		if err != nil {
			return err
		}
		if next <= committed {
			return nil
		}
		payload, err := json.Marshal(s3CommittedState{Version: s3HeadStateVersion, CommittedOffset: next})
		if err != nil {
			return err
		}
		if _, err := m.s3.ConditionalPut(ctx, key, payload, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue // another writer advanced; re-read and re-walk.
			}
			return fmt.Errorf("update committed %s: %w", key, err)
		}
		return nil
	}
}

// contiguousCommitted returns the end of the longest run of segment refs for
// the partition that is contiguous with the current committed head.
func (m *S3MetaStore) contiguousCommitted(ctx context.Context, topic string, partition int, committed int64) (int64, error) {
	refs, err := m.partitionSegmentRefs(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	return contiguousCommittedEnd(committed, refs), nil
}

// partitionSegmentRefs returns a partition's segment refs in offset order from
// its catalog.
func (m *S3MetaStore) partitionSegmentRefs(ctx context.Context, topic string, partition int) ([]SegmentRef, error) {
	cat, _, err := m.readCatalog(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	refs := make([]SegmentRef, 0, len(cat.Refs))
	for _, r := range cat.Refs {
		refs = append(refs, SegmentRef(r))
	}
	return refs, nil
}

// QuerySegments returns segment references covering [fromOffset, ...) for a
// given topic-partition, up to maxBytes of data.
func (m *S3MetaStore) QuerySegments(ctx context.Context, topic string, partition int,
	fromOffset int64, maxBytes int) ([]SegmentRef, error) {
	cat, _, err := m.readCatalog(ctx, topic, partition)
	if err != nil {
		return nil, err
	}

	var refs []SegmentRef
	var totalBytes int64
	for _, r := range cat.Refs {
		if r.EndOffset <= fromOffset {
			continue
		}
		refs = append(refs, SegmentRef(r))
		totalBytes += r.ByteLength
		if totalBytes >= int64(maxBytes) {
			break
		}
	}
	return refs, nil
}

// GetPartitionHead returns the next offset that will be allocated for a partition.
func (m *S3MetaStore) GetPartitionHead(ctx context.Context, topic string, partition int) (int64, error) {
	data, err := m.s3.Get(ctx, s3HeadKey(topic, partition))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("get head %s/%d: %w", topic, partition, err)
	}
	var head s3HeadState
	if err := json.Unmarshal(data, &head); err != nil {
		return 0, fmt.Errorf("parse head %s/%d: %w", topic, partition, err)
	}
	return head.NextOffset, nil
}

// GetCommittedHead returns the highest offset durably materialized for a
// partition, or 0 if nothing has been registered yet.
func (m *S3MetaStore) GetCommittedHead(ctx context.Context, topic string, partition int) (int64, error) {
	data, err := m.s3.Get(ctx, s3CommittedKey(topic, partition))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("get committed %s/%d: %w", topic, partition, err)
	}
	var st s3CommittedState
	if err := json.Unmarshal(data, &st); err != nil {
		return 0, fmt.Errorf("parse committed %s/%d: %w", topic, partition, err)
	}
	return st.CommittedOffset, nil
}

// GetPartitionStart returns the first readable offset for a partition, or the
// current head if all prior segments have been expired.
func (m *S3MetaStore) GetPartitionStart(ctx context.Context, topic string, partition int) (int64, error) {
	cat, _, err := m.readCatalog(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	if len(cat.Refs) == 0 {
		return m.GetCommittedHead(ctx, topic, partition)
	}
	return cat.Refs[0].BaseOffset, nil
}

// PlanExpiredFileDeletes returns file keys whose refs for the given
// topic-partition are expired and whose remaining refs, if any, are also
// expired.
func (m *S3MetaStore) PlanExpiredFileDeletes(ctx context.Context, topic string, partition int, cutoff time.Time) ([]string, error) {
	cat, _, err := m.readCatalog(ctx, topic, partition)
	if err != nil {
		return nil, err
	}

	candidates := make(map[string]struct{})
	for _, r := range cat.Refs {
		if !r.CreatedAt.After(cutoff) {
			candidates[r.FileKey] = struct{}{}
		}
	}

	var deletable []string
	for fileKey := range candidates {
		fresh, err := m.fileHasFreshRef(ctx, fileKey, cutoff)
		if err != nil {
			return nil, err
		}
		if !fresh {
			deletable = append(deletable, fileKey)
		}
	}
	return deletable, nil
}

// DeleteFileRefs removes all segment references pointing at fileKey from every
// partition catalog.
func (m *S3MetaStore) DeleteFileRefs(ctx context.Context, fileKey string) error {
	catKeys, err := m.s3.List(ctx, s3CatalogPrefix)
	if err != nil {
		return fmt.Errorf("list catalogs: %w", err)
	}
	for _, key := range catKeys {
		topic, partition, err := parseCatalogKey(key)
		if err != nil {
			return err
		}
		for {
			cat, etag, err := m.readCatalog(ctx, topic, partition)
			if err != nil {
				return err
			}
			kept := cat.Refs[:0]
			changed := false
			for _, r := range cat.Refs {
				if r.FileKey == fileKey {
					changed = true
					continue
				}
				kept = append(kept, r)
			}
			if !changed {
				break
			}
			cat.Refs = kept
			if err := m.writeCatalog(ctx, topic, partition, cat, etag); err != nil {
				if errors.Is(err, storage.ErrConflict) {
					continue
				}
				return fmt.Errorf("delete file refs %s: %w", fileKey, err)
			}
			break
		}
	}
	return nil
}

// parseCatalogKey extracts the topic and partition from a catalog key of the
// form _diskless_meta/catalog/{topic}/{partition}.json.
func parseCatalogKey(key string) (string, int, error) {
	rest := strings.TrimSuffix(strings.TrimPrefix(key, s3CatalogPrefix), s3HeadFile)
	idx := strings.LastIndex(rest, "/")
	if idx < 0 {
		return "", 0, fmt.Errorf("malformed catalog key %q", key)
	}
	partition, err := strconv.Atoi(rest[idx+1:])
	if err != nil {
		return "", 0, fmt.Errorf("malformed catalog key %q: %w", key, err)
	}
	return rest[:idx], partition, nil
}

// PlanUnreferencedFileDeletes returns the subset of fileKeys that appear in no
// partition catalog, so their data objects can be deleted after compaction.
func (m *S3MetaStore) PlanUnreferencedFileDeletes(ctx context.Context, fileKeys []string) ([]string, error) {
	referenced := make(map[string]bool, len(fileKeys))
	catKeys, err := m.s3.List(ctx, s3CatalogPrefix)
	if err != nil {
		return nil, fmt.Errorf("list catalogs: %w", err)
	}
	for _, key := range catKeys {
		data, err := m.s3.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("get catalog %s: %w", key, err)
		}
		var c s3Catalog
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, fmt.Errorf("parse catalog %s: %w", key, err)
		}
		for _, r := range c.Refs {
			referenced[r.FileKey] = true
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
// points at fileKey, by scanning the per-partition catalogs.
func (m *S3MetaStore) ListFileRefs(ctx context.Context, fileKey string) ([]FileRef, error) {
	catKeys, err := m.s3.List(ctx, s3CatalogPrefix)
	if err != nil {
		return nil, fmt.Errorf("list catalogs: %w", err)
	}
	var refs []FileRef
	for _, key := range catKeys {
		topic, partition, err := parseCatalogKey(key)
		if err != nil {
			return nil, err
		}
		data, err := m.s3.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("get catalog %s: %w", key, err)
		}
		var c s3Catalog
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, fmt.Errorf("parse catalog %s: %w", key, err)
		}
		for _, r := range c.Refs {
			if r.FileKey == fileKey {
				refs = append(refs, FileRef{Topic: topic, Partition: partition, Ref: SegmentRef(r)})
			}
		}
	}
	return refs, nil
}

// DeleteTopic removes all MetaStore state for a topic.
func (m *S3MetaStore) DeleteTopic(ctx context.Context, topic string) error {
	for _, prefix := range []string{
		s3CatalogPrefixForTopic(topic),
		s3HeadPrefixForTopic(topic),
		s3CommittedPrefix + topic + "/",
		s3SegPrefixForTopic(topic), // legacy per-batch refs, if any remain
	} {
		keys, err := m.s3.List(ctx, prefix)
		if err != nil {
			return fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, key := range keys {
			if err := m.s3.Delete(ctx, key); err != nil {
				return fmt.Errorf("delete %s: %w", key, err)
			}
		}
	}
	return nil
}

// Close releases any resources held by the MetaStore.
func (m *S3MetaStore) Close() error {
	return nil
}

func (m *S3MetaStore) fileHasFreshRef(ctx context.Context, fileKey string, cutoff time.Time) (bool, error) {
	catKeys, err := m.s3.List(ctx, s3CatalogPrefix)
	if err != nil {
		return false, fmt.Errorf("list catalogs: %w", err)
	}
	for _, key := range catKeys {
		data, err := m.s3.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return false, fmt.Errorf("get catalog %s: %w", key, err)
		}
		var c s3Catalog
		if err := json.Unmarshal(data, &c); err != nil {
			return false, fmt.Errorf("parse catalog %s: %w", key, err)
		}
		for _, r := range c.Refs {
			if r.FileKey == fileKey && r.CreatedAt.After(cutoff) {
				return true, nil
			}
		}
	}
	return false, nil
}

// ReplaceSegmentRefs atomically removes the refs identified by remove and
// inserts add into the partition catalog via a read-modify-write CAS, so
// readers never observe a gap or a duplicate for the covered range. The added
// refs must exactly cover the union of the removed ranges (compaction of a
// contiguous run); the committed watermark is never modified. Retries are
// idempotent: an added ref already present is skipped, and already-removed refs
// are simply absent.
func (m *S3MetaStore) ReplaceSegmentRefs(ctx context.Context, topic string, partition int, remove []RefKey, add []SegmentRef) error {
	removeSet := make(map[RefKey]bool, len(remove))
	for _, rk := range remove {
		removeSet[rk] = true
	}
	for {
		cat, etag, err := m.readCatalog(ctx, topic, partition)
		if err != nil {
			return err
		}

		kept := make([]s3CatalogRef, 0, len(cat.Refs))
		changed := false
		for _, r := range cat.Refs {
			if removeSet[RefKey{BaseOffset: r.BaseOffset, EndOffset: r.EndOffset}] {
				changed = true
				continue
			}
			kept = append(kept, r)
		}
		for _, ref := range add {
			cr := s3CatalogRef{
				FileKey:    ref.FileKey,
				ByteOffset: ref.ByteOffset,
				ByteLength: ref.ByteLength,
				BaseOffset: ref.BaseOffset,
				EndOffset:  ref.EndOffset,
				CreatedAt:  time.Now(),
			}
			if cat.hasRef(cr.BaseOffset, cr.EndOffset) {
				continue
			}
			if !overlapsAny(kept, cr.BaseOffset, cr.EndOffset) {
				kept = append(kept, cr)
				changed = true
			}
		}
		if !changed {
			return nil
		}
		cat.Refs = kept
		cat.sortRefs()
		if err := m.writeCatalog(ctx, topic, partition, cat, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue
			}
			return fmt.Errorf("replace refs %s/%d: %w", topic, partition, err)
		}
		return nil
	}
}

// overlapsAny reports whether the range [base,end) overlaps any of refs.
func overlapsAny(refs []s3CatalogRef, baseOffset, endOffset int64) bool {
	for _, r := range refs {
		if baseOffset < r.EndOffset && r.BaseOffset < endOffset {
			return true
		}
	}
	return false
}
