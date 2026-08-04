package diskless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// a lost CAS retries with a fresh head. Segment references are immutable
// create-only objects keyed by padded base offset, so a query lists the
// partition's refs in offset order.
type S3MetaStore struct {
	s3 *storage.S3Client
}

const (
	s3MetaPrefix       = "_diskless_meta/"
	s3HeadPrefix       = s3MetaPrefix + "head/"
	s3SegmentPrefix    = s3MetaPrefix + "seg/"
	s3HeadFile         = ".json"
	s3SegmentFile      = ".json"
	s3HeadStateVersion = 1
)

type s3HeadState struct {
	Version   int   `json:"version"`
	NextOffset int64 `json:"next_offset"`
}

type s3SegmentRef struct {
	FileKey    string    `json:"file_key"`
	ByteOffset int64     `json:"byte_offset"`
	ByteLength int64     `json:"byte_length"`
	BaseOffset int64     `json:"base_offset"`
	EndOffset  int64     `json:"end_offset"`
	CreatedAt  time.Time `json:"created_at"`
}

// NewS3MetaStore creates a MetaStore backed by s3.
func NewS3MetaStore(s3 *storage.S3Client) *S3MetaStore {
	return &S3MetaStore{s3: s3}
}

func s3HeadKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d%s", s3HeadPrefix, topic, partition, s3HeadFile)
}

func s3SegPrefix(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d/", s3SegmentPrefix, topic, partition)
}

func s3SegKey(topic string, partition int, baseOffset, endOffset int64) string {
	return fmt.Sprintf("%s%020d-%020d%s", s3SegPrefix(topic, partition), baseOffset, endOffset, s3SegmentFile)
}

// AllocateOffsets atomically assigns offset ranges for one or more partition
// batches via per-partition CAS on the head object.
func (m *S3MetaStore) AllocateOffsets(ctx context.Context, allocs []OffsetAllocation) ([]OffsetResult, error) {
	results := make([]OffsetResult, len(allocs))
	for i, alloc := range allocs {
		base, err := m.allocateOne(ctx, alloc.Topic, alloc.Partition, alloc.Count)
		if err != nil {
			return nil, err
		}
		results[i] = OffsetResult{BaseOffset: base}
	}
	return results, nil
}

func (m *S3MetaStore) allocateOne(ctx context.Context, topic string, partition, count int) (int64, error) {
	key := s3HeadKey(topic, partition)
	for {
		data, etag, err := m.s3.GetWithETag(ctx, key)
		var next int64
		switch {
		case err == nil:
			var head s3HeadState
			if err := json.Unmarshal(data, &head); err != nil {
				return 0, fmt.Errorf("parse head %s: %w", key, err)
			}
			next = head.NextOffset
		case errors.Is(err, storage.ErrNotFound):
			next = 0
		default:
			return 0, fmt.Errorf("read head %s: %w", key, err)
		}

		newHead, err := json.Marshal(s3HeadState{Version: s3HeadStateVersion, NextOffset: next + int64(count)})
		if err != nil {
			return 0, err
		}
		if _, err := m.s3.ConditionalPut(ctx, key, newHead, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue // another writer advanced the head; retry with a fresh read.
			}
			return 0, fmt.Errorf("update head %s: %w", key, err)
		}
		return next, nil
	}
}

// RegisterSegment records a flushed data file in the segment catalog as
// immutable per-batch reference objects.
func (m *S3MetaStore) RegisterSegment(ctx context.Context, seg SegmentRecord) error {
	for _, b := range seg.Batches {
		key := s3SegKey(b.Topic, b.Partition, b.BaseOffset, b.EndOffset)
		ref, err := json.Marshal(s3SegmentRef{
			FileKey:    seg.FileKey,
			ByteOffset: b.ByteOffset,
			ByteLength: b.ByteLength,
			BaseOffset: b.BaseOffset,
			EndOffset:  b.EndOffset,
			CreatedAt:  seg.CreatedAt,
		})
		if err != nil {
			return err
		}
		// Create-only: retries after a partial registration must not fail on
		// refs that were already written with identical content.
		if _, err := m.s3.ConditionalPut(ctx, key, ref, ""); err != nil && !errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("register segment ref %s: %w", key, err)
		}
	}
	return nil
}

// QuerySegments returns segment references covering [fromOffset, ...) for a
// given topic-partition, up to maxBytes of data.
func (m *S3MetaStore) QuerySegments(ctx context.Context, topic string, partition int,
	fromOffset int64, maxBytes int) ([]SegmentRef, error) {
	keys, err := m.s3.List(ctx, s3SegPrefix(topic, partition))
	if err != nil {
		return nil, fmt.Errorf("list segment refs %s/%d: %w", topic, partition, err)
	}

	var refs []SegmentRef
	var totalBytes int64
	for _, key := range keys {
		data, err := m.s3.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("get segment ref %s: %w", key, err)
		}
		var r s3SegmentRef
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("parse segment ref %s: %w", key, err)
		}
		if r.EndOffset <= fromOffset {
			continue
		}
		refs = append(refs, SegmentRef{
			FileKey:    r.FileKey,
			ByteOffset: r.ByteOffset,
			ByteLength: r.ByteLength,
			BaseOffset: r.BaseOffset,
			EndOffset:  r.EndOffset,
		})
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

// GetPartitionStart returns the first readable offset for a partition, or the
// current head if all prior segments have been expired.
func (m *S3MetaStore) GetPartitionStart(ctx context.Context, topic string, partition int) (int64, error) {
	keys, err := m.s3.List(ctx, s3SegPrefix(topic, partition))
	if err != nil {
		return 0, fmt.Errorf("list segment refs %s/%d: %w", topic, partition, err)
	}
	if len(keys) == 0 {
		return m.GetPartitionHead(ctx, topic, partition)
	}
	base, err := s3SegBaseOffset(keys[0])
	if err != nil {
		return 0, err
	}
	return base, nil
}

// PlanExpiredFileDeletes returns file keys whose refs for the given
// topic-partition are expired and whose remaining refs, if any, are also
// expired.
func (m *S3MetaStore) PlanExpiredFileDeletes(ctx context.Context, topic string, partition int, cutoff time.Time) ([]string, error) {
	keys, err := m.s3.List(ctx, s3SegPrefix(topic, partition))
	if err != nil {
		return nil, fmt.Errorf("list segment refs %s/%d: %w", topic, partition, err)
	}

	candidates := make(map[string]struct{})
	for _, key := range keys {
		data, err := m.s3.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("get segment ref %s: %w", key, err)
		}
		var r s3SegmentRef
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("parse segment ref %s: %w", key, err)
		}
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

// DeleteFileRefs removes all segment references pointing at fileKey.
func (m *S3MetaStore) DeleteFileRefs(ctx context.Context, fileKey string) error {
	prefix := s3SegmentPrefix
	keys, err := m.s3.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list segment refs: %w", err)
	}
	for _, key := range keys {
		data, err := m.s3.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("get segment ref %s: %w", key, err)
		}
		var r s3SegmentRef
		if err := json.Unmarshal(data, &r); err != nil {
			return fmt.Errorf("parse segment ref %s: %w", key, err)
		}
		if r.FileKey == fileKey {
			if err := m.s3.Delete(ctx, key); err != nil {
				return fmt.Errorf("delete segment ref %s: %w", key, err)
			}
		}
	}
	return nil
}

// DeleteTopic removes all MetaStore state for a topic.
func (m *S3MetaStore) DeleteTopic(ctx context.Context, topic string) error {
	prefix := s3SegPrefixForTopic(topic)
	keys, err := m.s3.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list segment refs for %s: %w", topic, err)
	}
	for _, key := range keys {
		if err := m.s3.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete segment ref %s: %w", key, err)
		}
	}
	headPrefix := s3HeadPrefixForTopic(topic)
	keys, err = m.s3.List(ctx, headPrefix)
	if err != nil {
		return fmt.Errorf("list heads for %s: %w", topic, err)
	}
	for _, key := range keys {
		if err := m.s3.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete head %s: %w", key, err)
		}
	}
	return nil
}

// Close releases any resources held by the MetaStore.
func (m *S3MetaStore) Close() error {
	return nil
}

func (m *S3MetaStore) fileHasFreshRef(ctx context.Context, fileKey string, cutoff time.Time) (bool, error) {
	keys, err := m.s3.List(ctx, s3SegmentPrefix)
	if err != nil {
		return false, fmt.Errorf("list segment refs: %w", err)
	}
	for _, key := range keys {
		data, err := m.s3.Get(ctx, key)
		if err != nil {
			return false, fmt.Errorf("get segment ref %s: %w", key, err)
		}
		var r s3SegmentRef
		if err := json.Unmarshal(data, &r); err != nil {
			return false, fmt.Errorf("parse segment ref %s: %w", key, err)
		}
		if r.FileKey == fileKey && r.CreatedAt.After(cutoff) {
			return true, nil
		}
	}
	return false, nil
}

func s3SegPrefixForTopic(topic string) string {
	return s3SegmentPrefix + topic + "/"
}

func s3HeadPrefixForTopic(topic string) string {
	return s3HeadPrefix + topic + "/"
}

// s3SegBaseOffset parses the padded base offset from a segment ref key of the
// form _diskless_meta/seg/{topic}/{partition}/{base:020d}-{end:020d}.json.
func s3SegBaseOffset(key string) (int64, error) {
	name := key[strings.LastIndex(key, "/")+1:]
	idx := strings.Index(name, "-")
	if idx < 0 {
		return 0, fmt.Errorf("malformed segment ref key %q", key)
	}
	return strconv.ParseInt(name[:idx], 10, 64)
}
