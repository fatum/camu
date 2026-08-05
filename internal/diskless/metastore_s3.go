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
	s3MetaPrefix     = "_diskless_meta/"
	s3ManifestPrefix = s3MetaPrefix + "manifest/"
	// Catalog operations (compaction and retention) operate on the same
	// authoritative manifest as commits; there is no second ref format.
	s3CatalogPrefix = s3ManifestPrefix
	s3SegmentPrefix = s3MetaPrefix + "seg/"
	s3HeadFile      = ".json"
)

// s3UploadManifest is the clean-cut upload-first state. Unlike the transitional
// head/catalog objects, this one object is the complete ordering authority for
// a partition.
type s3UploadManifest struct {
	Version         int64                        `json:"version"`
	NextOffset      int64                        `json:"next_offset"`
	CommittedOffset int64                        `json:"committed_offset"`
	Refs            []s3CatalogRef               `json:"refs"`
	Producers       map[string][]s3ProducerBatch `json:"producers,omitempty"`
	BatchCommits    map[string]committedBatch    `json:"batch_commits,omitempty"`
}

// s3ProducerBatch records the most recent idempotent allocation for a producer
// in a partition, keyed by producer ID. Idempotency follows the Kafka contract:
// a retried batch is deduplicated only when it is the producer's latest batch.
type s3ProducerBatch struct {
	FirstSequence int64 `json:"first_sequence"`
	BaseOffset    int64 `json:"base_offset"`
	Count         int   `json:"count"`
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

func s3ManifestKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d.json", s3ManifestPrefix, topic, partition)
}

// CommitUploadedBatches publishes already-uploaded batches through a single
// CAS-protected partition manifest. An upload which never reaches this method
// cannot consume an offset or affect the readable head.
func (m *S3MetaStore) CommitUploadedBatches(ctx context.Context, batches []UploadedBatch) ([]OffsetResult, error) {
	if len(batches) > 1 {
		return nil, fmt.Errorf("commit uploaded batches accepts one batch per invocation")
	}
	results := make([]OffsetResult, len(batches))
	for i, batch := range batches {
		if batch.BatchID == "" || batch.Count <= 0 {
			return nil, fmt.Errorf("uploaded batch %s has invalid count %d", batch.FileKey, batch.Count)
		}
		for {
			key := s3ManifestKey(batch.Topic, batch.Partition)
			data, etag, err := m.s3.GetWithETag(ctx, key)
			manifest := s3UploadManifest{Producers: map[string][]s3ProducerBatch{}, BatchCommits: map[string]committedBatch{}}
			if err == nil {
				if err := json.Unmarshal(data, &manifest); err != nil {
					return nil, fmt.Errorf("parse upload manifest %s/%d: %w", batch.Topic, batch.Partition, err)
				}
			} else if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			if old, ok := manifest.BatchCommits[batch.BatchID]; ok {
				old.Result.Duplicate = true
				results[i] = old.Result
				break
			}
			pid := strconv.FormatInt(batch.ProducerID, 10)
			duplicate := false
			if batch.ProducerID != 0 {
				h := manifest.Producers[pid]
				for _, old := range h {
					if old.FirstSequence != batch.Sequence {
						continue
					}
					if old.Count != batch.Count {
						return nil, fmt.Errorf("producer %d retried sequence %d with different count", batch.ProducerID, batch.Sequence)
					}
					results[i] = OffsetResult{BaseOffset: old.BaseOffset, Duplicate: true}
					duplicate = true
					break
				}
				if len(h) > 0 {
					last := h[len(h)-1]
					if _, err := checkProducerSequence(batch.ProducerID, batch.Sequence, batch.Count, last.FirstSequence, last.Count); err != nil {
						return nil, err
					}
				} else if err := checkInitialProducerSequence(batch.ProducerID, batch.Sequence); err != nil {
					return nil, err
				}
			}
			if duplicate {
				break
			}
			base := manifest.NextOffset
			end := base + int64(batch.Count)
			manifest.NextOffset, manifest.CommittedOffset = end, end
			manifest.Refs = append(manifest.Refs, s3CatalogRef{FileKey: batch.FileKey, ByteOffset: batch.ByteOffset, ByteLength: batch.ByteLength, BaseOffset: base, EndOffset: end, CreatedAt: batch.CreatedAt})
			manifest.BatchCommits[batch.BatchID] = committedBatch{Result: OffsetResult{BaseOffset: base}, CommittedAt: time.Now()}
			pruneBatchCommits(manifest.BatchCommits, time.Now())
			if batch.ProducerID != 0 {
				h := manifest.Producers[pid]
				h = append(h, s3ProducerBatch{FirstSequence: batch.Sequence, BaseOffset: base, Count: batch.Count})
				if len(h) > uploadedProducerHistory {
					h = h[len(h)-uploadedProducerHistory:]
				}
				manifest.Producers[pid] = h
			}
			manifest.Version++
			encoded, err := json.Marshal(manifest)
			if err != nil {
				return nil, err
			}
			if _, err = m.s3.ConditionalPut(ctx, key, encoded, etag); err != nil {
				if errors.Is(err, storage.ErrConflict) {
					continue
				}
				return nil, fmt.Errorf("commit uploaded batch %s: %w", batch.FileKey, err)
			}
			results[i] = OffsetResult{BaseOffset: base}
			break
		}
	}
	return results, nil
}

func s3CatalogPrefixForTopic(topic string) string {
	return s3CatalogPrefix + topic + "/"
}

func s3SegPrefixForTopic(topic string) string {
	return s3SegmentPrefix + topic + "/"
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
	data, etag, err := m.s3.GetWithETag(ctx, s3ManifestKey(topic, partition))
	switch {
	case err == nil:
		var manifest s3UploadManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, "", fmt.Errorf("parse catalog %s/%d: %w", topic, partition, err)
		}
		return &s3Catalog{Version: manifest.Version, Refs: manifest.Refs}, etag, nil
	case errors.Is(err, storage.ErrNotFound):
		return &s3Catalog{}, "", nil
	default:
		return nil, "", fmt.Errorf("read catalog %s/%d: %w", topic, partition, err)
	}
}

// writeCatalog CAS-writes the catalog, returning storage.ErrConflict when a
// concurrent writer changed it first.
func (m *S3MetaStore) writeCatalog(ctx context.Context, topic string, partition int, cat *s3Catalog, etag string) error {
	// The ETag passed by readCatalog is for the manifest. Re-read is avoided so
	// the CAS detects a concurrent append, preserving producer/batch history.
	data, currentETag, err := m.s3.GetWithETag(ctx, s3ManifestKey(topic, partition))
	if err != nil {
		return fmt.Errorf("read manifest for catalog update %s/%d: %w", topic, partition, err)
	}
	if currentETag != etag {
		return storage.ErrConflict
	}
	var manifest s3UploadManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("parse manifest for catalog update %s/%d: %w", topic, partition, err)
	}
	manifest.Version++
	manifest.Refs = cat.Refs
	data, err = json.Marshal(manifest)
	if err != nil {
		return err
	}
	if _, err := m.s3.ConditionalPut(ctx, s3ManifestKey(topic, partition), data, etag); err != nil {
		return fmt.Errorf("update catalog %s/%d: %w", topic, partition, err)
	}
	return nil
}

func (m *S3MetaStore) QuerySegments(ctx context.Context, topic string, partition int,
	fromOffset int64, maxBytes int) ([]SegmentRef, error) {
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	if err != nil {
		return nil, err
	}

	var refs []SegmentRef
	var totalBytes int64
	for _, r := range manifest.Refs {
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
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	return manifest.NextOffset, err
}

// GetCommittedHead returns the highest offset durably materialized for a
// partition, or 0 if nothing has been registered yet.
func (m *S3MetaStore) GetCommittedHead(ctx context.Context, topic string, partition int) (int64, error) {
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	return manifest.CommittedOffset, err
}

func (m *S3MetaStore) readUploadManifest(ctx context.Context, topic string, partition int) (*s3UploadManifest, error) {
	data, err := m.s3.Get(ctx, s3ManifestKey(topic, partition))
	if errors.Is(err, storage.ErrNotFound) {
		return &s3UploadManifest{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get upload manifest %s/%d: %w", topic, partition, err)
	}
	var manifest s3UploadManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse upload manifest %s/%d: %w", topic, partition, err)
	}
	return &manifest, nil
}

// GetPartitionStart returns the first readable offset for a partition, or the
// current head if all prior segments have been expired.
func (m *S3MetaStore) GetPartitionStart(ctx context.Context, topic string, partition int) (int64, error) {
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	if len(manifest.Refs) == 0 {
		return m.GetCommittedHead(ctx, topic, partition)
	}
	return manifest.Refs[0].BaseOffset, nil
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
		s3ManifestPrefix + topic + "/",
		"_diskless_meta/head/" + topic + "/",      // legacy, if any remain
		"_diskless_meta/committed/" + topic + "/", // legacy, if any remain
		s3SegPrefixForTopic(topic),                // legacy per-batch refs, if any remain
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
