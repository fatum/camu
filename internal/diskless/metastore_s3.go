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
// a lost CAS retries with a fresh head.
//
// Unlike a monolithic catalog, the head is a *bounded window* of recent
// segment references. Old compaction-sized refs are rolled into immutable
// archived checkpoints by a background job, so no single object ever holds the
// whole partition history: commits stay O(window), not O(history), and the hot
// head object stays small regardless of how many segments have ever been
// produced. Reads of recent data touch only the head; reads of older data walk
// the checkpoint chain (immutable, so amortized cheaply cacheable).
//
// Gap-freedom is structural: offsets, refs, and the committed watermark move
// together in the head's single CAS, exactly as before the split. Checkpoints
// only ever contain fully-committed contiguous refs that were already in the
// head, so archiving cannot expose a hole or a duplicate to readers.
type S3MetaStore struct {
	s3 *storage.S3Client

	// headMaxRefCount / headMaxRefBytes bound the head window; once either is
	// crossed the archive job rolls compaction-final refs into checkpoints.
	headMaxRefCount   int
	headMaxRefBytes   int64
	checkpointMaxRefs int
}

const (
	s3MetaPrefix     = "_diskless_meta/"
	s3ManifestPrefix = s3MetaPrefix + "manifest/"
	// Catalog operations (compaction and retention) operate on the head object;
	// there is no second ref format.
	s3CatalogPrefix = s3ManifestPrefix
	// s3ArchivePrefix holds immutable per-partition checkpoints that archived
	// compaction-sized refs out of the head window.
	s3ArchivePrefix = s3MetaPrefix + "archive/"
	s3SegmentPrefix = s3MetaPrefix + "seg/"
	s3HeadFile      = ".json"
)

// Head-window and checkpoint bounds. Commits rewrite the head, so it stays
// small; the archive job rolls refs out of it once it crosses a bound.
const (
	// s3HeadMaxRefCount bounds the number of refs in the head window.
	s3HeadMaxRefCount = 512
	// s3HeadMaxRefBytes bounds the head window's total ref bytes.
	s3HeadMaxRefBytes = 1 << 17 // 128KiB
	// s3CheckpointMaxRefs bounds one archived checkpoint.
	s3CheckpointMaxRefs = 4096
)

// s3UploadManifest is the head: the complete ordering authority for a
// partition's recent state. Offsets below Archive.End live in checkpoints;
// offsets from Archive.End up live in Refs.
type s3UploadManifest struct {
	Version         int64                        `json:"version"`
	NextOffset      int64                        `json:"next_offset"`
	CommittedOffset int64                        `json:"committed_offset"`
	Producers       map[string][]s3ProducerBatch `json:"producers,omitempty"`
	Archive         *s3ArchivePointer            `json:"archive,omitempty"`
	Refs            []s3CatalogRef               `json:"refs"`
}

// s3ArchivePointer names the newest archived checkpoint and the offset where
// the head window begins (every offset below End is covered by checkpoints).
type s3ArchivePointer struct {
	Key string `json:"key"`
	End int64  `json:"end"`
}

// s3Checkpoint is an immutable batch of archived refs covering
// [PrevEnd, End), chained to its predecessor via PrevKey. It is written once by
// the archive job and only ever removed wholesale by retention; a retention
// rewrite rewrites the object in place (versioned), never the data it points at.
type s3Checkpoint struct {
	Version int64          `json:"version"`
	End     int64          `json:"end"`
	PrevKey string         `json:"prev_key,omitempty"`
	PrevEnd int64          `json:"prev_end,omitempty"`
	Refs    []s3CatalogRef `json:"refs"`
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

// s3Catalog is the per-partition window of segment references, kept sorted by
// base offset. Refs are immutable once written; compaction replaces a
// contiguous run with a single merged ref via a read-modify-write CAS on the
// head.
type s3Catalog struct {
	Version int64          `json:"version"`
	Refs    []s3CatalogRef `json:"refs"`
}

// NewS3MetaStore creates a MetaStore backed by s3.
func NewS3MetaStore(s3 *storage.S3Client) *S3MetaStore {
	return &S3MetaStore{
		s3:                s3,
		headMaxRefCount:   s3HeadMaxRefCount,
		headMaxRefBytes:   s3HeadMaxRefBytes,
		checkpointMaxRefs: s3CheckpointMaxRefs,
	}
}

func s3ManifestKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d.json", s3ManifestPrefix, topic, partition)
}

func s3ArchiveKey(topic string, partition int, end int64) string {
	return fmt.Sprintf("%s%s/%d/%020d.json", s3ArchivePrefix, topic, partition, end)
}

// CommitUploadedBatches publishes already-uploaded batches through a single
// CAS-protected partition head: one read-modify-write publishes every batch in
// the invocation together, so a partial commit can never leak a gap. An upload
// which never reaches this method cannot consume an offset or affect the
// readable head. All batches must belong to the same partition.
func (m *S3MetaStore) CommitUploadedBatches(ctx context.Context, batches []UploadedBatch) ([]OffsetResult, error) {
	if err := samePartitionBatches(batches); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, nil
	}
	topic, partition := batches[0].Topic, batches[0].Partition
	results := make([]OffsetResult, len(batches))
	for {
		key := s3ManifestKey(topic, partition)
		data, etag, err := m.s3.GetWithETag(ctx, key)
		manifest := s3UploadManifest{Producers: map[string][]s3ProducerBatch{}}
		if err == nil {
			if err := json.Unmarshal(data, &manifest); err != nil {
				return nil, fmt.Errorf("parse upload manifest %s/%d: %w", topic, partition, err)
			}
		} else if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		changed := false
		for i, batch := range batches {
			if batch.BatchID == "" || batch.Count <= 0 {
				return nil, fmt.Errorf("uploaded batch %s has invalid count %d", batch.FileKey, batch.Count)
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
				if !duplicate {
					if len(h) > 0 {
						last := h[len(h)-1]
						if _, err := checkProducerSequence(batch.ProducerID, batch.Sequence, batch.Count, last.FirstSequence, last.Count); err != nil {
							return nil, err
						}
					} else if err := checkInitialProducerSequence(batch.ProducerID, batch.Sequence); err != nil {
						return nil, err
					}
				}
			}
			if duplicate {
				continue
			}
			base := manifest.NextOffset
			end := base + int64(batch.Count)
			manifest.NextOffset, manifest.CommittedOffset = end, end
			manifest.Refs = append(manifest.Refs, s3CatalogRef{FileKey: batch.FileKey, ByteOffset: batch.ByteOffset, ByteLength: batch.ByteLength, BaseOffset: base, EndOffset: end, CreatedAt: batch.CreatedAt})
			if batch.ProducerID != 0 {
				h := manifest.Producers[pid]
				h = append(h, s3ProducerBatch{FirstSequence: batch.Sequence, BaseOffset: base, Count: batch.Count})
				if len(h) > uploadedProducerHistory {
					h = h[len(h)-uploadedProducerHistory:]
				}
				manifest.Producers[pid] = h
			}
			results[i] = OffsetResult{BaseOffset: base}
			changed = true
		}
		if !changed {
			return results, nil // every batch was a duplicate
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
			return nil, fmt.Errorf("commit uploaded batches %s/%d: %w", topic, partition, err)
		}
		return results, nil
	}
}

// ArchiveCommitted rolls the oldest compaction-final refs out of the head
// window into a new immutable checkpoint once the window crosses a bound.
//
// A ref is compaction-final when it is at least targetBytes (so the data job
// will never merge it again) or when targetBytes <= 0 (compaction disabled: the
// head must still stay bounded). Retention-pending refs are left in the head so
// retention, not archiving, drops them. The roll stops at the first ineligible
// ref, so the window is compaction-paced: small refs wait in the head until the
// merge job compacts them, which is what guarantees archived refs are never
// touched by compaction again.
//
// Returns the number of refs archived.
func (m *S3MetaStore) ArchiveCommitted(ctx context.Context, topic string, partition int, targetBytes int64, retentionCutoff time.Time) (int, error) {
	for {
		data, etag, err := m.s3.GetWithETag(ctx, s3ManifestKey(topic, partition))
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("read head %s/%d: %w", topic, partition, err)
		}
		var head s3UploadManifest
		if err := json.Unmarshal(data, &head); err != nil {
			return 0, fmt.Errorf("parse head %s/%d: %w", topic, partition, err)
		}
		if len(head.Refs) == 0 {
			return 0, nil
		}
		var windowBytes int64
		for _, r := range head.Refs {
			windowBytes += r.ByteLength
		}
		if len(head.Refs) < m.headMaxRefCount && windowBytes < m.headMaxRefBytes {
			return 0, nil
		}

		// Front run of archivable refs.
		run := make([]s3CatalogRef, 0)
		for _, r := range head.Refs {
			if !r.CreatedAt.After(retentionCutoff) {
				break // retention-pending: leave for retention
			}
			if targetBytes > 0 && r.ByteLength < targetBytes {
				break // needs compaction first
			}
			run = append(run, r)
			if len(run) >= m.checkpointMaxRefs {
				break
			}
		}
		if len(run) == 0 {
			return 0, nil
		}

		runEnd := run[len(run)-1].EndOffset
		prevKey, prevEnd := "", int64(0)
		if head.Archive != nil {
			prevKey, prevEnd = head.Archive.Key, head.Archive.End
		}
		chk := s3Checkpoint{Version: 1, End: runEnd, PrevKey: prevKey, PrevEnd: prevEnd, Refs: run}
		encodedChk, err := json.Marshal(chk)
		if err != nil {
			return 0, err
		}
		chkKey := s3ArchiveKey(topic, partition, runEnd)
		// Checkpoints are immutable, so publish with create-if-not-exists: two
		// racing archive runs for the same range must never silently overwrite
		// each other. A conflict means the object already exists — either the
		// identical range from a concurrent run, or an orphan from a run whose
		// head CAS lost. Adopt it when it covers the same range; otherwise
		// retry against a fresh head.
		if _, err := m.s3.ConditionalPut(ctx, chkKey, encodedChk, ""); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				existing, gerr := m.readCheckpoint(ctx, chkKey)
				if gerr != nil {
					return 0, gerr
				}
				if existing == nil || existing.End != runEnd || existing.PrevKey != prevKey || existing.PrevEnd != prevEnd {
					continue
				}
			} else {
				return 0, fmt.Errorf("write checkpoint %s: %w", chkKey, err)
			}
		}

		head.Version++
		head.Archive = &s3ArchivePointer{Key: chkKey, End: runEnd}
		head.Refs = head.Refs[len(run):]
		encoded, err := json.Marshal(head)
		if err != nil {
			return 0, err
		}
		if _, err := m.s3.ConditionalPut(ctx, s3ManifestKey(topic, partition), encoded, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue
			}
			return 0, fmt.Errorf("archive head %s/%d: %w", topic, partition, err)
		}
		return len(run), nil
	}
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

// loadCheckpoints returns the head's archived checkpoints ordered oldest to
// newest, walking the PrevKey chain from the newest. A missing checkpoint
// (retention truncation) ends the walk: nothing older is reachable.
func (m *S3MetaStore) loadCheckpoints(ctx context.Context, head *s3UploadManifest) ([]*s3Checkpoint, error) {
	if head.Archive == nil {
		return nil, nil
	}
	var newestFirst []*s3Checkpoint
	key := head.Archive.Key
	for key != "" {
		data, err := m.s3.Get(ctx, key)
		if errors.Is(err, storage.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read checkpoint %s: %w", key, err)
		}
		var chk s3Checkpoint
		if err := json.Unmarshal(data, &chk); err != nil {
			return nil, fmt.Errorf("parse checkpoint %s: %w", key, err)
		}
		newestFirst = append(newestFirst, &chk)
		key = chk.PrevKey
	}
	oldest := make([]*s3Checkpoint, 0, len(newestFirst))
	for i := len(newestFirst) - 1; i >= 0; i-- {
		oldest = append(oldest, newestFirst[i])
	}
	return oldest, nil
}

// FileIndex is a single-pass snapshot of every segment reference across all
// partitions plus the set of live archived checkpoints. The S3 metastore's
// full-metadata checks (orphan sweep, checkpoint sweep, retention planning)
// each previously enumerated every partition's head and checkpoint chain; build
// the index once per maintenance pass and reuse it across those checks.
type FileIndex struct {
	// ByFile maps every referenced data file key to its refs.
	ByFile map[string][]FileRef
	// LiveCheckpoints contains every checkpoint key reachable from a head.
	LiveCheckpoints map[string]struct{}
	// PartitionLatest maps "topic#partition" to fileKey -> latest ref CreatedAt
	// in that partition (for retention expiry checks).
	PartitionLatest map[string]map[string]time.Time
	// FileLatest maps fileKey -> latest ref CreatedAt anywhere.
	FileLatest map[string]time.Time
}

// PartitionFileLatest returns the latest ref CreatedAt per file for one
// partition, or nil when the partition has no refs in the index.
func (idx *FileIndex) PartitionFileLatest(topic string, partition int) map[string]time.Time {
	return idx.PartitionLatest[partitionKey(topic, partition)]
}

// BuildFileIndex enumerates every partition head and its archived checkpoint
// chain once, returning a snapshot of all references. On the S3 metastore this
// is the shared source for the orphan sweep, the checkpoint sweep, and
// retention planning, so those checks never re-enumerate metadata per file.
func (m *S3MetaStore) BuildFileIndex(ctx context.Context) (*FileIndex, error) {
	idx := &FileIndex{
		ByFile:          map[string][]FileRef{},
		LiveCheckpoints: map[string]struct{}{},
		PartitionLatest: map[string]map[string]time.Time{},
		FileLatest:      map[string]time.Time{},
	}
	err := m.forEachPartitionHead(ctx, func(topic string, partition int, head *s3UploadManifest) error {
		pk := partitionKey(topic, partition)
		pl := idx.PartitionLatest[pk]
		if pl == nil {
			pl = make(map[string]time.Time)
			idx.PartitionLatest[pk] = pl
		}
		note := func(r s3CatalogRef) {
			idx.ByFile[r.FileKey] = append(idx.ByFile[r.FileKey], FileRef{Topic: topic, Partition: partition, Ref: SegmentRef(r)})
			if r.CreatedAt.After(pl[r.FileKey]) {
				pl[r.FileKey] = r.CreatedAt
			}
			if r.CreatedAt.After(idx.FileLatest[r.FileKey]) {
				idx.FileLatest[r.FileKey] = r.CreatedAt
			}
		}
		keys, err := m.loadCheckpointKeys(ctx, head)
		if err != nil {
			return err
		}
		for _, key := range keys {
			idx.LiveCheckpoints[key] = struct{}{}
			chk, err := m.readCheckpoint(ctx, key)
			if err != nil {
				return err
			}
			if chk == nil {
				continue
			}
			for _, r := range chk.Refs {
				note(r)
			}
		}
		for _, r := range head.Refs {
			note(r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// loadCheckpointKeys walks the head's archive chain and returns checkpoint keys
// oldest to newest, stopping at a missing checkpoint (retention truncation).
func (m *S3MetaStore) loadCheckpointKeys(ctx context.Context, head *s3UploadManifest) ([]string, error) {
	if head.Archive == nil {
		return nil, nil
	}
	var newestFirst []string
	key := head.Archive.Key
	for key != "" {
		newestFirst = append(newestFirst, key)
		chk, err := m.readCheckpoint(ctx, key)
		if err != nil {
			return nil, err
		}
		if chk == nil {
			break
		}
		key = chk.PrevKey
	}
	oldest := make([]string, 0, len(newestFirst))
	for i := len(newestFirst) - 1; i >= 0; i-- {
		oldest = append(oldest, newestFirst[i])
	}
	return oldest, nil
}

// readCheckpoint reads and parses a checkpoint object, returning nil when it
// does not exist.
func (m *S3MetaStore) readCheckpoint(ctx context.Context, key string) (*s3Checkpoint, error) {
	data, err := m.s3.Get(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint %s: %w", key, err)
	}
	var chk s3Checkpoint
	if err := json.Unmarshal(data, &chk); err != nil {
		return nil, fmt.Errorf("parse checkpoint %s: %w", key, err)
	}
	return &chk, nil
}

// ListOrphanedCheckpoints returns checkpoint keys under _diskless_meta/archive/
// that no partition head's archive chain reaches. Such objects are left behind
// when an archive run's checkpoint write succeeds but its head CAS loses (the
// refs remain in the head and the checkpoint is never linked), or when a
// partition was deleted without cleaning its archive.
func (m *S3MetaStore) ListOrphanedCheckpoints(ctx context.Context) ([]string, error) {
	referenced := make(map[string]bool)
	err := m.forEachPartitionHead(ctx, func(_ string, _ int, head *s3UploadManifest) error {
		keys, err := m.loadCheckpointKeys(ctx, head)
		if err != nil {
			return err
		}
		for _, key := range keys {
			referenced[key] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	keys, err := m.s3.List(ctx, s3ArchivePrefix)
	if err != nil {
		return nil, fmt.Errorf("list archive: %w", err)
	}
	var orphans []string
	for _, key := range keys {
		if !referenced[key] {
			orphans = append(orphans, key)
		}
	}
	return orphans, nil
}

func (m *S3MetaStore) QuerySegments(ctx context.Context, topic string, partition int,
	fromOffset int64, maxBytes int) ([]SegmentRef, error) {
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	if err != nil {
		return nil, err
	}

	var refs []SegmentRef
	var totalBytes int64
	appendRef := func(r s3CatalogRef) bool {
		if r.EndOffset <= fromOffset {
			return true
		}
		refs = append(refs, SegmentRef(r))
		totalBytes += r.ByteLength
		return totalBytes < int64(maxBytes)
	}

	if manifest.Archive != nil && fromOffset < manifest.Archive.End {
		checkpoints, err := m.loadCheckpoints(ctx, manifest)
		if err != nil {
			return nil, err
		}
		for _, chk := range checkpoints {
			if chk.End <= fromOffset {
				continue
			}
			for _, r := range chk.Refs {
				if !appendRef(r) {
					return refs, nil
				}
			}
		}
	}
	for _, r := range manifest.Refs {
		if !appendRef(r) {
			break
		}
	}
	return refs, nil
}

// GetPartitionHead returns the next offset that will be allocated for a partition.
func (m *S3MetaStore) GetPartitionHead(ctx context.Context, topic string, partition int) (int64, error) {
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	return manifest.NextOffset, nil
}

// GetCommittedHead returns the highest offset durably materialized for a
// partition, or 0 if nothing has been registered yet.
func (m *S3MetaStore) GetCommittedHead(ctx context.Context, topic string, partition int) (int64, error) {
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	return manifest.CommittedOffset, nil
}

// GetPartitionStart returns the earliest readable offset for a partition after
// retention cleanup has removed old segment references.
func (m *S3MetaStore) GetPartitionStart(ctx context.Context, topic string, partition int) (int64, error) {
	manifest, err := m.readUploadManifest(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	if len(manifest.Refs) > 0 {
		return manifest.Refs[0].BaseOffset, nil
	}
	if manifest.Archive != nil {
		checkpoints, err := m.loadCheckpoints(ctx, manifest)
		if err != nil {
			return 0, err
		}
		for _, chk := range checkpoints {
			if len(chk.Refs) > 0 {
				return chk.Refs[0].BaseOffset, nil
			}
		}
	}
	return m.GetCommittedHead(ctx, topic, partition)
}

// forEachPartitionHead enumerates every stored partition head.
func (m *S3MetaStore) forEachPartitionHead(ctx context.Context, fn func(topic string, partition int, head *s3UploadManifest) error) error {
	keys, err := m.s3.List(ctx, s3CatalogPrefix)
	if err != nil {
		return fmt.Errorf("list heads: %w", err)
	}
	for _, key := range keys {
		topic, partition, err := parseCatalogKey(key)
		if err != nil {
			return err
		}
		head, err := m.readUploadManifest(ctx, topic, partition)
		if err != nil {
			return err
		}
		if err := fn(topic, partition, head); err != nil {
			return err
		}
	}
	return nil
}

// partitionRefs returns every ref of a partition: archived checkpoints (oldest
// first) followed by the head window.
func (m *S3MetaStore) partitionRefs(ctx context.Context, head *s3UploadManifest) ([]s3CatalogRef, error) {
	checkpoints, err := m.loadCheckpoints(ctx, head)
	if err != nil {
		return nil, err
	}
	refs := make([]s3CatalogRef, 0, len(head.Refs))
	for _, chk := range checkpoints {
		refs = append(refs, chk.Refs...)
	}
	refs = append(refs, head.Refs...)
	return refs, nil
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

// readCatalog returns the partition head's ref window and its current etag.
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

// writeCatalog CAS-writes the head's ref window, returning storage.ErrConflict
// when a concurrent writer changed it first.
func (m *S3MetaStore) writeCatalog(ctx context.Context, topic string, partition int, cat *s3Catalog, etag string) error {
	// The ETag passed by readCatalog is for the head. Re-read is avoided so
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

// PlanExpiredFileDeletes returns file keys whose refs for the given
// topic-partition are expired and whose remaining refs, if any, are also
// expired.
func (m *S3MetaStore) PlanExpiredFileDeletes(ctx context.Context, topic string, partition int, cutoff time.Time) ([]string, error) {
	head, err := m.readUploadManifest(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	refs, err := m.partitionRefs(ctx, head)
	if err != nil {
		return nil, err
	}

	candidates := make(map[string]struct{})
	for _, r := range refs {
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
// partition: the head windows and any archived checkpoints.
func (m *S3MetaStore) DeleteFileRefs(ctx context.Context, fileKey string) error {
	keys, err := m.s3.List(ctx, s3CatalogPrefix)
	if err != nil {
		return fmt.Errorf("list heads: %w", err)
	}
	for _, key := range keys {
		topic, partition, err := parseCatalogKey(key)
		if err != nil {
			return err
		}
		if err := m.deleteFileRefsForPartition(ctx, topic, partition, fileKey); err != nil {
			return err
		}
	}
	return nil
}

// deleteFileRefsForPartition removes refs to fileKey from one partition's head
// window and its archived checkpoints in a single head CAS.
func (m *S3MetaStore) deleteFileRefsForPartition(ctx context.Context, topic string, partition int, fileKey string) error {
	for {
		data, etag, err := m.s3.GetWithETag(ctx, s3ManifestKey(topic, partition))
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read head %s/%d: %w", topic, partition, err)
		}
		var head s3UploadManifest
		if err := json.Unmarshal(data, &head); err != nil {
			return fmt.Errorf("parse head %s/%d: %w", topic, partition, err)
		}
		kept := make([]s3CatalogRef, 0, len(head.Refs))
		headChanged := false
		for _, r := range head.Refs {
			if r.FileKey == fileKey {
				headChanged = true
				continue
			}
			kept = append(kept, r)
		}
		newArchive, chkChanged, err := m.rewriteCheckpointsForFile(ctx, head.Archive, fileKey)
		if err != nil {
			return err
		}
		if !headChanged && !chkChanged {
			return nil
		}
		head.Version++
		head.Refs = kept
		head.Archive = newArchive
		encoded, err := json.Marshal(head)
		if err != nil {
			return err
		}
		if _, err := m.s3.ConditionalPut(ctx, s3ManifestKey(topic, partition), encoded, etag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue
			}
			return fmt.Errorf("delete file refs %s/%d: %w", topic, partition, err)
		}
		return nil
	}
}

// rewriteCheckpointsForFile rewrites the partition's archived checkpoint chain,
// dropping refs that point at fileKey and re-linking the chain across any
// checkpoint that becomes empty (deleted wholesale). Returns the head's new
// archive pointer, or nil when every checkpoint was removed.
func (m *S3MetaStore) rewriteCheckpointsForFile(ctx context.Context, archive *s3ArchivePointer, fileKey string) (*s3ArchivePointer, bool, error) {
	if archive == nil {
		return nil, false, nil
	}
	type entry struct {
		key     string
		chk     *s3Checkpoint
		kept    []s3CatalogRef
		changed bool
	}
	var chain []entry // newest first
	key := archive.Key
	for key != "" {
		data, err := m.s3.Get(ctx, key)
		if errors.Is(err, storage.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, false, fmt.Errorf("read checkpoint %s: %w", key, err)
		}
		var chk s3Checkpoint
		if err := json.Unmarshal(data, &chk); err != nil {
			return nil, false, fmt.Errorf("parse checkpoint %s: %w", key, err)
		}
		kept := make([]s3CatalogRef, 0, len(chk.Refs))
		changed := false
		for _, r := range chk.Refs {
			if r.FileKey == fileKey {
				changed = true
				continue
			}
			kept = append(kept, r)
		}
		chain = append(chain, entry{key: key, chk: &chk, kept: kept, changed: changed})
		key = chk.PrevKey
	}

	// Walk oldest -> newest, relinking PrevKey/PrevEnd across deletions.
	prevKey, prevEnd := "", int64(0)
	var newestSurvivor *s3ArchivePointer
	anyChanged := false
	for i := len(chain) - 1; i >= 0; i-- {
		e := &chain[i]
		if len(e.kept) == 0 {
			anyChanged = true
			continue
		}
		origPrevKey, origPrevEnd := e.chk.PrevKey, e.chk.PrevEnd
		e.chk.PrevKey, e.chk.PrevEnd = prevKey, prevEnd
		if e.chk.PrevKey != origPrevKey || e.chk.PrevEnd != origPrevEnd {
			e.changed = true
		}
		prevKey, prevEnd = e.key, e.chk.End
		newestSurvivor = &s3ArchivePointer{Key: e.key, End: e.chk.End}
	}

	for i := 0; i < len(chain); i++ {
		e := chain[i]
		if len(e.kept) == 0 {
			if err := m.s3.Delete(ctx, e.key); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, false, fmt.Errorf("delete checkpoint %s: %w", e.key, err)
			}
			continue
		}
		if !e.changed {
			continue
		}
		for {
			_, etag, err := m.s3.GetWithETag(ctx, e.key)
			if err != nil {
				return nil, false, fmt.Errorf("read checkpoint %s for rewrite: %w", e.key, err)
			}
			e.chk.Version++
			encoded, err := json.Marshal(e.chk)
			if err != nil {
				return nil, false, err
			}
			if _, err := m.s3.ConditionalPut(ctx, e.key, encoded, etag); err != nil {
				if errors.Is(err, storage.ErrConflict) {
					continue
				}
				return nil, false, fmt.Errorf("rewrite checkpoint %s: %w", e.key, err)
			}
			break
		}
		anyChanged = true
	}
	return newestSurvivor, anyChanged, nil
}

// parseCatalogKey extracts the topic and partition from a head key of the form
// _diskless_meta/manifest/{topic}/{partition}.json.
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
// partition head or checkpoint, so their data objects can be deleted after
// compaction.
func (m *S3MetaStore) PlanUnreferencedFileDeletes(ctx context.Context, fileKeys []string) ([]string, error) {
	referenced := make(map[string]bool, len(fileKeys))
	err := m.forEachPartitionHead(ctx, func(_ string, _ int, head *s3UploadManifest) error {
		refs, err := m.partitionRefs(ctx, head)
		if err != nil {
			return err
		}
		for _, r := range refs {
			referenced[r.FileKey] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
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
// points at fileKey, scanning the per-partition heads and their checkpoints.
func (m *S3MetaStore) ListFileRefs(ctx context.Context, fileKey string) ([]FileRef, error) {
	var refs []FileRef
	err := m.forEachPartitionHead(ctx, func(topic string, partition int, head *s3UploadManifest) error {
		pr, err := m.partitionRefs(ctx, head)
		if err != nil {
			return err
		}
		for _, r := range pr {
			if r.FileKey == fileKey {
				refs = append(refs, FileRef{Topic: topic, Partition: partition, Ref: SegmentRef(r)})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

// DeleteTopic removes all MetaStore state for a topic.
func (m *S3MetaStore) DeleteTopic(ctx context.Context, topic string) error {
	for _, prefix := range []string{
		s3CatalogPrefixForTopic(topic),
		s3ManifestPrefix + topic + "/",
		s3ArchivePrefix + topic + "/",
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
	fresh := false
	err := m.forEachPartitionHead(ctx, func(_ string, _ int, head *s3UploadManifest) error {
		refs, err := m.partitionRefs(ctx, head)
		if err != nil {
			return err
		}
		for _, r := range refs {
			if r.FileKey == fileKey && r.CreatedAt.After(cutoff) {
				fresh = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return fresh, nil
}

// ReplaceSegmentRefs atomically removes the refs identified by remove and
// inserts add into the partition head window via a read-modify-write CAS, so
// readers never observe a gap or a duplicate for the covered range. The added
// refs must exactly cover the union of the removed ranges (compaction of a
// contiguous run); the committed watermark is never modified. Retries are
// idempotent: an added ref already present is skipped, and already-removed refs
// are simply absent.
//
// The removed ranges must live in the head window: compaction never targets an
// archived ref (archived refs are compaction-final), so a removed range that is
// neither present nor already covered by a ref indicates an invariant violation.
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
		for _, rk := range remove {
			if catalogHasRange(cat.Refs, rk.BaseOffset, rk.EndOffset) {
				continue
			}
			return fmt.Errorf("replace refs %s/%d: range [%d,%d) is not in the head window", topic, partition, rk.BaseOffset, rk.EndOffset)
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

// catalogHasRange reports whether refs contain a ref whose range exactly covers
// [baseOffset, endOffset) (an idempotent retry re-checks ranges already
// replaced by a prior publish).
func catalogHasRange(refs []s3CatalogRef, baseOffset, endOffset int64) bool {
	for _, r := range refs {
		if r.BaseOffset <= baseOffset && r.EndOffset >= endOffset {
			return true
		}
	}
	return false
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
