package diskless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func newTestS3MetaStore(t *testing.T) *S3MetaStore {
	t.Helper()
	s3, err := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("create s3 client: %v", err)
	}
	return NewS3MetaStore(s3)
}

func commitS3Batch(t *testing.T, m *S3MetaStore, fileKey string, count int, byteLength int64, createdAt time.Time) {
	t.Helper()
	_, err := m.CommitUploadedBatches(context.Background(), []UploadedBatch{{
		BatchID: fmt.Sprintf("%s:0:%d", fileKey, byteLength), FileKey: fileKey, Topic: "t", Partition: 0,
		Count: count, ByteLength: byteLength, CreatedAt: createdAt,
	}})
	if err != nil {
		t.Fatalf("commit %s: %v", fileKey, err)
	}
}

func readS3Head(t *testing.T, m *S3MetaStore) *s3UploadManifest {
	t.Helper()
	data, err := m.s3.Get(context.Background(), s3ManifestKey("t", 0))
	if err != nil {
		t.Fatalf("get head: %v", err)
	}
	var head s3UploadManifest
	if err := json.Unmarshal(data, &head); err != nil {
		t.Fatalf("parse head: %v", err)
	}
	return &head
}

// TestS3MetaStore_ArchiveCommittedBoundsHeadAndReadsAcrossCheckpoints verifies
// the head window is bounded by archiving and that reads span checkpoints + the
// head without gaps or duplicates, while the committed watermark is untouched.
func TestS3MetaStore_ArchiveCommittedBoundsHeadAndReadsAcrossCheckpoints(t *testing.T) {
	m := newTestS3MetaStore(t)
	m.headMaxRefCount = 2
	m.headMaxRefBytes = 1 << 20
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 5; i++ {
		commitS3Batch(t, m, fmt.Sprintf("obj%d", i), 1, 100, now)
	}
	archived, err := m.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if archived != 5 {
		t.Fatalf("archived = %d, want 5", archived)
	}
	head := readS3Head(t, m)
	if len(head.Refs) != 0 || head.Archive == nil || head.Archive.End != 5 {
		t.Fatalf("head after archive = %+v (want empty window, archive to 5)", head)
	}
	committed, err := m.GetCommittedHead(ctx, "t", 0)
	if err != nil || committed != 5 {
		t.Fatalf("committed = %d, %v; want 5", committed, err)
	}
	start, err := m.GetPartitionStart(ctx, "t", 0)
	if err != nil || start != 0 {
		t.Fatalf("start = %d, %v; want 0", start, err)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 5 {
		t.Fatalf("refs = %d, want 5", len(refs))
	}
	for i, r := range refs {
		if r.BaseOffset != int64(i) || r.EndOffset != int64(i+1) {
			t.Fatalf("ref[%d] = %+v, want [%d,%d)", i, r, i, i+1)
		}
	}
	// A windowed query inside the archive region returns contiguous refs too.
	tail, err := m.QuerySegments(ctx, "t", 0, 2, 1024)
	if err != nil || len(tail) != 3 || tail[0].BaseOffset != 2 || tail[2].EndOffset != 5 {
		t.Fatalf("tail query = %+v, %v; want [2,3),[3,4),[4,5)", tail, err)
	}
}

// TestS3MetaStore_QuerySegmentsBoundsToMaxBytes verifies the ref-level byte
// budget: the first ref is always included even when it alone exceeds maxBytes
// (offset coverage — the reader trims the response to whole batches), and
// later refs are skipped once the total would exceed the budget.
func TestS3MetaStore_QuerySegmentsBoundsToMaxBytes(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	now := time.Now()
	// One oversized merged ref (64MB, the compaction target) followed by small
	// refs, as a compacted partition's manifest looks after the S3 compaction
	// change.
	commitS3Batch(t, m, "merged", 100, 64<<20, now)
	commitS3Batch(t, m, "small1", 1, 100, now)
	commitS3Batch(t, m, "small2", 1, 100, now)

	// A 16MB budget still includes the oversized first ref (the reader trims),
	// but stops there instead of piling on more refs past the budget.
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 16<<20)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 1 || refs[0].FileKey != "merged" {
		t.Fatalf("refs = %+v, want only the oversized merged ref", refs)
	}

	// A budget that fits the first two refs stops before the third.
	refs, err = m.QuerySegments(ctx, "t", 0, 0, 64<<20+100)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 2 || refs[1].FileKey != "small1" {
		t.Fatalf("refs = %+v, want merged + small1", refs)
	}
}

// TestS3MetaStore_ArchiveCommittedSkipsSmallAndRetentionPending verifies the
// roll leaves compaction-pending small refs and retention-pending refs in the
// head window.
func TestS3MetaStore_ArchiveCommittedSkipsSmallAndRetentionPending(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	m := newTestS3MetaStore(t)
	m.headMaxRefCount = 2
	commitS3Batch(t, m, "small", 1, 5, now)
	commitS3Batch(t, m, "big1", 1, 100, now)
	commitS3Batch(t, m, "big2", 1, 100, now)
	if n, err := m.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("archive with small front = %d, %v; want 0", n, err)
	}
	if head := readS3Head(t, m); len(head.Refs) != 3 || head.Archive != nil {
		t.Fatalf("small front must block archiving; head = %+v", head)
	}

	m2 := newTestS3MetaStore(t)
	m2.headMaxRefCount = 2
	old := now.Add(-48 * time.Hour)
	commitS3Batch(t, m2, "old1", 1, 100, old)
	commitS3Batch(t, m2, "old2", 1, 100, old)
	commitS3Batch(t, m2, "fresh", 1, 100, now)
	if n, err := m2.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-24*time.Hour)); err != nil || n != 0 {
		t.Fatalf("archive with retention-pending front = %d, %v; want 0", n, err)
	}
}

// TestS3MetaStore_DeleteFileRefsAcrossCheckpoints verifies retention drops refs
// that live in archived checkpoints and relinks/clears the archive chain without
// disturbing live refs.
func TestS3MetaStore_DeleteFileRefsAcrossCheckpoints(t *testing.T) {
	m := newTestS3MetaStore(t)
	m.headMaxRefCount = 2
	ctx := context.Background()
	now := time.Now()
	commitS3Batch(t, m, "a", 1, 100, now) // offset 0, archived
	commitS3Batch(t, m, "a", 1, 100, now) // offset 1, archived
	commitS3Batch(t, m, "b", 1, 5, now)   // offset 2, small -> stays in head
	if _, err := m.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-time.Hour)); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if head := readS3Head(t, m); head.Archive == nil || len(head.Refs) != 1 {
		t.Fatalf("setup: head = %+v", head)
	}
	if err := m.DeleteFileRefs(ctx, "a"); err != nil {
		t.Fatalf("delete file refs: %v", err)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 1 || refs[0].FileKey != "b" || refs[0].BaseOffset != 2 {
		t.Fatalf("refs after delete = %+v, want [b@2]", refs)
	}
	head := readS3Head(t, m)
	if head.Archive != nil || len(head.Refs) != 1 {
		t.Fatalf("head after delete = %+v (want cleared archive, one window ref)", head)
	}
}

// TestS3MetaStore_ListOrphanedCheckpoints verifies the reaper lists archive
// checkpoints no head chain references, while leaving linked ones alone.
func TestS3MetaStore_ListOrphanedCheckpoints(t *testing.T) {
	m := newTestS3MetaStore(t)
	m.headMaxRefCount = 2
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 3; i++ {
		commitS3Batch(t, m, fmt.Sprintf("obj%d", i), 1, 100, now)
	}
	if _, err := m.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-time.Hour)); err != nil {
		t.Fatalf("archive: %v", err)
	}
	// A stray checkpoint from a run whose head CAS lost.
	strayKey := s3ArchiveKey("t", 0, 99)
	if err := m.s3.Put(ctx, strayKey, []byte(`{"version":1,"end":99,"refs":[]}`), storage.PutOpts{}); err != nil {
		t.Fatalf("put stray checkpoint: %v", err)
	}
	orphans, err := m.ListOrphanedCheckpoints(ctx)
	if err != nil {
		t.Fatalf("list orphans: %v", err)
	}
	if len(orphans) != 1 || orphans[0] != strayKey {
		t.Fatalf("orphans = %v, want [%s]", orphans, strayKey)
	}
	// Deleting the stray leaves nothing orphaned; the linked checkpoint stays.
	if err := m.s3.Delete(ctx, strayKey); err != nil {
		t.Fatalf("delete stray: %v", err)
	}
	orphans, err = m.ListOrphanedCheckpoints(ctx)
	if err != nil {
		t.Fatalf("list orphans: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans after cleanup = %v, want none", orphans)
	}
	head := readS3Head(t, m)
	if head.Archive == nil {
		t.Fatal("linked checkpoint must survive the orphan listing")
	}
}

// TestS3MetaStore_ArchiveCommittedAdoptsExistingCheckpoint verifies that when a
// racing run already published the same checkpoint range (create-if-not-exists
// conflict) but its head CAS lost, the next run adopts the existing immutable
// checkpoint instead of failing or livelocking.
func TestS3MetaStore_ArchiveCommittedAdoptsExistingCheckpoint(t *testing.T) {
	m := newTestS3MetaStore(t)
	m.headMaxRefCount = 1
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 3; i++ {
		commitS3Batch(t, m, fmt.Sprintf("obj%d", i), 1, 100, now)
	}
	// Simulate the concurrent run: the same checkpoint range already exists.
	head := readS3Head(t, m)
	chk := s3Checkpoint{Version: 1, End: 3, Refs: head.Refs}
	chkData, err := json.Marshal(chk)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	chkKey := s3ArchiveKey("t", 0, 3)
	if err := m.s3.Put(ctx, chkKey, chkData, storage.PutOpts{}); err != nil {
		t.Fatalf("put existing checkpoint: %v", err)
	}
	n, err := m.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-time.Hour))
	if err != nil || n != 3 {
		t.Fatalf("archive = %d, %v; want 3, nil", n, err)
	}
	h := readS3Head(t, m)
	if h.Archive == nil || h.Archive.Key != chkKey || h.Archive.End != 3 || len(h.Refs) != 0 {
		t.Fatalf("head after adopt = %+v, want archive=%s end=3 empty window", h, chkKey)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1024)
	if err != nil || len(refs) != 3 {
		t.Fatalf("refs after adopt = %+v, %v; want 3", refs, err)
	}
}

// TestS3MetaStore_BuildFileIndex verifies the single-pass index captures every
// referenced file (archived + head window) and every live checkpoint key.
func TestS3MetaStore_BuildFileIndex(t *testing.T) {
	m := newTestS3MetaStore(t)
	m.headMaxRefCount = 2
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 3; i++ {
		commitS3Batch(t, m, fmt.Sprintf("obj%d", i), 1, 100, now)
	}
	if n, err := m.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-time.Hour)); err != nil || n != 3 {
		t.Fatalf("archive = %d, %v; want 3", n, err)
	}
	commitS3Batch(t, m, "obj3", 1, 100, now) // head window

	idx, err := m.BuildFileIndex(ctx)
	if err != nil {
		t.Fatalf("build index: %v", err)
	}
	for _, key := range []string{"obj0", "obj1", "obj2", "obj3"} {
		if len(idx.ByFile[key]) != 1 {
			t.Fatalf("ByFile[%s] = %d refs, want 1", key, len(idx.ByFile[key]))
		}
	}
	if len(idx.LiveCheckpoints) != 1 {
		t.Fatalf("LiveCheckpoints = %d, want 1 (the archived checkpoint)", len(idx.LiveCheckpoints))
	}
	pl := idx.PartitionFileLatest("t", 0)
	if len(pl) != 4 {
		t.Fatalf("PartitionFileLatest = %d files, want 4", len(pl))
	}
	for _, key := range []string{"obj0", "obj1", "obj2", "obj3"} {
		if idx.FileLatest[key].IsZero() {
			t.Fatalf("FileLatest[%s] missing", key)
		}
	}
}

// TestS3MetaStore_ReplaceSegmentRefsRejectsArchivedRange verifies compaction
// cannot silently rewrite a range that has been archived.
func TestS3MetaStore_ReplaceSegmentRefsRejectsArchivedRange(t *testing.T) {
	m := newTestS3MetaStore(t)
	m.headMaxRefCount = 1
	ctx := context.Background()
	now := time.Now()
	commitS3Batch(t, m, "a", 1, 100, now)
	commitS3Batch(t, m, "b", 1, 100, now)
	if _, err := m.ArchiveCommitted(ctx, "t", 0, 10, now.Add(-time.Hour)); err != nil {
		t.Fatalf("archive: %v", err)
	}
	err := m.ReplaceSegmentRefs(ctx, "t", 0, []RefKey{{BaseOffset: 0, EndOffset: 1}},
		[]SegmentRef{{FileKey: "m", ByteLength: 100, BaseOffset: 0, EndOffset: 1}})
	if err == nil {
		t.Fatal("expected error replacing an archived range")
	}
}

// TestS3MetaStore_CommitUploadedBatchIsReadableAndDeduplicated verifies a
// committed batch is readable and that an idempotent retry is deduplicated by
// the producer-sequence history (retroactive tombstone): the retry, even as a
// new physical upload, returns the original base without a second ref.
func TestS3MetaStore_CommitUploadedBatchIsReadableAndDeduplicated(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	first, err := m.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj1:0:10", FileKey: "obj1", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	// A client retry re-uploads the same logical batch (new BatchID/file). The
	// producer-sequence history deduplicates it: same base, no new ref.
	retry, err := m.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj2:0:10", FileKey: "obj2", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()}})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first[0].BaseOffset != 0 || !retry[0].Duplicate || retry[0].BaseOffset != 0 {
		t.Fatalf("unexpected outcomes: first=%+v retry=%+v", first, retry)
	}
	head, err := m.GetCommittedHead(ctx, "t", 0)
	if err != nil || head != 2 {
		t.Fatalf("committed head = %d, %v; want 2", head, err)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1024)
	if err != nil || len(refs) != 1 || refs[0].BaseOffset != 0 {
		t.Fatalf("refs = %+v, %v", refs, err)
	}
}

// TestS3MetaStore_CommitMultipleBatchesAtomically verifies a single invocation
// carrying several same-partition batches publishes them in one head CAS with
// contiguous offsets.
func TestS3MetaStore_CommitMultipleBatchesAtomically(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	results, err := m.CommitUploadedBatches(ctx, []UploadedBatch{
		{BatchID: "a:0:10", FileKey: "a", Topic: "t", Partition: 0, Count: 1, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()},
		{BatchID: "b:10:10", FileKey: "b", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, ProducerID: 7, Sequence: 1, CreatedAt: time.Now()},
		{BatchID: "c:20:10", FileKey: "c", Topic: "t", Partition: 0, Count: 1, ByteLength: 10, CreatedAt: time.Now()},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(results) != 3 || results[0].BaseOffset != 0 || results[1].BaseOffset != 1 || results[2].BaseOffset != 3 {
		t.Fatalf("unexpected results: %+v", results)
	}
	if head, err := m.GetCommittedHead(ctx, "t", 0); err != nil || head != 4 {
		t.Fatalf("committed head = %d, %v; want 4", head, err)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1024)
	if err != nil || len(refs) != 3 {
		t.Fatalf("refs = %+v, %v; want 3", refs, err)
	}
}

// TestS3MetaStore_HeadReadsReturnErrorOnGetFailure verifies GetCommittedHead and
// GetPartitionHead propagate a non-NotFound S3 read error instead of panicking on
// the nil manifest (regression: nil pointer dereference in GetCommittedHead
// crashed every node's diskless parquet export on transient S3 errors).
func TestS3MetaStore_HeadReadsReturnErrorOnGetFailure(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	injected := errors.New("injected get failure")
	m.s3.SetFaultInjector(func(op string) error {
		if op == "get" {
			return injected
		}
		return nil
	})
	defer m.s3.SetFaultInjector(nil)

	if _, err := m.GetCommittedHead(ctx, "t", 0); !errors.Is(err, injected) {
		t.Fatalf("GetCommittedHead err = %v, want injected error", err)
	}
	if _, err := m.GetPartitionHead(ctx, "t", 0); !errors.Is(err, injected) {
		t.Fatalf("GetPartitionHead err = %v, want injected error", err)
	}
}

// TestS3MetaStore_CommitRejectsCrossPartitionBatch verifies the one-partition
// atomicity boundary of a commit invocation.
func TestS3MetaStore_CommitRejectsCrossPartitionBatch(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	_, err := m.CommitUploadedBatches(ctx, []UploadedBatch{
		{BatchID: "a:0:10", FileKey: "a", Topic: "t", Partition: 0, Count: 1, ByteLength: 10},
		{BatchID: "b:0:10", FileKey: "b", Topic: "t", Partition: 1, Count: 1, ByteLength: 10},
	})
	if err == nil {
		t.Fatal("expected cross-partition batch to be rejected")
	}
}

// TestS3MetaStore_CommitNonIdempotentRetryAppends verifies non-idempotent
// batches are not deduplicated, matching Kafka semantics: a retried upload is a
// fresh append.
func TestS3MetaStore_CommitNonIdempotentRetryAppends(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	b := UploadedBatch{BatchID: "obj:0:10", FileKey: "obj", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, CreatedAt: time.Now()}
	first, err := m.CommitUploadedBatches(ctx, []UploadedBatch{b})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	retry, err := m.CommitUploadedBatches(ctx, []UploadedBatch{b})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first[0].BaseOffset != 0 || retry[0].Duplicate || retry[0].BaseOffset != 2 {
		t.Fatalf("unexpected outcomes: first=%+v retry=%+v", first, retry)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1024)
	if err != nil || len(refs) != 2 {
		t.Fatalf("refs = %+v, %v; want 2", refs, err)
	}
}
