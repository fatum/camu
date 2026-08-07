package diskless

import (
	"context"
	"fmt"
	"testing"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

func setupFlushForRead(t *testing.T) (*storage.S3Client, MetaStore) {
	t.Helper()
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch := makeTestBatch(t, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	})

	done := make(chan FlushResult, 1)
	if err := w.Flush(ctx, []BufferEntry{{
		Topic:     "t1",
		Partition: 0,
		Batch:     batch,
		Done:      done,
	}}); err != nil {
		t.Fatalf("setup flush: %v", err)
	}

	result := <-done
	if result.Err != nil {
		t.Fatalf("setup flush result: %v", result.Err)
	}

	return s3, meta
}

func TestReader_FetchFromStart(t *testing.T) {
	s3, meta := setupFlushForRead(t)
	r := NewReader(s3, meta)
	ctx := context.Background()

	data, hw, err := r.Fetch(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 3 {
		t.Fatalf("expected hw=3, got %d", hw)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}

	// Decode header to verify patched offset and record count.
	hdr, err := log.ReadRecordBatchHeader(data)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	if hdr.FirstOffset != 0 {
		t.Fatalf("expected FirstOffset=0, got %d", hdr.FirstOffset)
	}
	if hdr.NumRecords != 3 {
		t.Fatalf("expected NumRecords=3, got %d", hdr.NumRecords)
	}
}

func TestReader_FetchEmptyPartition(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	r := NewReader(s3, meta)
	ctx := context.Background()

	data, hw, err := r.Fetch(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %d bytes", len(data))
	}
	if hw != 0 {
		t.Fatalf("expected hw=0, got %d", hw)
	}
}

func TestReader_FetchPastEnd(t *testing.T) {
	s3, meta := setupFlushForRead(t)
	r := NewReader(s3, meta)
	ctx := context.Background()

	data, hw, err := r.Fetch(ctx, "t1", 0, 3, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %d bytes", len(data))
	}
	if hw != 3 {
		t.Fatalf("expected hw=3, got %d", hw)
	}
}

// TestReader_FetchCompactedRefPatchesAllBatches verifies that a ref covering
// several concatenated raw RecordBatches — the shape produced by diskless
// compaction — has every batch's base offset materialized on fetch, not just
// the first batch's. Regression for a bug where a merged ref read back with
// stale (0) bases on all but the first batch.
func TestReader_FetchCompactedRefPatchesAllBatches(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch := makeTestBatch(t, []log.Message{{Key: []byte("k"), Value: []byte("v")}})
	for i := 0; i < 3; i++ {
		done := make(chan FlushResult, 1)
		if err := w.Flush(ctx, []BufferEntry{{Topic: "t1", Partition: 0, Batch: batch, Done: done}}); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
		if result := <-done; result.Err != nil {
			t.Fatalf("flush %d result: %v", i, result.Err)
		}
	}

	refs, err := meta.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}

	// Concatenate the raw source batches into a merged object, then replace the
	// three source refs with one merged ref covering [0, 3), exactly what
	// compaction publishes.
	merged := make([]byte, 0)
	for _, ref := range refs {
		buf := make([]byte, ref.ByteLength)
		if err := s3.GetRangeInto(ctx, ref.FileKey, ref.ByteOffset, ref.ByteLength, buf); err != nil {
			t.Fatalf("get range: %v", err)
		}
		merged = append(merged, buf...)
	}
	if err := s3.Put(ctx, "_diskless_merge/t1/0/test.data", merged, storage.PutOpts{}); err != nil {
		t.Fatalf("put merged: %v", err)
	}
	remove := make([]RefKey, 0, len(refs))
	for _, ref := range refs {
		remove = append(remove, RefKey{BaseOffset: ref.BaseOffset, EndOffset: ref.EndOffset})
	}
	mergedRef := SegmentRef{
		FileKey:    "_diskless_merge/t1/0/test.data",
		ByteOffset: 0,
		ByteLength: int64(len(merged)),
		BaseOffset: 0,
		EndOffset:  3,
	}
	if err := meta.ReplaceSegmentRefs(ctx, "t1", 0, remove, []SegmentRef{mergedRef}); err != nil {
		t.Fatalf("replace refs: %v", err)
	}

	data, hw, err := NewReader(s3, meta).Fetch(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 3 {
		t.Fatalf("expected hw=3, got %d", hw)
	}

	pos, want := 0, int64(0)
	for pos < len(data) {
		hdr, err := log.ReadRecordBatchHeader(data[pos:])
		if err != nil {
			t.Fatalf("read header at %d: %v", pos, err)
		}
		if hdr.FirstOffset != want {
			t.Fatalf("batch at %d: expected FirstOffset=%d, got %d", pos, want, hdr.FirstOffset)
		}
		want += int64(hdr.NumRecords)
		pos += int(hdr.RecordBatchSize())
	}
	if want != 3 {
		t.Fatalf("expected 3 records, got %d", want)
	}
}

// TestReader_FetchTrimsOversizedMergedRefToWholeBatches verifies that a fetch
// with a byte budget smaller than a compacted merged ref returns only whole
// record batches within the budget — never a partial batch or an oversized
// response — while always including the first batch so the client makes
// progress. This is the regression test for the leaky ref-level cap: a 64MB
// merged object served to a Kafka consumer that requested 16MB used to be
// treated as a budget violation and silently paused the partition.
func TestReader_FetchTrimsOversizedMergedRefToWholeBatches(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()

	batch := makeTestBatch(t, []log.Message{{Key: []byte("k"), Value: []byte("v")}})
	for i := 0; i < 5; i++ {
		done := make(chan FlushResult, 1)
		if err := w.Flush(ctx, []BufferEntry{{Topic: "t1", Partition: 0, Batch: batch, Done: done}}); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
		if result := <-done; result.Err != nil {
			t.Fatalf("flush %d result: %v", i, result.Err)
		}
	}
	refs, err := meta.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	merged := make([]byte, 0)
	for _, ref := range refs {
		buf := make([]byte, ref.ByteLength)
		if err := s3.GetRangeInto(ctx, ref.FileKey, ref.ByteOffset, ref.ByteLength, buf); err != nil {
			t.Fatalf("get range: %v", err)
		}
		merged = append(merged, buf...)
	}
	if err := s3.Put(ctx, "_diskless_merge/t1/0/merged.data", merged, storage.PutOpts{}); err != nil {
		t.Fatalf("put merged: %v", err)
	}
	remove := make([]RefKey, 0, len(refs))
	for _, ref := range refs {
		remove = append(remove, RefKey{BaseOffset: ref.BaseOffset, EndOffset: ref.EndOffset})
	}
	if err := meta.ReplaceSegmentRefs(ctx, "t1", 0, remove, []SegmentRef{{
		FileKey: "_diskless_merge/t1/0/merged.data", ByteOffset: 0,
		ByteLength: int64(len(merged)), BaseOffset: 0, EndOffset: 5,
	}}); err != nil {
		t.Fatalf("replace refs: %v", err)
	}

	// Walk the merged object to get the per-batch sizes and the cumulative
	// boundary of whole batches for a given budget.
	batchSizes := batchSizesOf(t, merged)
	mergedSize := int64(len(merged))

	reader := NewReader(s3, meta)

	// Budget covers a fraction of the merged object: the response must be
	// whole batches within the budget.
	budget := int(mergedSize / 2)
	data, hw, err := reader.Fetch(ctx, "t1", 0, 0, budget)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 5 {
		t.Fatalf("hw = %d, want 5", hw)
	}
	if len(data) == 0 {
		t.Fatal("expected at least the first batch")
	}
	if len(data) > budget {
		t.Fatalf("response %d bytes exceeds budget %d", len(data), budget)
	}
	gotSizes := batchSizesOf(t, data)
	if len(gotSizes) == 0 || len(gotSizes) == len(batchSizes) {
		t.Fatalf("response covers %d of %d batches, want a strict trim", len(gotSizes), len(batchSizes))
	}
	// The batches returned must be the leading whole batches of the merged
	// object with their logical offsets patched.
	var wantOffset int64
	for _, size := range gotSizes {
		hdr, err := log.ReadRecordBatchHeader(data[0:size])
		if err != nil {
			t.Fatalf("header: %v", err)
		}
		if hdr.FirstOffset != wantOffset {
			t.Fatalf("batch offset = %d, want %d", hdr.FirstOffset, wantOffset)
		}
		wantOffset += int64(hdr.NumRecords)
		data = data[size:]
	}

	// A budget smaller than a single batch still returns the whole first batch
	// so the fetch always makes progress.
	tiny := batchSizes[0] - 1
	if tiny < 1 {
		tiny = 1
	}
	data, _, err = NewReader(s3, meta).Fetch(ctx, "t1", 0, 0, tiny)
	if err != nil {
		t.Fatalf("fetch tiny: %v", err)
	}
	if len(data) != batchSizes[0] {
		t.Fatalf("tiny fetch returned %d bytes, want the whole first batch (%d)", len(data), batchSizes[0])
	}
}

// batchSizesOf returns the wire size of every self-framing RecordBatch in data.
func batchSizesOf(t *testing.T, data []byte) []int {
	t.Helper()
	var sizes []int
	for pos := 0; pos < len(data); {
		hdr, err := log.ReadRecordBatchHeader(data[pos:])
		if err != nil {
			t.Fatalf("read header at %d: %v", pos, err)
		}
		sizes = append(sizes, int(hdr.RecordBatchSize()))
		pos += int(hdr.RecordBatchSize())
	}
	return sizes
}

// TestReader_HighWatermarkIsCommittedNotAllocated verifies that the readable
// high watermark reflects only durably materialized segments, never offsets
// that were allocated but not yet registered (in-flight flushes or gaps).

// makeMergedRef flushes batches times batches of recs records each and replaces
// them with a single merged ref covering [0, batches*recs), returning the
// merged object bytes and the ref. This is the compacted shape that previously
// forced whole-ref downloads and broke mid-ref reads.
func makeMergedRef(t *testing.T, s3 *storage.S3Client, meta MetaStore, batches, recs int) ([]byte, SegmentRef) {
	t.Helper()
	w := NewWriter(s3, meta, "node1")
	ctx := context.Background()
	msgs := make([]log.Message, recs)
	for j := range msgs {
		msgs[j] = log.Message{Key: []byte(fmt.Sprintf("k%d", j)), Value: []byte("v")}
	}
	batch := makeTestBatch(t, msgs)
	for i := 0; i < batches; i++ {
		done := make(chan FlushResult, 1)
		if err := w.Flush(ctx, []BufferEntry{{Topic: "t1", Partition: 0, Batch: batch, Done: done}}); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
		if result := <-done; result.Err != nil {
			t.Fatalf("flush %d result: %v", i, result.Err)
		}
	}
	refs, err := meta.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query segments: %v", err)
	}
	merged := make([]byte, 0)
	for _, ref := range refs {
		buf := make([]byte, ref.ByteLength)
		if err := s3.GetRangeInto(ctx, ref.FileKey, ref.ByteOffset, ref.ByteLength, buf); err != nil {
			t.Fatalf("get range: %v", err)
		}
		merged = append(merged, buf...)
	}
	if err := s3.Put(ctx, "_diskless_merge/t1/0/merged.data", merged, storage.PutOpts{}); err != nil {
		t.Fatalf("put merged: %v", err)
	}
	remove := make([]RefKey, 0, len(refs))
	for _, ref := range refs {
		remove = append(remove, RefKey{BaseOffset: ref.BaseOffset, EndOffset: ref.EndOffset})
	}
	mergedRef := SegmentRef{
		FileKey: "_diskless_merge/t1/0/merged.data", ByteOffset: 0,
		ByteLength: int64(len(merged)), BaseOffset: 0, EndOffset: int64(batches * recs),
	}
	if err := meta.ReplaceSegmentRefs(ctx, "t1", 0, remove, []SegmentRef{mergedRef}); err != nil {
		t.Fatalf("replace refs: %v", err)
	}
	return merged, mergedRef
}

// TestReader_FetchFromMidMergedRef verifies that a fetch whose start offset
// falls inside a compacted merged ref returns data beginning at the batch that
// reaches the offset, with every batch base patched. Previously the reader
// returned the ref's beginning, so mid-ref reads produced no messages and
// sequential consumers wedged at the first non-boundary page.
func TestReader_FetchFromMidMergedRef(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	merged, _ := makeMergedRef(t, s3, meta, 6, 10) // [0, 60), batches of 10
	reader := NewReader(s3, meta)
	ctx := context.Background()

	for _, from := range []int64{0, 5, 19, 20, 25, 50, 55, 59} {
		data, hw, err := reader.Fetch(ctx, "t1", 0, from, len(merged))
		if err != nil {
			t.Fatalf("from %d: %v", from, err)
		}
		if hw != 60 {
			t.Fatalf("from %d: hw = %d, want 60", from, hw)
		}
		if len(data) == 0 {
			t.Fatalf("from %d: empty fetch", from)
		}
		hdr, err := log.ReadRecordBatchHeader(data)
		if err != nil {
			t.Fatalf("from %d: header: %v", from, err)
		}
		wantBase := (from / 10) * 10
		if hdr.FirstOffset != wantBase {
			t.Fatalf("from %d: first batch base = %d, want %d", from, hdr.FirstOffset, wantBase)
		}
	}
}

// TestReader_FetchFromMidMergedRefBoundedBudget verifies that a fetch into a
// merged ref with a budget far smaller than the ref returns whole batches
// starting at fromOffset and never exceeds the budget (except the always-kept
// first batch). This is the regression guard for whole-ref downloads: a 64 MiB
// merged object must not be fetched in full to serve a 64 KiB page.
func TestReader_FetchFromMidMergedRefBoundedBudget(t *testing.T) {
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	merged, _ := makeMergedRef(t, s3, meta, 8, 25) // [0, 200), batches of 25
	reader := NewReader(s3, meta)
	ctx := context.Background()

	budget := len(merged) / 4
	from := int64(90) // inside batch [75, 100)
	data, hw, err := reader.Fetch(ctx, "t1", 0, from, budget)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 200 {
		t.Fatalf("hw = %d, want 200", hw)
	}
	if len(data) == 0 {
		t.Fatal("expected data")
	}
	if len(data) > budget+log.RecordBatchHeaderSize*2 {
		t.Fatalf("response %d bytes far exceeds budget %d", len(data), budget)
	}
	// Data must start at the batch containing from (base 75) and be whole
	// self-framing batches.
	pos := 0
	wantBase := int64(75)
	for pos < len(data) {
		hdr, err := log.ReadRecordBatchHeader(data[pos:])
		if err != nil {
			t.Fatalf("header at %d: %v", pos, err)
		}
		if hdr.FirstOffset != wantBase {
			t.Fatalf("batch at %d: first offset = %d, want %d", pos, hdr.FirstOffset, wantBase)
		}
		wantBase += int64(hdr.NumRecords)
		pos += int(hdr.RecordBatchSize())
	}
	if pos != len(data) {
		t.Fatalf("response not aligned to whole batches: %d != %d", pos, len(data))
	}
}
