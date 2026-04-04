package log

import (
	"os"
	"path/filepath"
	"testing"
)

func TestActiveSegment_AppendAndIndex(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	// Batch 1: baseOffset=0, 2 messages (offsets 0 and 1)
	batch1 := EncodeRecordBatch(0, []Message{
		{Offset: 0, Timestamp: 1000, Value: []byte("msg0")},
		{Offset: 1, Timestamp: 1001, Value: []byte("msg1")},
	})
	if err := seg.Append(batch1); err != nil {
		t.Fatalf("Append batch1: %v", err)
	}

	// Batch 2: baseOffset=2, 1 message (offset 2)
	batch2 := EncodeRecordBatch(2, []Message{
		{Offset: 2, Timestamp: 2000, Value: []byte("msg2")},
	})
	if err := seg.Append(batch2); err != nil {
		t.Fatalf("Append batch2: %v", err)
	}

	idx := seg.OffsetIndex()
	if len(idx) != 2 {
		t.Fatalf("expected 2 index entries, got %d", len(idx))
	}

	// Check batch1 index entry
	e0 := idx[0]
	if e0.BaseOffset != 0 {
		t.Errorf("e0.BaseOffset = %d, want 0", e0.BaseOffset)
	}
	if e0.LastOffset != 1 {
		t.Errorf("e0.LastOffset = %d, want 1", e0.LastOffset)
	}
	if e0.Position != 0 {
		t.Errorf("e0.Position = %d, want 0", e0.Position)
	}
	if e0.BatchSize != int32(len(batch1)) {
		t.Errorf("e0.BatchSize = %d, want %d", e0.BatchSize, len(batch1))
	}

	// Check batch2 index entry
	e1 := idx[1]
	if e1.BaseOffset != 2 {
		t.Errorf("e1.BaseOffset = %d, want 2", e1.BaseOffset)
	}
	if e1.LastOffset != 2 {
		t.Errorf("e1.LastOffset = %d, want 2", e1.LastOffset)
	}
	if e1.Position != int64(len(batch1)) {
		t.Errorf("e1.Position = %d, want %d", e1.Position, len(batch1))
	}
	if e1.BatchSize != int32(len(batch2)) {
		t.Errorf("e1.BatchSize = %d, want %d", e1.BatchSize, len(batch2))
	}
}

func TestActiveSegment_ReadAt(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	batch := EncodeRecordBatch(0, []Message{
		{Offset: 0, Timestamp: 500, Value: []byte("hello")},
	})
	if err := seg.Append(batch); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Read the batch back
	buf := make([]byte, len(batch))
	n, err := seg.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(batch) {
		t.Fatalf("ReadAt returned %d bytes, want %d", n, len(batch))
	}

	// Verify by parsing the header
	hdr, err := ReadRecordBatchHeader(buf)
	if err != nil {
		t.Fatalf("ReadRecordBatchHeader: %v", err)
	}
	if hdr.FirstOffset != 0 {
		t.Errorf("FirstOffset = %d, want 0", hdr.FirstOffset)
	}
}

func TestActiveSegment_NextOffset(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 5)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	// Empty segment: NextOffset == baseOffset
	if got := seg.NextOffset(); got != 5 {
		t.Errorf("NextOffset (empty) = %d, want 5", got)
	}

	// Append a batch with 3 records (offsets 5, 6, 7)
	batch := EncodeRecordBatch(5, []Message{
		{Offset: 5, Timestamp: 100},
		{Offset: 6, Timestamp: 200},
		{Offset: 7, Timestamp: 300},
	})
	if err := seg.Append(batch); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if got := seg.NextOffset(); got != 8 {
		t.Errorf("NextOffset after append = %d, want 8", got)
	}
}

func TestActiveSegment_FileNameOnDisk(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	seg.Close()

	expectedName := "00000000000000000000.log"
	expectedPath := filepath.Join(dir, expectedName)
	if _, err := os.Stat(expectedPath); err != nil {
		t.Errorf("expected file %s to exist: %v", expectedPath, err)
	}

	if got := SegmentFilename(0); got != expectedName {
		t.Errorf("SegmentFilename(0) = %q, want %q", got, expectedName)
	}
	if got := SidecarFilename(0); got != "00000000000000000000.index" {
		t.Errorf("SidecarFilename(0) = %q, want %q", got, "00000000000000000000.index")
	}
}

func TestActiveSegment_TimestampIndexMonotonic(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	// Batch 1: MaxTimestamp=1000 → should be indexed
	batch1 := EncodeRecordBatch(0, []Message{
		{Offset: 0, Timestamp: 1000, Value: []byte("a")},
	})
	if err := seg.Append(batch1); err != nil {
		t.Fatalf("Append batch1: %v", err)
	}

	// Batch 2: MaxTimestamp=500 (lower than 1000) → should NOT be indexed
	batch2 := EncodeRecordBatch(1, []Message{
		{Offset: 1, Timestamp: 500, Value: []byte("b")},
	})
	if err := seg.Append(batch2); err != nil {
		t.Fatalf("Append batch2: %v", err)
	}

	// Batch 3: MaxTimestamp=2000 (higher than 1000) → should be indexed
	batch3 := EncodeRecordBatch(2, []Message{
		{Offset: 2, Timestamp: 2000, Value: []byte("c")},
	})
	if err := seg.Append(batch3); err != nil {
		t.Fatalf("Append batch3: %v", err)
	}

	tsIdx := seg.TimestampIndex()
	if len(tsIdx) != 2 {
		t.Fatalf("expected 2 timestamp index entries, got %d: %v", len(tsIdx), tsIdx)
	}

	if tsIdx[0].Timestamp != 1000 || tsIdx[0].BaseOffset != 0 {
		t.Errorf("tsIdx[0] = %+v, want {Timestamp:1000, BaseOffset:0}", tsIdx[0])
	}
	if tsIdx[1].Timestamp != 2000 || tsIdx[1].BaseOffset != 2 {
		t.Errorf("tsIdx[1] = %+v, want {Timestamp:2000, BaseOffset:2}", tsIdx[1])
	}
}

func TestActiveSegment_SizeAndPaths(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 100)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	if seg.Size() != 0 {
		t.Errorf("initial Size = %d, want 0", seg.Size())
	}
	if seg.BaseOffset() != 100 {
		t.Errorf("BaseOffset = %d, want 100", seg.BaseOffset())
	}
	if seg.Dir() != dir {
		t.Errorf("Dir = %q, want %q", seg.Dir(), dir)
	}

	expectedPath := filepath.Join(dir, "00000000000000000100.log")
	if seg.Path() != expectedPath {
		t.Errorf("Path = %q, want %q", seg.Path(), expectedPath)
	}
	expectedSidecar := filepath.Join(dir, "00000000000000000100.index")
	if seg.SidecarPath() != expectedSidecar {
		t.Errorf("SidecarPath = %q, want %q", seg.SidecarPath(), expectedSidecar)
	}

	batch := EncodeRecordBatch(100, []Message{
		{Offset: 100, Timestamp: 1, Value: []byte("x")},
	})
	if err := seg.Append(batch); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seg.Size() != int64(len(batch)) {
		t.Errorf("Size after append = %d, want %d", seg.Size(), len(batch))
	}
}

func TestActiveSegment_IndexEntryCopy(t *testing.T) {
	// Verify that OffsetIndex / TimestampIndex return copies, not live slices.
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	batch := EncodeRecordBatch(0, []Message{{Offset: 0, Timestamp: 1000, Value: []byte("z")}})
	if err := seg.Append(batch); err != nil {
		t.Fatalf("Append: %v", err)
	}

	idx := seg.OffsetIndex()
	idx[0].BaseOffset = 9999 // mutate the copy

	idx2 := seg.OffsetIndex()
	if idx2[0].BaseOffset == 9999 {
		t.Error("OffsetIndex returned a live slice, not a copy")
	}
}

func TestActiveSegmentSeal(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}

	batch1 := EncodeRecordBatch(0, []Message{
		{Offset: 0, Timestamp: 1000, Value: []byte("a")},
		{Offset: 1, Timestamp: 2000, Value: []byte("b")},
	})
	if err := seg.Append(batch1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	segPath, sidecarPath, err := seg.Seal()
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Segment file must exist.
	if _, err := os.Stat(segPath); err != nil {
		t.Errorf("segment file does not exist at %s: %v", segPath, err)
	}

	// Sidecar file must exist.
	if _, err := os.Stat(sidecarPath); err != nil {
		t.Errorf("sidecar file does not exist at %s: %v", sidecarPath, err)
	}

	// Sidecar must be parseable with correct entry count and BaseOffset.
	data, err := os.ReadFile(sidecarPath)
	if err != nil {
		t.Fatalf("ReadFile sidecar: %v", err)
	}
	entries, _, err := ReadSidecar(data)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("ReadSidecar: expected 1 index entry, got %d", len(entries))
	} else if entries[0].BaseOffset != 0 {
		t.Errorf("ReadSidecar: entries[0].BaseOffset = %d, want 0", entries[0].BaseOffset)
	}
}

func TestActiveSegmentRecover(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}

	batch1 := EncodeRecordBatch(0, []Message{
		{Offset: 0, Timestamp: 1000, Value: []byte("hello")},
	})
	batch2 := EncodeRecordBatch(1, []Message{
		{Offset: 1, Timestamp: 2000, Value: []byte("world")},
	})
	if err := seg.Append(batch1); err != nil {
		t.Fatalf("Append batch1: %v", err)
	}
	if err := seg.Append(batch2); err != nil {
		t.Fatalf("Append batch2: %v", err)
	}

	validSize := int64(len(batch1) + len(batch2))

	// Write garbage bytes after the two valid batches.
	garbage := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xFF}
	if _, err := seg.file.Write(garbage); err != nil {
		t.Fatalf("writing garbage: %v", err)
	}
	seg.size += int64(len(garbage))

	// Close and reopen without indices.
	if err := seg.file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seg2, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment (reopen): %v", err)
	}
	defer seg2.Close()

	if err := seg2.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// offsetIdx should have exactly 2 entries.
	idx := seg2.OffsetIndex()
	if len(idx) != 2 {
		t.Errorf("after Recover: expected 2 index entries, got %d", len(idx))
	}

	// NextOffset should be 2 (last valid offset was 1).
	if got := seg2.NextOffset(); got != 2 {
		t.Errorf("NextOffset = %d, want 2", got)
	}

	// File size must equal the sum of the two valid batches (garbage truncated).
	if seg2.Size() != validSize {
		t.Errorf("Size after Recover = %d, want %d", seg2.Size(), validSize)
	}

	// Confirm on-disk size matches.
	info, err := os.Stat(seg2.Path())
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != validSize {
		t.Errorf("on-disk size = %d, want %d", info.Size(), validSize)
	}
}

func TestActiveSegmentLookupOffset(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	// Batch 0: offsets [0, 1]
	if err := seg.Append(EncodeRecordBatch(0, []Message{
		{Offset: 0, Timestamp: 100, Value: []byte("a")},
		{Offset: 1, Timestamp: 101, Value: []byte("b")},
	})); err != nil {
		t.Fatalf("Append batch0: %v", err)
	}
	// Batch 1: offsets [2, 3]
	if err := seg.Append(EncodeRecordBatch(2, []Message{
		{Offset: 2, Timestamp: 200, Value: []byte("c")},
		{Offset: 3, Timestamp: 201, Value: []byte("d")},
	})); err != nil {
		t.Fatalf("Append batch1: %v", err)
	}
	// Batch 2: offsets [4, 5]
	if err := seg.Append(EncodeRecordBatch(4, []Message{
		{Offset: 4, Timestamp: 300, Value: []byte("e")},
		{Offset: 5, Timestamp: 301, Value: []byte("f")},
	})); err != nil {
		t.Fatalf("Append batch2: %v", err)
	}

	tests := []struct {
		offset     int64
		wantFound  bool
		wantBase   int64
	}{
		{0, true, 0},
		{1, true, 0},
		{2, true, 2},
		{3, true, 2},
		{4, true, 4},
		{5, true, 4},
		{6, false, 0},
	}

	for _, tc := range tests {
		entry, found := seg.LookupOffset(tc.offset)
		if found != tc.wantFound {
			t.Errorf("LookupOffset(%d): found=%v, want %v", tc.offset, found, tc.wantFound)
			continue
		}
		if found && entry.BaseOffset != tc.wantBase {
			t.Errorf("LookupOffset(%d): BaseOffset=%d, want %d", tc.offset, entry.BaseOffset, tc.wantBase)
		}
	}
}

func TestActiveSegmentLookupTimestamp(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	// Batch 0: timestamps [100, 200], offsets [0, 1]
	if err := seg.Append(EncodeRecordBatch(0, []Message{
		{Offset: 0, Timestamp: 100, Value: []byte("a")},
		{Offset: 1, Timestamp: 200, Value: []byte("b")},
	})); err != nil {
		t.Fatalf("Append batch0: %v", err)
	}
	// Batch 1: timestamps [300, 400], offsets [2, 3]
	if err := seg.Append(EncodeRecordBatch(2, []Message{
		{Offset: 2, Timestamp: 300, Value: []byte("c")},
		{Offset: 3, Timestamp: 400, Value: []byte("d")},
	})); err != nil {
		t.Fatalf("Append batch1: %v", err)
	}

	tests := []struct {
		ts        int64
		wantFound bool
		wantBase  int64
	}{
		{50, true, 0},   // before all timestamps → first batch
		{100, true, 0},  // exact match of first batch MinTimestamp
		{200, true, 0},  // exact match of first batch MaxTimestamp
		{201, true, 2},  // just past first batch → second batch
		{400, true, 2},  // exact match of second batch MaxTimestamp
		{500, false, 0}, // beyond all timestamps
	}

	for _, tc := range tests {
		entry, found := seg.LookupTimestamp(tc.ts)
		if found != tc.wantFound {
			t.Errorf("LookupTimestamp(%d): found=%v, want %v", tc.ts, found, tc.wantFound)
			continue
		}
		if found && entry.BaseOffset != tc.wantBase {
			t.Errorf("LookupTimestamp(%d): BaseOffset=%d, want %d", tc.ts, entry.BaseOffset, tc.wantBase)
		}
	}
}

func TestActiveSegmentRecoverEmpty(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenActiveSegment(dir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment: %v", err)
	}
	defer seg.Close()

	// Recover on an empty file — should succeed with 0 entries.
	if err := seg.Recover(); err != nil {
		t.Fatalf("Recover on empty file: %v", err)
	}

	if got := seg.NextOffset(); got != 0 {
		t.Errorf("NextOffset = %d, want 0", got)
	}
	if seg.Size() != 0 {
		t.Errorf("Size = %d, want 0", seg.Size())
	}
	if len(seg.OffsetIndex()) != 0 {
		t.Errorf("OffsetIndex non-empty after recover on empty file")
	}
}
