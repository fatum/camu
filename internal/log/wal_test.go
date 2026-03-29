package log

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Open, append 2 messages, close
	w, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: 2000, Key: []byte("k1"), Value: []byte("v1")},
	}
	for _, m := range msgs {
		if err := w.Append(m); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and replay
	w2, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL (reopen): %v", err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(replayed))
	}
	for i, want := range msgs {
		got := replayed[i]
		if got.Offset != want.Offset {
			t.Errorf("msg[%d] offset: got %d, want %d", i, got.Offset, want.Offset)
		}
		if got.Timestamp != want.Timestamp {
			t.Errorf("msg[%d] timestamp: got %d, want %d", i, got.Timestamp, want.Timestamp)
		}
		if string(got.Key) != string(want.Key) {
			t.Errorf("msg[%d] key: got %q, want %q", i, got.Key, want.Key)
		}
		if string(got.Value) != string(want.Value) {
			t.Errorf("msg[%d] value: got %q, want %q", i, got.Value, want.Value)
		}
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := OpenWAL(path, false, 64)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	for i := 0; i < 5; i++ {
		m := Message{Offset: uint64(i), Timestamp: int64(i * 100), Value: []byte("data")}
		if err := w.Append(m); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Truncate before offset 3 — keep offsets 3, 4
	if err := w.TruncateBefore(3); err != nil {
		t.Fatalf("TruncateBefore: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and replay
	w2, err := OpenWAL(path, false, 64)
	if err != nil {
		t.Fatalf("OpenWAL (reopen): %v", err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected 2 messages after truncation, got %d", len(replayed))
	}
	if replayed[0].Offset != 3 {
		t.Errorf("expected first offset 3, got %d", replayed[0].Offset)
	}
	if replayed[1].Offset != 4 {
		t.Errorf("expected second offset 4, got %d", replayed[1].Offset)
	}
}

func TestWAL_EmptyReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.wal")

	w, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	replayed, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay on empty WAL: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(replayed))
	}
}

func TestWAL_CorruptEntrySkipped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.wal")

	// Write one good message
	w, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.Append(Message{Offset: 0, Value: []byte("good")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the file by appending garbage (simulates crash mid-write)
	chunkPath := walChunkPath(walChunkDir(path), 1)
	f, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0xDE, 0xAD}) // partial entry
	f.Close()

	// Replay should return only the valid message
	w2, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL (reopen): %v", err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 valid message, got %d", len(replayed))
	}
	if string(replayed[0].Value) != "good" {
		t.Errorf("unexpected value: %q", replayed[0].Value)
	}
}

func TestWAL_AppendBatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch.wal")

	w, err := OpenWAL(path, true, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}

	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Timestamp: 2000, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Timestamp: 3000, Key: []byte("k2"), Value: []byte("v2")},
	}

	if err := w.AppendBatch(msgs); err != nil {
		t.Fatalf("AppendBatch() error: %v", err)
	}

	w.Close()

	// Verify replay after reopen
	w2, err := OpenWAL(path, true, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error: %v", err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("Replay() = %d messages, want 3", len(replayed))
	}
	if string(replayed[2].Key) != "k2" {
		t.Errorf("replayed[2].Key = %q, want %q", string(replayed[2].Key), "k2")
	}
}

func TestWAL_AppendBatchEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty-batch.wal")

	w, err := OpenWAL(path, true, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}
	defer w.Close()

	if err := w.AppendBatch(nil); err != nil {
		t.Fatalf("AppendBatch(nil) error: %v", err)
	}
	if err := w.AppendBatch([]Message{}); err != nil {
		t.Fatalf("AppendBatch([]) error: %v", err)
	}
}

func TestWAL_CachesChunkListInMemory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "chunked.wal")

	w, err := OpenWAL(path, false, 64)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}
	defer w.Close()

	// Append individually so each message gets its own envelope and can
	// trigger chunk rotation at the small 64-byte chunk size.
	for i := 0; i < 3; i++ {
		if err := w.Append(Message{Offset: uint64(i), Value: []byte("aaaaaaaaaaaaaaaa")}); err != nil {
			t.Fatalf("Append(%d) error: %v", i, err)
		}
	}

	if got := len(w.chunks); got < 2 {
		t.Fatalf("len(w.chunks) = %d, want at least 2", got)
	}

	if err := w.TruncateBefore(2); err != nil {
		t.Fatalf("TruncateBefore() error: %v", err)
	}
	if got := len(w.chunks); got != 1 {
		t.Fatalf("len(w.chunks) after truncate = %d, want 1", got)
	}
	if got := w.chunks[0].baseOffset; got != 2 {
		t.Fatalf("w.chunks[0].baseOffset = %d, want 2", got)
	}
}

func TestWAL_FlushedChunksRetainedForReadsButSkippedByReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "retained.wal")

	w, err := OpenWAL(path, false, 64)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}
	defer w.Close()

	if err := w.AppendBatch([]Message{
		{Offset: 0, Value: []byte("v0")},
		{Offset: 1, Value: []byte("v1")},
	}); err != nil {
		t.Fatalf("AppendBatch() first error: %v", err)
	}
	if err := w.Seal(); err != nil {
		t.Fatalf("Seal() error: %v", err)
	}
	if err := w.MarkFlushed(2); err != nil {
		t.Fatalf("MarkFlushed() error: %v", err)
	}
	if err := w.AppendBatch([]Message{
		{Offset: 2, Value: []byte("v2")},
		{Offset: 3, Value: []byte("v3")},
	}); err != nil {
		t.Fatalf("AppendBatch() second error: %v", err)
	}

	replayed, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("Replay() returned %d messages, want 2", len(replayed))
	}
	if replayed[0].Offset != 2 || replayed[1].Offset != 3 {
		t.Fatalf("Replay() offsets = [%d %d], want [2 3]", replayed[0].Offset, replayed[1].Offset)
	}

	readMsgs, err := w.ReadFrom(0, 10)
	if err != nil {
		t.Fatalf("ReadFrom() error: %v", err)
	}
	if len(readMsgs) != 4 {
		t.Fatalf("ReadFrom() returned %d messages, want 4", len(readMsgs))
	}
	for i := range readMsgs {
		if got := readMsgs[i].Offset; got != uint64(i) {
			t.Fatalf("ReadFrom()[%d].Offset = %d, want %d", i, got, i)
		}
	}
}

func TestWAL_BatchEnvelopeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "envelope.wal")

	w, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}

	batch := Batch{
		ProducerID: 42,
		Sequence:   7,
		Messages: []Message{
			{Offset: 10, Timestamp: 1000, Key: []byte("k0"), Value: []byte("v0")},
			{Offset: 11, Timestamp: 2000, Key: []byte("k1"), Value: []byte("v1")},
		},
	}
	if err := w.AppendBatchWithMeta(batch); err != nil {
		t.Fatalf("AppendBatchWithMeta() error: %v", err)
	}
	w.Close()

	w2, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error: %v", err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("Replay() = %d messages, want 2", len(replayed))
	}
	if replayed[0].Offset != 10 || replayed[1].Offset != 11 {
		t.Fatalf("offsets = [%d %d], want [10 11]", replayed[0].Offset, replayed[1].Offset)
	}
	if string(replayed[0].Key) != "k0" {
		t.Errorf("replayed[0].Key = %q, want %q", replayed[0].Key, "k0")
	}
	if string(replayed[1].Value) != "v1" {
		t.Errorf("replayed[1].Value = %q, want %q", replayed[1].Value, "v1")
	}
}

func TestWAL_AppendBatchBackwardsCompat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "compat.wal")

	w, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}

	msgs := []Message{
		{Offset: 0, Timestamp: 100, Key: []byte("a"), Value: []byte("b")},
		{Offset: 1, Timestamp: 200, Key: []byte("c"), Value: []byte("d")},
	}
	if err := w.AppendBatch(msgs); err != nil {
		t.Fatalf("AppendBatch() error: %v", err)
	}
	w.Close()

	w2, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error: %v", err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("Replay() = %d messages, want 2", len(replayed))
	}
	for i, want := range msgs {
		got := replayed[i]
		if got.Offset != want.Offset || got.Timestamp != want.Timestamp {
			t.Errorf("msg[%d] offset/ts mismatch: got (%d,%d), want (%d,%d)",
				i, got.Offset, got.Timestamp, want.Offset, want.Timestamp)
		}
		if string(got.Key) != string(want.Key) || string(got.Value) != string(want.Value) {
			t.Errorf("msg[%d] key/value mismatch", i)
		}
	}
}

func TestWAL_ScanBatchesReturnsBatchMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scan-batches.wal")

	w, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}

	b1 := Batch{
		ProducerID: 1,
		Sequence:   10,
		Messages: []Message{
			{Offset: 0, Value: []byte("a")},
			{Offset: 1, Value: []byte("b")},
		},
	}
	b2 := Batch{
		ProducerID: 2,
		Sequence:   20,
		Messages: []Message{
			{Offset: 2, Value: []byte("c")},
		},
	}
	if err := w.AppendBatchWithMeta(b1); err != nil {
		t.Fatalf("AppendBatchWithMeta(b1) error: %v", err)
	}
	if err := w.AppendBatchWithMeta(b2); err != nil {
		t.Fatalf("AppendBatchWithMeta(b2) error: %v", err)
	}
	w.Close()

	// Read the active chunk directly with scanWALBatches.
	chunkPath := walActiveChunkPath(walChunkDir(path))
	// After close+reopen the active chunk gets sealed; find whatever chunk exists.
	w2, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error: %v", err)
	}
	defer w2.Close()

	w2.mu.RLock()
	chunks := w2.snapshotChunksLocked(true, true)
	w2.mu.RUnlock()

	var batches []Batch
	for _, chunk := range chunks {
		p := w2.chunkPathLocked(chunk)
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("open chunk %q: %v", chunkPath, err)
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			t.Fatalf("stat chunk: %v", err)
		}
		_, _, _, err = scanWALBatches(f, info.Size(), 0, func(b Batch) bool {
			batches = append(batches, b)
			return true
		})
		f.Close()
		if err != nil {
			t.Fatalf("scanWALBatches() error: %v", err)
		}
	}

	if len(batches) != 2 {
		t.Fatalf("scanWALBatches() = %d batches, want 2", len(batches))
	}
	if batches[0].ProducerID != 1 || batches[0].Sequence != 10 {
		t.Errorf("batch[0] = (pid=%d, seq=%d), want (1, 10)", batches[0].ProducerID, batches[0].Sequence)
	}
	if len(batches[0].Messages) != 2 {
		t.Errorf("batch[0] has %d messages, want 2", len(batches[0].Messages))
	}
	if batches[1].ProducerID != 2 || batches[1].Sequence != 20 {
		t.Errorf("batch[1] = (pid=%d, seq=%d), want (2, 20)", batches[1].ProducerID, batches[1].Sequence)
	}
	if len(batches[1].Messages) != 1 {
		t.Errorf("batch[1] has %d messages, want 1", len(batches[1].Messages))
	}
}

func TestWAL_ReadBatchMetasFrom(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch-metas.wal")

	w, err := OpenWAL(path, false, 1<<20)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}
	defer w.Close()

	if err := w.AppendBatchWithMeta(Batch{
		ProducerID: 7,
		Sequence:   11,
		Messages: []Message{
			{Offset: 0, Key: []byte("k0"), Value: []byte("v0"), Headers: map[string]string{"a": "1"}},
			{Offset: 1, Key: []byte("k1"), Value: []byte("v1"), Headers: map[string]string{"b": "2"}},
		},
	}); err != nil {
		t.Fatalf("AppendBatchWithMeta(first) error: %v", err)
	}
	if err := w.AppendBatchWithMeta(Batch{
		ProducerID: 9,
		Sequence:   20,
		Messages: []Message{
			{Offset: 2, Key: []byte("k2"), Value: []byte("v2"), Headers: map[string]string{"c": "3"}},
		},
	}); err != nil {
		t.Fatalf("AppendBatchWithMeta(second) error: %v", err)
	}

	metas, err := w.ReadBatchMetasFrom(1)
	if err != nil {
		t.Fatalf("ReadBatchMetasFrom() error: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("ReadBatchMetasFrom() returned %d metas, want 2", len(metas))
	}

	if metas[0].ProducerID != 7 || metas[0].Sequence != 11 {
		t.Fatalf("meta[0] = (pid=%d, seq=%d), want (7, 11)", metas[0].ProducerID, metas[0].Sequence)
	}
	if metas[0].MessageCount != 2 || metas[0].FirstOffset != 0 || metas[0].LastOffset != 1 {
		t.Fatalf("meta[0] = %+v, want count=2 first=0 last=1", metas[0])
	}

	if metas[1].ProducerID != 9 || metas[1].Sequence != 20 {
		t.Fatalf("meta[1] = (pid=%d, seq=%d), want (9, 20)", metas[1].ProducerID, metas[1].Sequence)
	}
	if metas[1].MessageCount != 1 || metas[1].FirstOffset != 2 || metas[1].LastOffset != 2 {
		t.Fatalf("meta[1] = %+v, want count=1 first=2 last=2", metas[1])
	}
}

func TestWAL_TruncateRemovesRetainedFlushedChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "truncate-retained.wal")

	w, err := OpenWAL(path, false, 64)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}
	defer w.Close()

	if err := w.AppendBatch([]Message{
		{Offset: 0, Value: []byte("v0")},
		{Offset: 1, Value: []byte("v1")},
		{Offset: 2, Value: []byte("v2")},
	}); err != nil {
		t.Fatalf("AppendBatch() error: %v", err)
	}
	if err := w.Seal(); err != nil {
		t.Fatalf("Seal() error: %v", err)
	}
	if err := w.MarkFlushed(3); err != nil {
		t.Fatalf("MarkFlushed() error: %v", err)
	}
	if err := w.TruncateBefore(3); err != nil {
		t.Fatalf("TruncateBefore() error: %v", err)
	}

	msgs, err := w.ReadFrom(0, 10)
	if err != nil {
		t.Fatalf("ReadFrom() error: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("ReadFrom() returned %d messages after truncate, want 0", len(msgs))
	}
}
