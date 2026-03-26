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

	msgs := []Message{
		{Offset: 0, Value: []byte("aaaaaaaaaaaaaaaa")},
		{Offset: 1, Value: []byte("bbbbbbbbbbbbbbbb")},
		{Offset: 2, Value: []byte("cccccccccccccccc")},
	}
	if err := w.AppendBatch(msgs); err != nil {
		t.Fatalf("AppendBatch() error: %v", err)
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
