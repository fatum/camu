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
	w, err := OpenWAL(path, false)
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
	w2, err := OpenWAL(path, false)
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

	w, err := OpenWAL(path, false)
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
	w2, err := OpenWAL(path, false)
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

	w, err := OpenWAL(path, false)
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

func TestWAL_UnflushedMessages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := OpenWAL(path, false)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	for i := 0; i < 3; i++ {
		m := Message{Offset: uint64(i), Timestamp: int64(i * 100), Value: []byte("val")}
		if err := w.Append(m); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	unflushed := w.UnflushedFrom(1)
	if len(unflushed) != 2 {
		t.Fatalf("expected 2 unflushed messages from offset 1, got %d", len(unflushed))
	}
	if unflushed[0].Offset != 1 {
		t.Errorf("expected offset 1, got %d", unflushed[0].Offset)
	}
	if unflushed[1].Offset != 2 {
		t.Errorf("expected offset 2, got %d", unflushed[1].Offset)
	}
}

func TestWAL_CorruptEntrySkipped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.wal")

	// Write one good message
	w, err := OpenWAL(path, false)
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
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0xDE, 0xAD}) // partial entry
	f.Close()

	// Replay should return only the valid message
	w2, err := OpenWAL(path, false)
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

	w, err := OpenWAL(path, true)
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

	// Verify in-memory buffer
	unflushed := w.UnflushedFrom(0)
	if len(unflushed) != 3 {
		t.Fatalf("UnflushedFrom(0) = %d messages, want 3", len(unflushed))
	}

	w.Close()

	// Verify replay after reopen
	w2, err := OpenWAL(path, true)
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

	w, err := OpenWAL(path, true)
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
