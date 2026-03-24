package log

import (
	"path/filepath"
	"testing"
)

func TestWAL_ReadFrom(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(filepath.Join(dir, "test.wal"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.AppendBatch([]Message{
		{Offset: 10, Key: []byte("a"), Value: []byte("1")},
		{Offset: 11, Key: []byte("b"), Value: []byte("2")},
		{Offset: 12, Key: []byte("c"), Value: []byte("3")},
	})

	// Read from offset 11
	msgs, err := w.ReadFrom(11, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d, want 2", len(msgs))
	}
	if msgs[0].Offset != 11 {
		t.Errorf("first offset = %d, want 11", msgs[0].Offset)
	}
	if msgs[1].Offset != 12 {
		t.Errorf("second offset = %d, want 12", msgs[1].Offset)
	}
}

func TestWAL_ReadFrom_WithLimit(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(filepath.Join(dir, "test.wal"), true)
	defer w.Close()
	w.AppendBatch([]Message{
		{Offset: 10}, {Offset: 11}, {Offset: 12}, {Offset: 13},
	})
	msgs, _ := w.ReadFrom(10, 2)
	if len(msgs) != 2 {
		t.Fatalf("got %d, want 2", len(msgs))
	}
}

func TestWAL_ReadFrom_Empty(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(filepath.Join(dir, "test.wal"), true)
	defer w.Close()
	msgs, err := w.ReadFrom(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Errorf("got %d, want 0", len(msgs))
	}
}
