package log

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSidecarRoundTrip(t *testing.T) {
	entries := []IndexEntry{
		{
			BaseOffset:     0,
			LastOffset:     9,
			Position:       0,
			BatchSize:      512,
			FirstTimestamp: 1000000,
			MaxTimestamp:   1000099,
		},
		{
			BaseOffset:     10,
			LastOffset:     19,
			Position:       512,
			BatchSize:      768,
			FirstTimestamp: 1000100,
			MaxTimestamp:   1000199,
		},
		{
			BaseOffset:     20,
			LastOffset:     29,
			Position:       1280,
			BatchSize:      1024,
			FirstTimestamp: 1000200,
			MaxTimestamp:   1000299,
		},
	}

	tsEntries := []TimestampIndexEntry{
		{Timestamp: 1000000, BaseOffset: 0},
		{Timestamp: 1000100, BaseOffset: 10},
		{Timestamp: 1000200, BaseOffset: 20},
	}

	var buf bytes.Buffer
	if err := WriteSidecar(&buf, entries, tsEntries); err != nil {
		t.Fatalf("WriteSidecar error: %v", err)
	}

	gotEntries, gotTS, err := ReadSidecar(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadSidecar error: %v", err)
	}

	if len(gotEntries) != len(entries) {
		t.Fatalf("IndexEntry count: want %d, got %d", len(entries), len(gotEntries))
	}
	for i, want := range entries {
		got := gotEntries[i]
		if got != want {
			t.Errorf("IndexEntry[%d]: want %+v, got %+v", i, want, got)
		}
	}

	if len(gotTS) != len(tsEntries) {
		t.Fatalf("TimestampIndexEntry count: want %d, got %d", len(tsEntries), len(gotTS))
	}
	for i, want := range tsEntries {
		got := gotTS[i]
		if got != want {
			t.Errorf("TimestampIndexEntry[%d]: want %+v, got %+v", i, want, got)
		}
	}
}

func TestSidecarEmptyEntries(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteSidecar(&buf, nil, nil); err != nil {
		t.Fatalf("WriteSidecar(nil, nil) error: %v", err)
	}

	gotEntries, gotTS, err := ReadSidecar(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadSidecar error: %v", err)
	}
	if len(gotEntries) != 0 {
		t.Errorf("want 0 IndexEntry, got %d", len(gotEntries))
	}
	if len(gotTS) != 0 {
		t.Errorf("want 0 TimestampIndexEntry, got %d", len(gotTS))
	}
}

func TestSidecarBadMagic(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteSidecar(&buf, nil, nil); err != nil {
		t.Fatalf("WriteSidecar error: %v", err)
	}
	data := buf.Bytes()
	// Corrupt magic bytes.
	data[0] ^= 0xFF

	_, _, err := ReadSidecar(data)
	if err == nil {
		t.Fatal("expected error for bad magic, got nil")
	}
}

func TestSidecarBadVersion(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteSidecar(&buf, nil, nil); err != nil {
		t.Fatalf("WriteSidecar error: %v", err)
	}
	data := buf.Bytes()
	// Version byte is at offset 4 (after 4-byte magic).
	data[4] = 0xFF

	_, _, err := ReadSidecar(data)
	if err == nil {
		t.Fatal("expected error for bad version, got nil")
	}
}

func TestSidecarTruncatedHeader(t *testing.T) {
	// Only 3 bytes — not even a full magic word.
	_, _, err := ReadSidecar([]byte{0x43, 0x49, 0x44})
	if err == nil {
		t.Fatal("expected error for truncated header, got nil")
	}
}

func TestSidecarTruncatedBody(t *testing.T) {
	// Build a valid header claiming 2 index entries, but provide no body bytes.
	var buf bytes.Buffer
	// magic
	_ = binary.Write(&buf, binary.BigEndian, uint32(0x43494458))
	// version
	buf.WriteByte(1)
	// offset_count = 2, ts_count = 0
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(0))
	// No body bytes.

	_, _, err := ReadSidecar(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for truncated body, got nil")
	}
}

func TestSidecarExactSize(t *testing.T) {
	// Verify the serialized size matches the spec.
	// header = 4 (magic) + 1 (version) + 4 (offset_count) + 4 (ts_count) = 13 bytes
	// 2 IndexEntry * 48 bytes = 96 bytes
	// 1 TimestampIndexEntry * 16 bytes = 16 bytes
	// total = 125 bytes
	entries := []IndexEntry{
		{BaseOffset: 0, LastOffset: 4, Position: 0, BatchSize: 100, FirstTimestamp: 1, MaxTimestamp: 5},
		{BaseOffset: 5, LastOffset: 9, Position: 100, BatchSize: 200, FirstTimestamp: 6, MaxTimestamp: 10},
	}
	tsEntries := []TimestampIndexEntry{
		{Timestamp: 1, BaseOffset: 0},
	}

	var buf bytes.Buffer
	if err := WriteSidecar(&buf, entries, tsEntries); err != nil {
		t.Fatalf("WriteSidecar error: %v", err)
	}

	const wantSize = 13 + 2*48 + 1*16
	if buf.Len() != wantSize {
		t.Errorf("serialized size: want %d, got %d", wantSize, buf.Len())
	}
}
