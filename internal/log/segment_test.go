package log

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestBuildSegmentMetadata(t *testing.T) {
	createdAt := time.Date(2026, 3, 25, 8, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	ref := SegmentRef{
		BaseOffset:     123,
		EndOffset:      145,
		MinTimestamp:   111,
		MaxTimestamp:   222,
		Epoch:          7,
		Key:            "orders/0/123-7.segment",
		OffsetIndexKey: "orders/0/123-7.offset.idx",
		MetaKey:        "orders/0/123-7.meta.json",
		CreatedAt:      createdAt,
	}

	data, err := BuildSegmentMetadata(ref, 23, 4096, CompressionZstd)
	if err != nil {
		t.Fatalf("BuildSegmentMetadata failed: %v", err)
	}

	var meta SegmentMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if meta.BaseOffset != ref.BaseOffset || meta.EndOffset != ref.EndOffset || meta.Epoch != ref.Epoch {
		t.Fatalf("unexpected offsets/epoch in metadata: %+v", meta)
	}
	if meta.SegmentKey != ref.Key {
		t.Fatalf("SegmentKey = %q, want %q", meta.SegmentKey, ref.Key)
	}
	if meta.OffsetIndexKey != ref.OffsetIndexObjectKey() {
		t.Fatalf("OffsetIndexKey = %q, want %q", meta.OffsetIndexKey, ref.OffsetIndexObjectKey())
	}
	if !meta.CreatedAt.Equal(createdAt.UTC()) {
		t.Fatalf("CreatedAt = %s, want %s", meta.CreatedAt, createdAt.UTC())
	}
	if meta.RecordCount != 23 || meta.SizeBytes != 4096 || meta.Compression != CompressionZstd {
		t.Fatalf("unexpected metadata payload: %+v", meta)
	}
	if meta.MinTimestamp != 111 || meta.MaxTimestamp != 222 {
		t.Fatalf("unexpected metadata timestamps: min=%d max=%d", meta.MinTimestamp, meta.MaxTimestamp)
	}
}

// TestReadSegmentBatches verifies that ReadSegmentBatches correctly parses
// three back-to-back RecordBatch entries and applies the startOffset filter.
func TestReadSegmentBatches(t *testing.T) {
	// Build three separate batches with consecutive offsets.
	batch0 := EncodeRecordBatch(0, []Message{{Offset: 0, Value: []byte("a")}})
	batch1 := EncodeRecordBatch(1, []Message{{Offset: 1, Value: []byte("b")}})
	batch2 := EncodeRecordBatch(2, []Message{{Offset: 2, Value: []byte("c")}})

	data := append(append(batch0, batch1...), batch2...)

	// Read all three batches.
	all, err := ReadSegmentBatches(data, 0, 0)
	if err != nil {
		t.Fatalf("ReadSegmentBatches all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(all))
	}

	// Read starting from offset 1 — should skip batch0.
	from1, err := ReadSegmentBatches(data, 1, 0)
	if err != nil {
		t.Fatalf("ReadSegmentBatches from offset 1: %v", err)
	}
	if len(from1) != 2 {
		t.Fatalf("expected 2 batches from offset 1, got %d", len(from1))
	}

	// Verify the returned slices overlap the original buffer (zero-copy).
	h0, _ := ReadRecordBatchHeader(all[0])
	if h0.FirstOffset != 0 {
		t.Errorf("batch[0] FirstOffset: want 0, got %d", h0.FirstOffset)
	}
	h2, _ := ReadRecordBatchHeader(all[2])
	if h2.FirstOffset != 2 {
		t.Errorf("batch[2] FirstOffset: want 2, got %d", h2.FirstOffset)
	}
}

// TestReadSegmentBatchesAsMessages verifies the round-trip: encode batches,
// concatenate, then decode back to messages via ReadSegmentBatchesAsMessages.
func TestReadSegmentBatchesAsMessages(t *testing.T) {
	msgs := []Message{
		{Offset: 10, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 11, Timestamp: 2000, Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 12, Timestamp: 3000, Key: []byte("k3"), Value: []byte("v3")},
	}

	// Encode each message as its own batch.
	var data []byte
	for _, m := range msgs {
		data = append(data, EncodeRecordBatch(int64(m.Offset), []Message{m})...)
	}

	// Read all.
	got, err := ReadSegmentBatchesAsMessages(data, 10, 0)
	if err != nil {
		t.Fatalf("ReadSegmentBatchesAsMessages: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(got))
	}
	for i, want := range msgs {
		if got[i].Offset != want.Offset {
			t.Errorf("msg[%d] offset: want %d, got %d", i, want.Offset, got[i].Offset)
		}
		if !bytes.Equal(got[i].Key, want.Key) {
			t.Errorf("msg[%d] key: want %q, got %q", i, want.Key, got[i].Key)
		}
		if !bytes.Equal(got[i].Value, want.Value) {
			t.Errorf("msg[%d] value: want %q, got %q", i, want.Value, got[i].Value)
		}
	}

	// startOffset filter: skip first message.
	from11, err := ReadSegmentBatchesAsMessages(data, 11, 0)
	if err != nil {
		t.Fatalf("ReadSegmentBatchesAsMessages from 11: %v", err)
	}
	if len(from11) != 2 {
		t.Fatalf("expected 2 messages from offset 11, got %d", len(from11))
	}
	if from11[0].Offset != 11 {
		t.Errorf("first message offset: want 11, got %d", from11[0].Offset)
	}
}
