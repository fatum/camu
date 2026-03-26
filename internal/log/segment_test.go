package log

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

const testSegmentBatchSize = 16 * 1024
// testSegmentBatchSize is the default on-disk batch target used by segment
// tests unless a case needs smaller batches to force multiple batch boundaries.

// TestSegmentRoundTrip writes 3 messages and reads them back, verifying all fields.
func TestSegmentRoundTrip(t *testing.T) {
	msgs := []Message{
		{
			Offset:    1,
			Timestamp: 1000,
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Headers:   map[string]string{"content-type": "text/plain", "x-trace": "abc123"},
		},
		{
			Offset:    2,
			Timestamp: 2000,
			Key:       nil,
			Value:     []byte("value2"),
			Headers:   nil,
		},
		{
			Offset:    3,
			Timestamp: 3000,
			Key:       []byte("key3"),
			Value:     []byte("value3"),
			Headers: map[string]string{
				"h1": "v1",
				"h2": "v2",
				"h3": "v3",
			},
		},
	}

	var buf bytes.Buffer
	if err := WriteSegment(&buf, msgs, CompressionNone, testSegmentBatchSize); err != nil {
		t.Fatalf("WriteSegment failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	got, err := ReadSegment(r, int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment failed: %v", err)
	}

	if len(got) != len(msgs) {
		t.Fatalf("expected %d messages, got %d", len(msgs), len(got))
	}

	for i, want := range msgs {
		g := got[i]
		if g.Offset != want.Offset {
			t.Errorf("msg[%d]: offset: want %d, got %d", i, want.Offset, g.Offset)
		}
		if g.Timestamp != want.Timestamp {
			t.Errorf("msg[%d]: timestamp: want %d, got %d", i, want.Timestamp, g.Timestamp)
		}
		if !bytes.Equal(g.Key, want.Key) {
			t.Errorf("msg[%d]: key: want %q, got %q", i, want.Key, g.Key)
		}
		if !bytes.Equal(g.Value, want.Value) {
			t.Errorf("msg[%d]: value: want %q, got %q", i, want.Value, g.Value)
		}
		if len(g.Headers) != len(want.Headers) {
			t.Errorf("msg[%d]: headers count: want %d, got %d", i, len(want.Headers), len(g.Headers))
			continue
		}
		for k, v := range want.Headers {
			if g.Headers[k] != v {
				t.Errorf("msg[%d]: header[%q]: want %q, got %q", i, k, v, g.Headers[k])
			}
		}
	}
}

// TestSegmentMagicNumber verifies that garbage data returns an error.
func TestSegmentMagicNumber(t *testing.T) {
	garbage := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	r := bytes.NewReader(garbage)
	_, err := ReadSegment(r, int64(len(garbage)))
	if err == nil {
		t.Fatal("expected error reading garbage data, got nil")
	}
}

// TestSegmentEmpty writes an empty message slice and reads it back.
func TestSegmentEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteSegment(&buf, []Message{}, CompressionNone, testSegmentBatchSize); err != nil {
		t.Fatalf("WriteSegment failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	got, err := ReadSegment(r, int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment failed: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(got))
	}
}

func TestSegmentRoundTrip_Snappy(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k0"), Value: []byte("hello world")},
		{Offset: 1, Timestamp: 2000, Key: []byte("k1"), Value: []byte("goodbye world")},
	}
	var buf bytes.Buffer
	err := WriteSegment(&buf, msgs, CompressionSnappy, testSegmentBatchSize)
	if err != nil {
		t.Fatalf("WriteSegment(snappy) error: %v", err)
	}
	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment(snappy) error: %v", err)
	}
	if len(got) != len(msgs) {
		t.Fatalf("got %d messages, want %d", len(got), len(msgs))
	}
	if string(got[1].Value) != "goodbye world" {
		t.Errorf("value = %q, want %q", string(got[1].Value), "goodbye world")
	}
}

func TestSegmentRoundTrip_Zstd(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k0"), Value: []byte("hello world")},
		{Offset: 1, Timestamp: 2000, Key: []byte("k1"), Value: []byte("goodbye world")},
	}
	var buf bytes.Buffer
	err := WriteSegment(&buf, msgs, CompressionZstd, testSegmentBatchSize)
	if err != nil {
		t.Fatalf("WriteSegment(zstd) error: %v", err)
	}
	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment(zstd) error: %v", err)
	}
	if len(got) != len(msgs) {
		t.Fatalf("got %d messages, want %d", len(got), len(msgs))
	}
	if string(got[1].Value) != "goodbye world" {
		t.Errorf("value = %q, want %q", string(got[1].Value), "goodbye world")
	}
}

func TestSegmentReadFromOffset_Compressed(t *testing.T) {
	msgs := []Message{
		{Offset: 10, Timestamp: 100, Key: []byte("k10"), Value: []byte("v10")},
		{Offset: 11, Timestamp: 110, Key: []byte("k11"), Value: []byte("v11")},
		{Offset: 12, Timestamp: 120, Key: []byte("k12"), Value: []byte("v12")},
	}

	for _, compression := range []string{CompressionSnappy, CompressionZstd} {
		t.Run(compression, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteSegment(&buf, msgs, compression, testSegmentBatchSize); err != nil {
				t.Fatalf("WriteSegment(%s) failed: %v", compression, err)
			}

			got, err := ReadSegmentFromOffset(bytes.NewReader(buf.Bytes()), int64(buf.Len()), 11, 1)
			if err != nil {
				t.Fatalf("ReadSegmentFromOffset(%s) failed: %v", compression, err)
			}
			if len(got) != 1 {
				t.Fatalf("got %d messages, want 1", len(got))
			}
			if got[0].Offset != 11 {
				t.Fatalf("offset = %d, want 11", got[0].Offset)
			}
		})
	}
}

// TestSegmentReadFromOffset writes messages with offsets 10,11,12 and reads from offset 11.
func TestSegmentReadFromOffset(t *testing.T) {
	msgs := []Message{
		{Offset: 10, Timestamp: 100, Key: []byte("k10"), Value: []byte("v10")},
		{Offset: 11, Timestamp: 110, Key: []byte("k11"), Value: []byte("v11")},
		{Offset: 12, Timestamp: 120, Key: []byte("k12"), Value: []byte("v12")},
	}

	var buf bytes.Buffer
	if err := WriteSegment(&buf, msgs, CompressionNone, testSegmentBatchSize); err != nil {
		t.Fatalf("WriteSegment failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	got, err := ReadSegmentFromOffset(r, int64(buf.Len()), 11, 10)
	if err != nil {
		t.Fatalf("ReadSegmentFromOffset failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}

	if got[0].Offset != 11 {
		t.Errorf("first message offset: want 11, got %d", got[0].Offset)
	}
	if got[1].Offset != 12 {
		t.Errorf("second message offset: want 12, got %d", got[1].Offset)
	}
}

func TestSegmentOffsetIndex_SeekToBatchBoundary(t *testing.T) {
	msgs := []Message{
		{Offset: 10, Timestamp: 100, Key: []byte("k10"), Value: bytes.Repeat([]byte("a"), 700)},
		{Offset: 11, Timestamp: 110, Key: []byte("k11"), Value: bytes.Repeat([]byte("b"), 700)},
		{Offset: 12, Timestamp: 120, Key: []byte("k12"), Value: bytes.Repeat([]byte("c"), 700)},
		{Offset: 13, Timestamp: 130, Key: []byte("k13"), Value: bytes.Repeat([]byte("d"), 700)},
	}

	var buf bytes.Buffer
	if err := WriteSegment(&buf, msgs, CompressionSnappy, minSegmentRecordBatchTargetSize); err != nil {
		t.Fatalf("WriteSegment failed: %v", err)
	}
	indexData, err := BuildSegmentOffsetIndex(buf.Bytes(), 10, 1)
	if err != nil {
		t.Fatalf("BuildSegmentOffsetIndex failed: %v", err)
	}
	if len(indexData) < 16 {
		t.Fatalf("expected multiple index entries, got %d bytes", len(indexData))
	}

	got, err := ReadSegmentFromOffsetWithIndex(bytes.NewReader(buf.Bytes()), int64(buf.Len()), indexData, 10, 12, 2)
	if err != nil {
		t.Fatalf("ReadSegmentFromOffsetWithIndex failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}
	if got[0].Offset != 12 || got[1].Offset != 13 {
		t.Fatalf("offsets = [%d %d], want [12 13]", got[0].Offset, got[1].Offset)
	}
}

func TestReadSegmentFromOffsetWithIndex_NilIndexFallsBackToScan(t *testing.T) {
	msgs := []Message{
		{Offset: 20, Timestamp: 200, Key: []byte("k20"), Value: []byte("v20")},
		{Offset: 21, Timestamp: 210, Key: []byte("k21"), Value: []byte("v21")},
		{Offset: 22, Timestamp: 220, Key: []byte("k22"), Value: []byte("v22")},
	}

	var buf bytes.Buffer
	if err := WriteSegment(&buf, msgs, CompressionNone, testSegmentBatchSize); err != nil {
		t.Fatalf("WriteSegment failed: %v", err)
	}

	got, err := ReadSegmentFromOffsetWithIndex(bytes.NewReader(buf.Bytes()), int64(buf.Len()), nil, 20, 21, 2)
	if err != nil {
		t.Fatalf("ReadSegmentFromOffsetWithIndex failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}
	if got[0].Offset != 21 || got[1].Offset != 22 {
		t.Fatalf("offsets = [%d %d], want [21 22]", got[0].Offset, got[1].Offset)
	}
}

func TestBuildSegmentMetadata(t *testing.T) {
	createdAt := time.Date(2026, 3, 25, 8, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	ref := SegmentRef{
		BaseOffset:     123,
		EndOffset:      145,
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
}

func TestWriteSegment_DeterministicHeaderEncoding(t *testing.T) {
	msgs := []Message{
		{
			Offset:    1,
			Timestamp: 100,
			Key:       []byte("k"),
			Value:     []byte("v"),
			Headers: map[string]string{
				"z-key": "z",
				"a-key": "a",
				"m-key": "m",
			},
		},
	}

	var buf1 bytes.Buffer
	if err := WriteSegment(&buf1, msgs, CompressionNone, testSegmentBatchSize); err != nil {
		t.Fatalf("WriteSegment(first) failed: %v", err)
	}

	var buf2 bytes.Buffer
	if err := WriteSegment(&buf2, msgs, CompressionNone, testSegmentBatchSize); err != nil {
		t.Fatalf("WriteSegment(second) failed: %v", err)
	}

	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Fatal("segment encoding should be deterministic for identical input")
	}
}

func TestBuildSegmentBatches_AppliesMinimumBatchTargetSize(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1, Key: []byte("k0"), Value: bytes.Repeat([]byte("a"), 32)},
		{Offset: 1, Timestamp: 2, Key: []byte("k1"), Value: bytes.Repeat([]byte("b"), 32)},
	}

	tooSmall := int64(1)
	withFloor, err := buildSegmentBatches(msgs, tooSmall)
	if err != nil {
		t.Fatalf("buildSegmentBatches(with floor) failed: %v", err)
	}
	atMinimum, err := buildSegmentBatches(msgs, minSegmentRecordBatchTargetSize)
	if err != nil {
		t.Fatalf("buildSegmentBatches(at minimum) failed: %v", err)
	}

	if len(withFloor) != len(atMinimum) {
		t.Fatalf("batch count with tiny target = %d, want %d", len(withFloor), len(atMinimum))
	}
}
