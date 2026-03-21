package log

import (
	"bytes"
	"testing"
)

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
	if err := WriteSegment(&buf, msgs, CompressionNone); err != nil {
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
	if err := WriteSegment(&buf, []Message{}, CompressionNone); err != nil {
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
		{Offset: 0, Timestamp: 1000, Key: []byte("k"), Value: []byte("hello world")},
	}
	var buf bytes.Buffer
	err := WriteSegment(&buf, msgs, CompressionSnappy)
	if err != nil {
		t.Fatalf("WriteSegment(snappy) error: %v", err)
	}
	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment(snappy) error: %v", err)
	}
	if string(got[0].Value) != "hello world" {
		t.Errorf("value = %q, want %q", string(got[0].Value), "hello world")
	}
}

func TestSegmentRoundTrip_Zstd(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k"), Value: []byte("hello world")},
	}
	var buf bytes.Buffer
	err := WriteSegment(&buf, msgs, CompressionZstd)
	if err != nil {
		t.Fatalf("WriteSegment(zstd) error: %v", err)
	}
	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment(zstd) error: %v", err)
	}
	if string(got[0].Value) != "hello world" {
		t.Errorf("value = %q, want %q", string(got[0].Value), "hello world")
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
	if err := WriteSegment(&buf, msgs, CompressionNone); err != nil {
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
