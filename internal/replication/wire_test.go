package replication

import (
	"bytes"
	"testing"

	"github.com/maksim/camu/internal/log"
)

func TestWireFormat_BatchEnvelopeRoundTrip(t *testing.T) {
	batch := log.Batch{
		ProducerID: 42,
		Sequence:   7,
		Messages: []log.Message{
			{Offset: 100, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1")},
			{Offset: 101, Timestamp: 1001, Key: []byte("k2"), Value: []byte("v2"),
				Headers: map[string]string{"h1": "val1"}},
		},
	}

	var buf bytes.Buffer
	if err := WriteBatchFrames(&buf, batch); err != nil {
		t.Fatal(err)
	}

	got, err := ReadBatchFrames(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d batches, want 1", len(got))
	}
	b := got[0]
	if b.ProducerID != 42 {
		t.Errorf("ProducerID = %d, want 42", b.ProducerID)
	}
	if b.Sequence != 7 {
		t.Errorf("Sequence = %d, want 7", b.Sequence)
	}
	if len(b.Messages) != 2 {
		t.Fatalf("got %d msgs, want 2", len(b.Messages))
	}
	if b.Messages[0].Offset != 100 || string(b.Messages[0].Key) != "k1" {
		t.Errorf("msg 0 mismatch: %+v", b.Messages[0])
	}
	if b.Messages[1].Offset != 101 || string(b.Messages[1].Key) != "k2" {
		t.Errorf("msg 1 mismatch: %+v", b.Messages[1])
	}
	if b.Messages[1].Headers["h1"] != "val1" {
		t.Errorf("msg 1 header mismatch: %v", b.Messages[1].Headers)
	}
}

func TestWireFormat_MultipleBatchesRoundTrip(t *testing.T) {
	batch1 := log.Batch{
		ProducerID: 1,
		Sequence:   10,
		Messages:   []log.Message{{Offset: 1, Timestamp: 100, Key: []byte("a"), Value: []byte("b")}},
	}
	batch2 := log.Batch{
		ProducerID: 2,
		Sequence:   20,
		Messages:   []log.Message{{Offset: 2, Timestamp: 200, Key: []byte("c"), Value: []byte("d")}},
	}

	var buf bytes.Buffer
	if err := WriteBatchFrames(&buf, batch1); err != nil {
		t.Fatal(err)
	}
	if err := WriteBatchFrames(&buf, batch2); err != nil {
		t.Fatal(err)
	}

	got, err := ReadBatchFrames(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d batches, want 2", len(got))
	}
	if got[0].ProducerID != 1 || got[0].Sequence != 10 {
		t.Errorf("batch 0 metadata mismatch: pid=%d seq=%d", got[0].ProducerID, got[0].Sequence)
	}
	if got[1].ProducerID != 2 || got[1].Sequence != 20 {
		t.Errorf("batch 1 metadata mismatch: pid=%d seq=%d", got[1].ProducerID, got[1].Sequence)
	}
}

func TestWireFormat_LegacyRoundTrip(t *testing.T) {
	msgs := []log.Message{
		{Offset: 100, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 101, Timestamp: 1001, Key: []byte("k2"), Value: []byte("v2")},
	}
	var buf bytes.Buffer
	if err := WriteMessageFrames(&buf, msgs); err != nil {
		t.Fatal(err)
	}
	got, err := ReadMessageFrames(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d msgs, want 2", len(got))
	}
	if got[0].Offset != 100 || string(got[0].Key) != "k1" {
		t.Errorf("msg 0 mismatch")
	}
	if got[1].Offset != 101 || string(got[1].Key) != "k2" {
		t.Errorf("msg 1 mismatch")
	}
}

func TestWireFormat_LegacyRoundTrip_ReadAsBatch(t *testing.T) {
	// WriteMessageFrames should produce batch envelopes readable by ReadBatchFrames
	msgs := []log.Message{
		{Offset: 50, Timestamp: 500, Key: []byte("x"), Value: []byte("y")},
	}
	var buf bytes.Buffer
	if err := WriteMessageFrames(&buf, msgs); err != nil {
		t.Fatal(err)
	}
	batches, err := ReadBatchFrames(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(batches))
	}
	if batches[0].ProducerID != 0 {
		t.Errorf("ProducerID = %d, want 0", batches[0].ProducerID)
	}
	if batches[0].Sequence != 0 {
		t.Errorf("Sequence = %d, want 0", batches[0].Sequence)
	}
	if len(batches[0].Messages) != 1 {
		t.Fatalf("got %d msgs, want 1", len(batches[0].Messages))
	}
}

func TestWireFormat_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessageFrames(&buf, nil); err != nil {
		t.Fatal(err)
	}
	got, err := ReadMessageFrames(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

func TestWireFormat_EmptyBatch(t *testing.T) {
	batch := log.Batch{ProducerID: 5, Sequence: 1}
	var buf bytes.Buffer
	if err := WriteBatchFrames(&buf, batch); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBatchFrames(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d batches, want 1", len(got))
	}
	if got[0].ProducerID != 5 {
		t.Errorf("ProducerID = %d, want 5", got[0].ProducerID)
	}
	if len(got[0].Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(got[0].Messages))
	}
}
