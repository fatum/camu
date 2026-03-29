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
	if b.Meta.ProducerID != 42 {
		t.Errorf("ProducerID = %d, want 42", b.Meta.ProducerID)
	}
	if b.Meta.Sequence != 7 {
		t.Errorf("Sequence = %d, want 7", b.Meta.Sequence)
	}
	if b.Meta.MessageCount != 2 {
		t.Fatalf("got %d msgs, want 2", b.Meta.MessageCount)
	}
	msgs, err := ReadMessageFrames(bytes.NewReader(b.Data))
	if err != nil {
		t.Fatal(err)
	}
	if msgs[0].Offset != 100 || string(msgs[0].Key) != "k1" {
		t.Errorf("msg 0 mismatch: %+v", msgs[0])
	}
	if msgs[1].Offset != 101 || string(msgs[1].Key) != "k2" {
		t.Errorf("msg 1 mismatch: %+v", msgs[1])
	}
	if msgs[1].Headers["h1"] != "val1" {
		t.Errorf("msg 1 header mismatch: %v", msgs[1].Headers)
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
	if got[0].Meta.ProducerID != 1 || got[0].Meta.Sequence != 10 {
		t.Errorf("batch 0 metadata mismatch: pid=%d seq=%d", got[0].Meta.ProducerID, got[0].Meta.Sequence)
	}
	if got[1].Meta.ProducerID != 2 || got[1].Meta.Sequence != 20 {
		t.Errorf("batch 1 metadata mismatch: pid=%d seq=%d", got[1].Meta.ProducerID, got[1].Meta.Sequence)
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
	if batches[0].Meta.ProducerID != 0 {
		t.Errorf("ProducerID = %d, want 0", batches[0].Meta.ProducerID)
	}
	if batches[0].Meta.Sequence != 0 {
		t.Errorf("Sequence = %d, want 0", batches[0].Meta.Sequence)
	}
	if batches[0].Meta.MessageCount != 1 {
		t.Fatalf("got %d msgs, want 1", batches[0].Meta.MessageCount)
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
	if got[0].Meta.ProducerID != 5 {
		t.Errorf("ProducerID = %d, want 5", got[0].Meta.ProducerID)
	}
	if got[0].Meta.MessageCount != 0 {
		t.Errorf("expected 0 messages, got %d", got[0].Meta.MessageCount)
	}
}
