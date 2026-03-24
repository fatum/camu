package replication

import (
	"bytes"
	"testing"

	"github.com/maksim/camu/internal/log"
)

func TestWireFormat_RoundTrip(t *testing.T) {
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
