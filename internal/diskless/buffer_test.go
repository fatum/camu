package diskless

import "testing"

func TestBuffer_Append(t *testing.T) {
	buf := NewBuffer(1024)
	entry := BufferEntry{
		Topic:     "t1",
		Partition: 0,
		Batch:     make([]byte, 100),
		Done:      make(chan FlushResult, 1),
	}
	buf.Append(entry)

	if buf.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", buf.Len())
	}
	if buf.Size() != 100 {
		t.Fatalf("expected Size=100, got %d", buf.Size())
	}
}

func TestBuffer_DrainReturnsAllEntries(t *testing.T) {
	buf := NewBuffer(1024)
	for i := 0; i < 3; i++ {
		buf.Append(BufferEntry{
			Topic:     "t1",
			Partition: i,
			Batch:     make([]byte, 50),
			Done:      make(chan FlushResult, 1),
		})
	}

	entries := buf.Drain()
	if len(entries) != 3 {
		t.Fatalf("expected 3 drained entries, got %d", len(entries))
	}
	if buf.Len() != 0 {
		t.Fatalf("expected buffer empty after drain, got Len=%d", buf.Len())
	}
	if buf.Size() != 0 {
		t.Fatalf("expected Size=0 after drain, got %d", buf.Size())
	}
}

func TestBuffer_SizeThresholdReached(t *testing.T) {
	buf := NewBuffer(200)

	buf.Append(BufferEntry{
		Topic:     "t1",
		Partition: 0,
		Batch:     make([]byte, 150),
		Done:      make(chan FlushResult, 1),
	})
	if buf.ShouldFlush() {
		t.Fatal("should not flush at 150 bytes with threshold 200")
	}

	buf.Append(BufferEntry{
		Topic:     "t1",
		Partition: 0,
		Batch:     make([]byte, 60),
		Done:      make(chan FlushResult, 1),
	})
	if !buf.ShouldFlush() {
		t.Fatal("should flush at 210 bytes with threshold 200")
	}
}
