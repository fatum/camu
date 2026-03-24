package producer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatcher_FlushOnSize(t *testing.T) {
	var flushCount atomic.Int32

	cfg := BatcherConfig{
		MaxSize: 100,
		MaxAge:  10 * time.Second,
		OnFlush: func(partitionID int) error {
			flushCount.Add(1)
			return nil
		},
	}

	b := NewBatcher(cfg)
	defer b.Stop()

	// 10 messages × (20 + 40 overhead) = 600 bytes total — well over MaxSize=100
	for i := 0; i < 10; i++ {
		b.Append(0, int64(20+40))
	}

	time.Sleep(100 * time.Millisecond)

	if flushCount.Load() == 0 {
		t.Fatal("expected at least one flush due to size threshold, got none")
	}
}

func TestBatcher_FlushOnTime(t *testing.T) {
	var flushCount atomic.Int32

	cfg := BatcherConfig{
		MaxSize: 1 << 30, // 1 GB — won't trigger on size
		MaxAge:  50 * time.Millisecond,
		OnFlush: func(partitionID int) error {
			flushCount.Add(1)
			return nil
		},
	}

	b := NewBatcher(cfg)
	defer b.Stop()

	b.Append(0, int64(5+40))

	time.Sleep(200 * time.Millisecond)

	if flushCount.Load() == 0 {
		t.Fatal("expected time-based flush, got none")
	}
}

func TestBatcher_RetainsBufferOnFlushError(t *testing.T) {
	var flushCount atomic.Int32
	fail := atomic.Bool{}
	fail.Store(true)

	b := NewBatcher(BatcherConfig{
		MaxSize: 10,
		MaxAge:  50 * time.Millisecond,
		OnFlush: func(partitionID int) error {
			flushCount.Add(1)
			if fail.Load() {
				return errors.New("boom")
			}
			return nil
		},
	})
	defer b.Stop()

	if err := b.Append(0, 10); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if flushCount.Load() != 1 {
		t.Fatalf("expected first flush attempt, got %d", flushCount.Load())
	}

	fail.Store(false)
	if err := b.Flush(0); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if flushCount.Load() != 2 {
		t.Fatalf("expected buffered retry flush, got %d attempts", flushCount.Load())
	}
}
