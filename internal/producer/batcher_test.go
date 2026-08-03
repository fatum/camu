package producer

import (
	"errors"
	"sync"
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
		MaxAge:  time.Hour,
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

	deadline := time.Now().Add(time.Second)
	for flushCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
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

func TestBatcher_SizeFlushDoesNotBlockAppend(t *testing.T) {
	flushStarted := make(chan struct{})
	allowFlush := make(chan struct{})
	var startOnce sync.Once
	b := NewBatcher(BatcherConfig{
		MaxSize: 1,
		MaxAge:  time.Hour,
		OnFlush: func(int) error {
			startOnce.Do(func() { close(flushStarted) })
			<-allowFlush
			return nil
		},
	})
	defer func() {
		close(allowFlush)
		b.Stop()
	}()

	if err := b.Append(0, 1); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}

	done := make(chan error, 1)
	go func() { done <- b.Append(0, 1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Append blocked on an in-flight flush")
	}
}

func TestBatcher_StopFlushesDataAppendedDuringInflightFlush(t *testing.T) {
	flushStarted := make(chan struct{})
	allowFirstFlush := make(chan struct{})
	var flushes atomic.Int32
	b := NewBatcher(BatcherConfig{
		MaxSize: 1,
		MaxAge:  time.Hour,
		OnFlush: func(int) error {
			if flushes.Add(1) == 1 {
				close(flushStarted)
				<-allowFirstFlush
			}
			return nil
		},
	})

	requireAppend := func() {
		if err := b.Append(0, 1); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	requireAppend()
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("first flush did not start")
	}
	requireAppend()

	done := make(chan struct{})
	go func() {
		b.Stop()
		close(done)
	}()
	close(allowFirstFlush)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish")
	}
	if got := flushes.Load(); got != 2 {
		t.Fatalf("flushes = %d, want 2", got)
	}
}
