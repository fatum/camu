package producer

import (
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
)

func TestBatcher_FlushOnSize(t *testing.T) {
	var mu sync.Mutex
	var flushed []log.Message

	cfg := BatcherConfig{
		MaxSize: 100,
		MaxAge:  10 * time.Second,
		OnFlush: func(partitionID int, msgs []log.Message) error {
			mu.Lock()
			defer mu.Unlock()
			flushed = append(flushed, msgs...)
			return nil
		},
	}

	b := NewBatcher(cfg)
	defer b.Stop()

	// 10 messages × (20-byte value + 40 overhead) = 600 bytes total — well over MaxSize=100
	for i := 0; i < 10; i++ {
		b.Append(0, log.Message{
			Value: make([]byte, 20),
		})
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	count := len(flushed)
	mu.Unlock()

	if count == 0 {
		t.Fatal("expected at least one flush due to size threshold, got none")
	}
}

func TestBatcher_FlushOnTime(t *testing.T) {
	var mu sync.Mutex
	var flushed []log.Message

	cfg := BatcherConfig{
		MaxSize: 1 << 30, // 1 GB — won't trigger on size
		MaxAge:  50 * time.Millisecond,
		OnFlush: func(partitionID int, msgs []log.Message) error {
			mu.Lock()
			defer mu.Unlock()
			flushed = append(flushed, msgs...)
			return nil
		},
	}

	b := NewBatcher(cfg)
	defer b.Stop()

	b.Append(0, log.Message{
		Value: []byte("hello"),
	})

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	count := len(flushed)
	mu.Unlock()

	if count == 0 {
		t.Fatal("expected time-based flush, got none")
	}
}
