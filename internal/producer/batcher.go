package producer

import (
	"sync"
	"time"

	"github.com/maksim/camu/internal/log"
)

// BatcherConfig holds configuration for the Batcher.
type BatcherConfig struct {
	MaxSize int64
	MaxAge  time.Duration
	OnFlush func(partitionID int, msgs []log.Message) error
}

// partitionBuffer holds buffered messages for a single partition.
type partitionBuffer struct {
	msgs  []log.Message
	size  int64
	timer *time.Timer
	mu    sync.Mutex
}

// Batcher accumulates messages per partition and flushes them when either a
// size or time threshold is exceeded.
type Batcher struct {
	cfg     BatcherConfig
	buffers map[int]*partitionBuffer
	mu      sync.Mutex
}

// NewBatcher creates a new Batcher with the given configuration.
func NewBatcher(cfg BatcherConfig) *Batcher {
	return &Batcher{
		cfg:     cfg,
		buffers: make(map[int]*partitionBuffer),
	}
}

// getOrCreate returns the partitionBuffer for the given partition, creating it
// if it does not already exist. Caller must NOT hold b.mu.
func (b *Batcher) getOrCreate(partitionID int) *partitionBuffer {
	b.mu.Lock()
	buf, ok := b.buffers[partitionID]
	if !ok {
		buf = &partitionBuffer{}
		b.buffers[partitionID] = buf
	}
	b.mu.Unlock()
	return buf
}

// Append adds msg to the partition buffer. It estimates the message size as
// len(Key) + len(Value) + 40 bytes of overhead. If the buffer exceeds MaxSize
// after the append, a synchronous flush is triggered. Otherwise the age timer
// is (re)started so the buffer is flushed after MaxAge even without further
// writes.
func (b *Batcher) Append(partitionID int, msg log.Message) {
	buf := b.getOrCreate(partitionID)

	msgSize := int64(len(msg.Key) + len(msg.Value) + 40)

	buf.mu.Lock()
	buf.msgs = append(buf.msgs, msg)
	buf.size += msgSize

	shouldFlush := buf.size >= b.cfg.MaxSize

	if !shouldFlush {
		// Start or reset the age timer.
		if buf.timer == nil {
			buf.timer = time.AfterFunc(b.cfg.MaxAge, func() {
				b.flushPartition(partitionID)
			})
		} else {
			buf.timer.Reset(b.cfg.MaxAge)
		}
	}
	buf.mu.Unlock()

	if shouldFlush {
		b.flushPartition(partitionID)
	}
}

// flushPartition drains the buffer for partitionID and calls OnFlush. It is
// safe to call concurrently; the buffer mutex ensures only one flush runs at a
// time and an empty buffer is a no-op.
func (b *Batcher) flushPartition(partitionID int) {
	b.mu.Lock()
	buf, ok := b.buffers[partitionID]
	b.mu.Unlock()
	if !ok {
		return
	}

	buf.mu.Lock()
	if len(buf.msgs) == 0 {
		buf.mu.Unlock()
		return
	}
	msgs := buf.msgs
	buf.msgs = nil
	buf.size = 0
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	buf.mu.Unlock()

	if b.cfg.OnFlush != nil {
		_ = b.cfg.OnFlush(partitionID, msgs)
	}
}

// Flush manually flushes the buffer for partitionID.
func (b *Batcher) Flush(partitionID int) error {
	b.flushPartition(partitionID)
	return nil
}

// Stop flushes all remaining partition buffers and stops all timers.
func (b *Batcher) Stop() {
	b.mu.Lock()
	ids := make([]int, 0, len(b.buffers))
	for id := range b.buffers {
		ids = append(ids, id)
	}
	b.mu.Unlock()

	for _, id := range ids {
		b.flushPartition(id)
	}

	// Stop any timers that fired between the flush and now (edge case).
	b.mu.Lock()
	for _, buf := range b.buffers {
		buf.mu.Lock()
		if buf.timer != nil {
			buf.timer.Stop()
			buf.timer = nil
		}
		buf.mu.Unlock()
	}
	b.mu.Unlock()
}
