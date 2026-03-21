package producer

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBackpressure is returned by Append when the total buffered size across all
// partitions exceeds the configured HighWaterMark.
var ErrBackpressure = errors.New("backpressure: buffer full")

// BatcherConfig holds configuration for the Batcher.
type BatcherConfig struct {
	MaxSize       int64
	MaxAge        time.Duration
	OnFlush       func(partitionID int) error
	HighWaterMark int64 // 0 means disabled
}

// partitionBuffer holds size metadata for a single partition — no messages.
type partitionBuffer struct {
	count int
	size  int64
	timer *time.Timer
	mu    sync.Mutex
}

// Batcher tracks per-partition size metadata and triggers flushes when either a
// size or time threshold is exceeded. It does not store messages — the caller
// reads from the WAL at flush time.
type Batcher struct {
	cfg       BatcherConfig
	buffers   map[int]*partitionBuffer
	mu        sync.Mutex
	totalSize atomic.Int64 // total buffered bytes across all partitions
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

// Append records that msgSize bytes were added to partitionID. If the total
// buffered size across all partitions exceeds HighWaterMark (when non-zero),
// ErrBackpressure is returned. If the partition buffer exceeds MaxSize after the
// append, a synchronous flush is triggered. Otherwise the age timer is
// (re)started so the buffer is flushed after MaxAge even without further writes.
func (b *Batcher) Append(partitionID int, msgSize int64) error {
	// Check backpressure before buffering.
	if b.cfg.HighWaterMark > 0 && b.totalSize.Load()+msgSize > b.cfg.HighWaterMark {
		return ErrBackpressure
	}

	buf := b.getOrCreate(partitionID)

	buf.mu.Lock()
	buf.count++
	buf.size += msgSize
	b.totalSize.Add(msgSize)

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
	return nil
}

// flushPartition drains the metadata for partitionID and calls OnFlush. It is
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
	if buf.count == 0 {
		buf.mu.Unlock()
		return
	}
	flushedSize := buf.size
	buf.count = 0
	buf.size = 0
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	buf.mu.Unlock()

	b.totalSize.Add(-flushedSize)

	if b.cfg.OnFlush != nil {
		_ = b.cfg.OnFlush(partitionID)
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
