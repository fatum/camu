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
	HighWaterMark int64 // per-partition allowance in bytes; 0 means disabled
}

// partitionBuffer holds size metadata for a single partition — no messages.
type partitionBuffer struct {
	count    int
	size     int64
	timer    *time.Timer
	flushing bool
	mu       sync.Mutex
}

// Batcher tracks per-partition size metadata and triggers flushes when either a
// size or time threshold is exceeded. It does not store messages — the caller
// reads from local active segments at flush time.
type Batcher struct {
	cfg       BatcherConfig
	buffers   map[int]*partitionBuffer
	mu        sync.Mutex
	totalSize atomic.Int64 // total buffered bytes across all partitions
	stopped   atomic.Bool
	flushWG   sync.WaitGroup
	stopMu    sync.Mutex // prevents a flush from being scheduled after Stop starts waiting
}

// NewBatcher creates a new Batcher with the given configuration.
func NewBatcher(cfg BatcherConfig) *Batcher {
	return &Batcher{
		cfg:     cfg,
		buffers: make(map[int]*partitionBuffer),
	}
}

// activePartitions returns the number of partitions that currently have a
// buffer in the batcher.
func (b *Batcher) activePartitions() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffers)
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
// buffered size across all partitions exceeds HighWaterMark * activePartitions
// (when HighWaterMark is non-zero), ErrBackpressure is returned. If the
// partition buffer exceeds MaxSize after the append, a background flush is
// scheduled. Otherwise the age timer is (re)started so the buffer is flushed
// after MaxAge even without further writes.
// OnFlush is never invoked on the caller's goroutine.
func (b *Batcher) Append(partitionID int, msgSize int64) error {
	if b.stopped.Load() {
		return ErrBackpressure
	}
	// Check backpressure before buffering. The allowance scales with the
	// number of active partitions so that high-partition topics are not
	// spuriously rejected when each partition holds up to MaxSize buffered.
	if b.cfg.HighWaterMark > 0 {
		hwm := b.cfg.HighWaterMark * int64(b.activePartitions())
		if hwm > 0 && b.totalSize.Load()+msgSize > hwm {
			return ErrBackpressure
		}
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
				b.scheduleFlush(partitionID)
			})
		} else {
			buf.timer.Reset(b.cfg.MaxAge)
		}
	}
	buf.mu.Unlock()

	if shouldFlush {
		b.scheduleFlush(partitionID)
	}
	return nil
}

// scheduleFlush starts a flush in the background unless one is already running
// for this partition.
func (b *Batcher) scheduleFlush(partitionID int) {
	b.stopMu.Lock()
	defer b.stopMu.Unlock()
	if b.stopped.Load() {
		return
	}
	b.mu.Lock()
	buf, ok := b.buffers[partitionID]
	b.mu.Unlock()
	if !ok {
		return
	}

	buf.mu.Lock()
	if buf.count == 0 || buf.flushing {
		buf.mu.Unlock()
		return
	}
	buf.flushing = true
	buf.mu.Unlock()

	b.flushWG.Add(1)
	go func() {
		defer b.flushWG.Done()
		_ = b.flushPartition(partitionID)
	}()
}

// flushPartition drains a partition's metadata and calls OnFlush outside the
// buffer lock. This lets producers continue appending while a segment upload is
// in progress. Callers must mark buf.flushing before invoking it.
func (b *Batcher) flushPartition(partitionID int) error {
	b.mu.Lock()
	buf, ok := b.buffers[partitionID]
	b.mu.Unlock()
	if !ok {
		return nil
	}

	buf.mu.Lock()
	if buf.count == 0 {
		buf.flushing = false
		buf.mu.Unlock()
		return nil
	}

	flushedCount := buf.count
	flushedSize := buf.size
	buf.count = 0
	buf.size = 0
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	buf.mu.Unlock()

	var err error
	if b.cfg.OnFlush != nil {
		err = b.cfg.OnFlush(partitionID)
	}

	buf.mu.Lock()
	buf.flushing = false
	if err != nil {
		// Preserve the failed work so it is retried with any bytes appended while
		// the flush was in flight.
		buf.count += flushedCount
		buf.size += flushedSize
	}
	if err == nil {
		b.totalSize.Add(-flushedSize)
	}
	shouldFlush := err == nil && buf.count > 0 && buf.size >= b.cfg.MaxSize
	if !shouldFlush && buf.count > 0 && !b.stopped.Load() {
		if buf.timer == nil {
			buf.timer = time.AfterFunc(b.cfg.MaxAge, func() {
				b.scheduleFlush(partitionID)
			})
		} else {
			buf.timer.Reset(b.cfg.MaxAge)
		}
	}
	buf.mu.Unlock()

	if shouldFlush {
		b.scheduleFlush(partitionID)
	}
	return err
}

// Flush manually flushes the buffer for partitionID.
func (b *Batcher) Flush(partitionID int) error {
	b.mu.Lock()
	buf, ok := b.buffers[partitionID]
	b.mu.Unlock()
	if !ok {
		return nil
	}
	buf.mu.Lock()
	if buf.count == 0 || buf.flushing {
		buf.mu.Unlock()
		return nil
	}
	buf.flushing = true
	buf.mu.Unlock()
	return b.flushPartition(partitionID)
}

// Stop flushes all remaining partition buffers and stops all timers.
func (b *Batcher) Stop() {
	b.stopMu.Lock()
	b.stopped.Store(true)
	b.stopMu.Unlock()
	b.mu.Lock()
	ids := make([]int, 0, len(b.buffers))
	for id := range b.buffers {
		ids = append(ids, id)
	}
	b.mu.Unlock()

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

	b.flushWG.Wait()
	for _, id := range ids {
		_ = b.Flush(id)
	}
}
