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
	active    atomic.Int64 // number of partitions with buffered (unflushed) bytes
	counterMu sync.Mutex   // guards consistent read/update of totalSize+active together
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

// activePartitions returns the number of partitions that currently have
// buffered (unflushed) data. Once a partition's flush completes it stops
// contributing, so the allowance reflects live load rather than the full
// history of partitions ever touched.
func (b *Batcher) activePartitions() int {
	return int(b.active.Load())
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

	buf := b.getOrCreate(partitionID)

	buf.mu.Lock()
	// Check backpressure while holding the target partition's buffer lock so
	// that the active-count snapshot and the "becoming active" determination are
	// consistent with respect to this partition's own activation. The active
	// counter is updated under buf.mu, so reading it here is race-free w.r.t.
	// this partition; other partitions only increment it (never under-counting).
	//
	// The active count and totalSize must be read and updated as one
	// synchronized snapshot: a concurrent flush completion decrements both
	// together, and reading them separately lets this append see a stale larger
	// active with a newer smaller total (admitting up to an extra HWM).
	b.counterMu.Lock()
	if b.cfg.HighWaterMark > 0 {
		active := int(b.active.Load())
		// becomingActive is true when this partition is not yet contributing to
		// active (nothing buffered and no flush in flight). It must be counted
		// in the allowance now because this append will make it active.
		becomingActive := buf.count == 0 && !buf.flushing
		if becomingActive {
			active++
		}
		hwm := b.cfg.HighWaterMark * int64(active)
		if b.totalSize.Load()+msgSize > hwm {
			b.counterMu.Unlock()
			buf.mu.Unlock()
			return ErrBackpressure
		}
	}

	buf.count++
	// Only account the partition as becoming active when it was neither
	// buffered nor already in flight (flushing). During a flush the partition
	// is still counted in active because its bytes remain in totalSize.
	if buf.count == 1 && !buf.flushing {
		b.active.Add(1)
	}
	buf.size += msgSize
	b.totalSize.Add(msgSize)
	b.counterMu.Unlock()

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
	// NOTE: active is NOT decremented here. The flushed bytes remain in
	// totalSize until OnFlush succeeds, so the partition must continue to
	// count toward the backpressure allowance while the flush is in flight.
	// active is adjusted together with totalSize below.
	buf.mu.Unlock()

	var err error
	if b.cfg.OnFlush != nil {
		err = b.cfg.OnFlush(partitionID)
	}

	buf.mu.Lock()
	buf.flushing = false
	if err != nil {
		// Preserve the failed work so it is retried with any bytes appended while
		// the flush was in flight. The bytes never left totalSize and the
		// partition was never un-counted, so active stays unchanged.
		buf.count += flushedCount
		buf.size += flushedSize
	}
	if err == nil {
		// Decrement totalSize and active together under counterMu so a
		// concurrent Append cannot observe a stale larger active paired with a
		// newer smaller total (which would admit over the bound).
		b.counterMu.Lock()
		b.totalSize.Add(-flushedSize)
		// The flushed bytes have left totalSize. If no bytes arrived during the
		// flush the partition no longer contributes and must be un-counted; if
		// bytes did arrive they keep the partition active.
		if buf.count == 0 {
			b.active.Add(-1)
		}
		b.counterMu.Unlock()
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
