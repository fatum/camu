package diskless

import "sync"

// BufferEntry represents a single RecordBatch waiting to be flushed.
type BufferEntry struct {
	Topic     string
	Partition int
	Batch     []byte          // raw RecordBatch bytes
	Done      chan FlushResult // signaled when flush completes
}

// FlushResult is sent on the Done channel after a flush attempt.
type FlushResult struct {
	BaseOffset int64
	Duplicate  bool // true when the batch was an idempotent retry of an allocated range
	Err        error
}

// Buffer accumulates RecordBatch entries until a size threshold is reached.
type Buffer struct {
	mu            sync.Mutex
	entries       []BufferEntry
	size          int64
	maxBatchBytes int64
}

// NewBuffer creates a Buffer that signals flush readiness at maxBatchBytes.
func NewBuffer(maxBatchBytes int64) *Buffer {
	return &Buffer{maxBatchBytes: maxBatchBytes}
}

// Append adds an entry to the buffer, updates the accumulated size, and reports
// whether the flush threshold has been crossed.
func (b *Buffer) Append(entry BufferEntry) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, entry)
	b.size += int64(len(entry.Batch))
	return b.size >= b.maxBatchBytes
}

// Drain atomically removes and returns all entries, resetting the buffer.
func (b *Buffer) Drain() []BufferEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	entries := b.entries
	b.entries = nil
	b.size = 0
	return entries
}

// ShouldFlush returns true if accumulated size >= maxBatchBytes.
func (b *Buffer) ShouldFlush() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size >= b.maxBatchBytes
}

// Len returns the number of buffered entries.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

// Size returns the total byte size of buffered entries.
func (b *Buffer) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}
