package replication

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrReplicationTimeout = errors.New("replication timeout")
var ErrPurgatoryClosed = errors.New("purgatory closed")

// Purgatory holds pending produce requests waiting for the high watermark to
// advance (i.e. ISR followers to acknowledge).
type Purgatory struct {
	mu      sync.Mutex
	pending []*pendingWrite
	closed  bool
}

type pendingWrite struct {
	offset uint64
	doneCh chan struct{}
}

// NewPurgatory creates an empty Purgatory.
func NewPurgatory() *Purgatory {
	return &Purgatory{}
}

// Wait blocks until the high watermark advances past offset (Complete(hw) with
// hw > offset), until timeout elapses, or until ctx is cancelled.
// Returns nil on success, ErrReplicationTimeout on timeout, or ctx.Err() on
// cancellation.
func (p *Purgatory) Wait(ctx context.Context, offset uint64, timeout time.Duration) error {
	pw := &pendingWrite{
		offset: offset,
		doneCh: make(chan struct{}),
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPurgatoryClosed
	}
	p.pending = append(p.pending, pw)
	p.mu.Unlock()

	timeoutCh := time.After(timeout)

	select {
	case <-pw.doneCh:
		return nil
	case <-timeoutCh:
		p.mu.Lock()
		p.removeLocked(pw)
		p.mu.Unlock()
		return ErrReplicationTimeout
	case <-ctx.Done():
		p.mu.Lock()
		p.removeLocked(pw)
		p.mu.Unlock()
		return ctx.Err()
	}
}

// Complete unblocks all pending writes whose offset is strictly less than hw.
// HW is the next-to-commit offset, so hw=501 means offset 500 is committed.
//
// Safe to call multiple times: once a waiter is unblocked (channel closed), it's
// removed from the pending list, so subsequent Complete calls won't try to close
// it again. Additionally, waiters removed via timeout are also removed before
// Complete can close them.
func (p *Purgatory) Complete(hw uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	remaining := p.pending[:0]
	for _, pw := range p.pending {
		if pw.offset < hw {
			close(pw.doneCh)
		} else {
			remaining = append(remaining, pw)
		}
	}
	p.pending = remaining
}

// Close marks the purgatory as closed and unblocks all pending waiters.
func (p *Purgatory) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	for _, pw := range p.pending {
		close(pw.doneCh)
	}
	p.pending = nil
}

// removeLocked removes pw from p.pending. Must be called with p.mu held.
func (p *Purgatory) removeLocked(pw *pendingWrite) {
	for i, entry := range p.pending {
		if entry == pw {
			p.pending = append(p.pending[:i], p.pending[i+1:]...)
			return
		}
	}
}
