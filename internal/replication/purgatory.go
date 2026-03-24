package replication

import (
	"errors"
	"sync"
	"time"
)

var ErrReplicationTimeout = errors.New("replication timeout")

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
	timer  *time.Timer
}

// NewPurgatory creates an empty Purgatory.
func NewPurgatory() *Purgatory {
	return &Purgatory{}
}

// Wait blocks until the high watermark advances past offset (Complete(hw) with
// hw > offset) or until timeout elapses. Returns nil on success,
// ErrReplicationTimeout on timeout.
func (p *Purgatory) Wait(offset uint64, timeout time.Duration) error {
	pw := &pendingWrite{
		offset: offset,
		doneCh: make(chan struct{}),
	}

	pw.timer = time.AfterFunc(timeout, func() {
		// Signal doneCh so the select below unblocks; the caller distinguishes
		// timeout vs completion by whether doneCh was closed by Complete/Close.
		// We use a separate timedOut flag via the timer itself — see below.
	})

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		pw.timer.Stop()
		return nil
	}
	p.pending = append(p.pending, pw)
	p.mu.Unlock()

	timeoutCh := time.After(timeout)

	select {
	case <-pw.doneCh:
		pw.timer.Stop()
		return nil
	case <-timeoutCh:
		// Remove from pending list to avoid a leak.
		p.mu.Lock()
		p.removeLocked(pw)
		p.mu.Unlock()
		pw.timer.Stop()
		return ErrReplicationTimeout
	}
}

// Complete unblocks all pending writes whose offset is strictly less than hw.
// HW is the next-to-commit offset, so hw=501 means offset 500 is committed.
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
