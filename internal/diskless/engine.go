package diskless

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// EngineConfig controls Engine flush behavior.
type EngineConfig struct {
	LingerMs      int
	MaxBatchBytes int64
}

// Engine ties Buffer + Writer + Reader together with a linger timer.
type Engine struct {
	writer    *Writer
	reader    *Reader
	buf       *Buffer
	cfg       EngineConfig
	flushMu   sync.Mutex
	lingerRst chan struct{} // signals linger timer to start
	stop      chan struct{}
	wg        sync.WaitGroup
}

// NewEngine creates an Engine that flushes to s3 via meta, identified by nodeID.
func NewEngine(s3 *storage.S3Client, meta MetaStore, nodeID string, cfg EngineConfig) *Engine {
	e := &Engine{
		writer:    NewWriter(s3, meta, nodeID),
		reader:    NewReader(s3, meta),
		buf:       NewBuffer(cfg.MaxBatchBytes),
		cfg:       cfg,
		lingerRst: make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	e.wg.Add(1)
	go e.lingerLoop()
	return e
}

// Produce appends a batch to the buffer and blocks until flush completes.
// Callee takes ownership of batch — caller must not mutate it after this call.
func (e *Engine) Produce(ctx context.Context, topic string, partition int, batch []byte) (FlushResult, error) {
	done := make(chan FlushResult, 1)
	e.buf.Append(BufferEntry{
		Topic:     topic,
		Partition: partition,
		Batch:     batch,
		Done:      done,
	})

	// Signal linger timer (non-blocking).
	select {
	case e.lingerRst <- struct{}{}:
	default:
	}

	// Flush immediately if buffer exceeds threshold.
	if e.buf.ShouldFlush() {
		e.triggerFlush()
	}

	// Wait for result or context cancellation.
	select {
	case r := <-done:
		return r, r.Err
	case <-ctx.Done():
		return FlushResult{}, ctx.Err()
	}
}

// Fetch delegates to the underlying Reader.
func (e *Engine) Fetch(ctx context.Context, topic string, partition int, fromOffset int64, maxBytes int) ([]byte, int64, error) {
	return e.reader.Fetch(ctx, topic, partition, fromOffset, maxBytes)
}

// Close stops the linger loop and performs a final flush.
func (e *Engine) Close() {
	close(e.stop)
	e.wg.Wait()
}

func (e *Engine) lingerLoop() {
	defer e.wg.Done()
	linger := time.Duration(e.cfg.LingerMs) * time.Millisecond
	timer := time.NewTimer(linger)
	timer.Stop()
	defer timer.Stop()

	for {
		// Wait for a signal or stop.
		select {
		case <-e.lingerRst:
		case <-e.stop:
			e.triggerFlush()
			return
		}

		// Reset timer for linger duration.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(linger)

		select {
		case <-timer.C:
			e.triggerFlush()
		case <-e.stop:
			e.triggerFlush()
			return
		}
	}
}

func (e *Engine) triggerFlush() {
	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	entries := e.buf.Drain()
	if len(entries) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := e.writer.Flush(ctx, entries); err != nil {
		slog.Error("diskless_flush_error", "error", err)
	}
}
