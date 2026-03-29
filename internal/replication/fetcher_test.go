package replication

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
)

// mockPartitionManager records calls made by the fetcher.
type mockPartitionManager struct {
	mu             sync.Mutex
	appended       []log.BatchFrame
	truncated      []uint64
	highWatermarks []uint64
	flushedOffsets []uint64
}

func (m *mockPartitionManager) AppendReplicatedBatchFrames(_ context.Context, _ string, _ int, frames []log.BatchFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, frame := range frames {
		cp := make([]byte, len(frame.Data))
		copy(cp, frame.Data)
		m.appended = append(m.appended, log.BatchFrame{
			Data: cp,
			Meta: frame.Meta,
		})
	}
	return nil
}

func (m *mockPartitionManager) TruncateWAL(_ string, _ int, beforeOffset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.truncated = append(m.truncated, beforeOffset)
	return nil
}

func (m *mockPartitionManager) UpdateFollowerProgress(_ string, _ int, highWatermark, flushedOffset uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.highWatermarks = append(m.highWatermarks, highWatermark)
	m.flushedOffsets = append(m.flushedOffsets, flushedOffset)
}

func (m *mockPartitionManager) appendedFrames() []log.BatchFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appended
}

func (m *mockPartitionManager) truncatedOffsets() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.truncated
}

func (m *mockPartitionManager) progress() ([]uint64, []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.highWatermarks, m.flushedOffsets
}

// buildFrameBody encodes messages into the wire format used by the replication
// endpoint.
func buildFrameBody(t *testing.T, msgs []log.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteMessageFrames(&buf, msgs); err != nil {
		t.Fatalf("WriteMessageFrames: %v", err)
	}
	return buf.Bytes()
}

func TestFollowerFetcher_Basic(t *testing.T) {
	msgs := []log.Message{
		{Offset: 0, Value: []byte("hello")},
		{Offset: 1, Value: []byte("world")},
	}
	body := buildFrameBody(t, msgs)

	// served is used to ensure we only send messages once; subsequent requests
	// block until the context is cancelled so the loop stays alive long enough
	// for the test to observe the first batch.
	served := false
	var servedMu sync.Mutex
	doneCh := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servedMu.Lock()
		first := !served
		served = true
		servedMu.Unlock()

		if first {
			w.Header().Set("X-High-Watermark", "2")
			w.Header().Set("X-Leader-Epoch", "1")
			w.Header().Set("X-Flushed-Offset", "0")
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			return
		}
		// Subsequent calls: signal done and block until client disconnects.
		select {
		case doneCh <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetcher := NewFollowerFetcher(&http.Client{Timeout: 10 * time.Second}, nil)

	go func() {
		fetcher.Run(ctx, "test-topic", 0, srv.Listener.Addr().String(), 0, 1, "test-node", pm)
	}()

	// Wait until the second request signals us (meaning the first was processed).
	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for fetch to complete")
	}
	cancel()

	appended := pm.appendedFrames()
	if len(appended) == 0 {
		t.Fatal("expected at least one AppendReplicatedBatch call, got none")
	}
	frame := appended[0]
	if frame.Meta.MessageCount != 2 {
		t.Fatalf("expected 2 messages, got %d", frame.Meta.MessageCount)
	}
	if frame.Meta.FirstOffset != 0 {
		t.Errorf("unexpected first offset: %d", frame.Meta.FirstOffset)
	}
	if frame.Meta.LastOffset != 1 {
		t.Errorf("unexpected last offset: %d", frame.Meta.LastOffset)
	}
	hws, flushed := pm.progress()
	if len(hws) == 0 || hws[0] != 2 {
		t.Fatalf("expected follower progress with high watermark 2, got %v", hws)
	}
	if len(flushed) == 0 || flushed[0] != 0 {
		t.Fatalf("expected follower progress with flushed offset 0, got %v", flushed)
	}
}

func TestFollowerFetcher_Truncation(t *testing.T) {
	served := false
	var servedMu sync.Mutex
	doneCh := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servedMu.Lock()
		first := !served
		served = true
		servedMu.Unlock()

		if first {
			// Signal divergence: ask follower to truncate to offset 5.
			w.Header().Set("X-Truncate-To", "5")
			w.Header().Set("X-High-Watermark", "10")
			w.Header().Set("X-Leader-Epoch", "2")
			w.WriteHeader(http.StatusOK)
			return
		}
		select {
		case doneCh <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetcher := NewFollowerFetcher(&http.Client{Timeout: 10 * time.Second}, nil)

	go func() {
		fetcher.Run(ctx, "test-topic", 0, srv.Listener.Addr().String(), 10, 1, "test-node", pm)
	}()

	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for truncation fetch to complete")
	}
	cancel()

	truncated := pm.truncatedOffsets()
	if len(truncated) == 0 {
		t.Fatal("expected TruncateWAL to be called, but it was not")
	}
	if truncated[0] != 5 {
		t.Errorf("expected TruncateWAL(5), got TruncateWAL(%d)", truncated[0])
	}
	hws, _ := pm.progress()
	if len(hws) == 0 || hws[0] != 10 {
		t.Fatalf("expected follower progress with high watermark 10, got %v", hws)
	}
}

func TestFollowerFetcher_TruncationToZeroAdvancesEpoch(t *testing.T) {
	requestEpochs := make(chan string, 2)
	requestOffsets := make(chan string, 2)
	doneCh := make(chan struct{})
	var requests int
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		reqNum := requests
		mu.Unlock()

		requestEpochs <- r.Header.Get("X-Replica-Epoch")
		requestOffsets <- r.Header.Get("X-Replica-Offset")

		if reqNum == 1 {
			w.Header().Set("X-Truncate-To", "0")
			w.Header().Set("X-High-Watermark", "0")
			w.Header().Set("X-Leader-Epoch", "2")
			w.WriteHeader(http.StatusOK)
			return
		}

		select {
		case doneCh <- struct{}{}:
		default:
		}
		w.Header().Set("X-High-Watermark", "0")
		w.Header().Set("X-Leader-Epoch", "2")
		w.WriteHeader(http.StatusOK)
		<-r.Context().Done()
	}))
	defer srv.Close()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetcher := NewFollowerFetcher(&http.Client{Timeout: 10 * time.Second}, nil)
	go func() {
		fetcher.Run(ctx, "test-topic", 0, srv.Listener.Addr().String(), 0, 0, "test-node", pm)
	}()

	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for second fetch")
	}
	cancel()

	truncated := pm.truncatedOffsets()
	if len(truncated) == 0 || truncated[0] != 0 {
		t.Fatalf("expected TruncateWAL(0), got %v", truncated)
	}

	firstEpoch := <-requestEpochs
	firstOffset := <-requestOffsets
	secondEpoch := <-requestEpochs
	secondOffset := <-requestOffsets
	if firstEpoch != "0" || firstOffset != "0" {
		t.Fatalf("first request headers = epoch %q offset %q, want 0/0", firstEpoch, firstOffset)
	}
	if secondEpoch != "2" || secondOffset != "0" {
		t.Fatalf("second request headers = epoch %q offset %q, want 2/0", secondEpoch, secondOffset)
	}
}
