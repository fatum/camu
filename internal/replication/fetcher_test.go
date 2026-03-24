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
	mu       sync.Mutex
	appended [][]log.Message
	truncated []uint64
}

func (m *mockPartitionManager) AppendReplicatedBatch(_ context.Context, _ string, _ int, msgs []log.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]log.Message, len(msgs))
	copy(cp, msgs)
	m.appended = append(m.appended, cp)
	return nil
}

func (m *mockPartitionManager) TruncateWAL(_ string, _ int, beforeOffset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.truncated = append(m.truncated, beforeOffset)
	return nil
}

func (m *mockPartitionManager) appendedMessages() [][]log.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appended
}

func (m *mockPartitionManager) truncatedOffsets() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.truncated
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

	fetcher := NewFollowerFetcher(nil)

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

	appended := pm.appendedMessages()
	if len(appended) == 0 {
		t.Fatal("expected at least one AppendReplicatedBatch call, got none")
	}
	batch := appended[0]
	if len(batch) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(batch))
	}
	if batch[0].Offset != 0 || string(batch[0].Value) != "hello" {
		t.Errorf("unexpected first message: %+v", batch[0])
	}
	if batch[1].Offset != 1 || string(batch[1].Value) != "world" {
		t.Errorf("unexpected second message: %+v", batch[1])
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

	fetcher := NewFollowerFetcher(nil)

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
}
