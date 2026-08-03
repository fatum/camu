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
	appendedRaw    [][]byte
	truncatedFrom  []uint64
	highWatermarks []uint64
	flushedOffsets []uint64
}

func (m *mockPartitionManager) AppendReplicatedRawBatch(_ context.Context, _ string, _ int, batch []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(batch))
	copy(cp, batch)
	m.appendedRaw = append(m.appendedRaw, cp)
	return nil
}

func (m *mockPartitionManager) TruncateLogFrom(_ string, _ int, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.truncatedFrom = append(m.truncatedFrom, offset)
	return nil
}

func (m *mockPartitionManager) SyncFollowerSealedPrefix(_ context.Context, _ string, _ int, _ uint64) uint64 {
	return 0
}

func (m *mockPartitionManager) UpdateFollowerProgress(_ string, _ int, _ uint64, highWatermark, flushedOffset uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.highWatermarks = append(m.highWatermarks, highWatermark)
	m.flushedOffsets = append(m.flushedOffsets, flushedOffset)
}

func (m *mockPartitionManager) appendedRawBatches() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendedRaw
}

func (m *mockPartitionManager) truncatedOffsets() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.truncatedFrom
}

func (m *mockPartitionManager) progress() ([]uint64, []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.highWatermarks, m.flushedOffsets
}

func TestReadReplicaBatchesStreamsOneBatchAtATime(t *testing.T) {
	first := log.EncodeRecordBatch(0, []log.Message{{Offset: 0, Value: []byte("first")}})
	second := log.EncodeRecordBatch(1, []log.Message{{Offset: 1, Value: []byte("second")}})

	var got [][]byte
	err := readReplicaBatches(bytes.NewReader(append(first, second...)), 0, func(batch []byte, header log.RecordBatchHeader) error {
		if len(got) == 0 && header.FirstOffset != 0 {
			t.Fatalf("first callback offset = %d, want 0", header.FirstOffset)
		}
		if len(got) == 1 && header.FirstOffset != 1 {
			t.Fatalf("second callback offset = %d, want 1", header.FirstOffset)
		}
		got = append(got, append([]byte(nil), batch...))
		return nil
	})
	if err != nil {
		t.Fatalf("readReplicaBatches() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("callback count = %d, want 2", len(got))
	}
	if !bytes.Equal(got[0], first) || !bytes.Equal(got[1], second) {
		t.Fatal("streamed batches differ from the input")
	}
}

func TestReadReplicaBatchesRejectsTruncatedBatch(t *testing.T) {
	batch := log.EncodeRecordBatch(0, []log.Message{{Offset: 0, Value: []byte("value")}})
	called := false
	err := readReplicaBatches(bytes.NewReader(batch[:len(batch)-1]), 0, func([]byte, log.RecordBatchHeader) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("readReplicaBatches() error = nil, want truncated-body error")
	}
	if called {
		t.Fatal("callback was called for a truncated batch")
	}
}

func TestFollowerFetcher_Basic(t *testing.T) {
	body := log.EncodeRecordBatch(0, []log.Message{
		{Offset: 0, Value: []byte("hello")},
		{Offset: 1, Value: []byte("world")},
	})

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

	appended := pm.appendedRawBatches()
	if len(appended) != 1 {
		t.Fatalf("expected one raw batch append, got %d", len(appended))
	}
	hdr, err := log.ReadRecordBatchHeader(appended[0])
	if err != nil {
		t.Fatalf("ReadRecordBatchHeader() error = %v", err)
	}
	if hdr.FirstOffset != 0 || hdr.LastOffset() != 1 {
		t.Fatalf("unexpected fetched offsets %d-%d", hdr.FirstOffset, hdr.LastOffset())
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
		t.Fatal("expected TruncateLogFrom to be called, but it was not")
	}
	if truncated[0] != 5 {
		t.Errorf("expected TruncateLogFrom(5), got TruncateLogFrom(%d)", truncated[0])
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
		t.Fatalf("expected TruncateLogFrom(0), got %v", truncated)
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

func TestFollowerFetcher_AppliesRawRecordBatches(t *testing.T) {
	raw := log.EncodeRecordBatch(10, []log.Message{
		{Offset: 10, Value: []byte("hello")},
		{Offset: 11, Value: []byte("world")},
	})

	served := false
	var servedMu sync.Mutex
	doneCh := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servedMu.Lock()
		first := !served
		served = true
		servedMu.Unlock()

		if first {
			w.Header().Set("X-High-Watermark", "12")
			w.Header().Set("X-Leader-Epoch", "1")
			w.Header().Set("X-Flushed-Offset", "0")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(raw)
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
		t.Fatal("timed out waiting for raw fetch to complete")
	}
	cancel()

	appended := pm.appendedRawBatches()
	if len(appended) != 1 {
		t.Fatalf("expected 1 raw batch append, got %d", len(appended))
	}
	if !bytes.Equal(appended[0], raw) {
		t.Fatal("raw batch bytes changed during follower fetch/apply")
	}
}
