package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
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

// startReplicationTestServer starts a TCP server that speaks the replication
// wire protocol. The handler is called for each request; the response it
// returns is written back to the follower. The server blocks on reading the
// next request after each response, so tests can control the flow by choosing
// when to respond.
func startReplicationTestServer(t *testing.T, handler func(req *ReplicaFetchRequest) *ReplicaFetchResponse) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var conns sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns.Add(1)
			go func(conn net.Conn) {
				defer conns.Done()
				defer conn.Close()
				for {
					req, err := DecodeRequest(conn)
					if err != nil {
						return
					}
					resp := handler(req)
					writeTestResponse(t, conn, resp)
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		ln.Close()
		conns.Wait()
	}
}

func writeTestResponse(t *testing.T, conn net.Conn, resp *ReplicaFetchResponse) {
	t.Helper()
	header := EncodeResponseHeader(resp)
	totalPayload := len(header) + len(resp.BatchData)
	var frameLen [4]byte
	binary.BigEndian.PutUint32(frameLen[:], uint32(totalPayload))
	if len(resp.BatchData) > 0 {
		if _, err := (&net.Buffers{frameLen[:], header, resp.BatchData}).WriteTo(conn); err != nil {
			return
		}
	} else {
		if _, err := (&net.Buffers{frameLen[:], header}).WriteTo(conn); err != nil {
			return
		}
	}
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

	served := false
	var servedMu sync.Mutex
	doneCh := make(chan struct{})

	addr, cleanup := startReplicationTestServer(t, func(req *ReplicaFetchRequest) *ReplicaFetchResponse {
		servedMu.Lock()
		first := !served
		served = true
		servedMu.Unlock()

		if first {
			return &ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ReplicaErrOK,
				HighWatermark: 2,
				LeaderEpoch:   1,
				FlushedOffset: 0,
				BatchData:     body,
			}
		}
		select {
		case doneCh <- struct{}{}:
		default:
		}
		// Block by sleeping; the test will cancel the context.
		time.Sleep(10 * time.Second)
		return &ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     ReplicaErrOK,
			HighWatermark: 2,
			LeaderEpoch:   1,
		}
	})
	defer cleanup()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetcher := NewFollowerFetcher(nil)

	go func() {
		fetcher.Run(ctx, "test-topic", 0, addr, 0, 1, "test-node", pm)
	}()

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

func TestFollowerFetcher_CaughtUpDoesNotBusyLoop(t *testing.T) {
	var requests atomic.Int64
	addr, cleanup := startReplicationTestServer(t, func(req *ReplicaFetchRequest) *ReplicaFetchResponse {
		requests.Add(1)
		return &ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     ReplicaErrOK,
			HighWatermark: 0,
			LeaderEpoch:   1,
		}
	})
	defer cleanup()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		NewFollowerFetcher(nil).Run(
			ctx, "test-topic", 0, addr, 0, 1, "test-node", pm,
		)
	}()
	<-done

	if got := requests.Load(); got > 4 {
		t.Fatalf("caught-up follower made %d fetches in 250ms; want bounded polling", got)
	}
}

func TestFollowerFetcher_Truncation(t *testing.T) {
	served := false
	var servedMu sync.Mutex
	doneCh := make(chan struct{})

	addr, cleanup := startReplicationTestServer(t, func(req *ReplicaFetchRequest) *ReplicaFetchResponse {
		servedMu.Lock()
		first := !served
		served = true
		servedMu.Unlock()

		if first {
			return &ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ReplicaErrTruncate,
				TruncateTo:    5,
				LeaderEpoch:   2,
				HighWatermark: 10,
			}
		}
		select {
		case doneCh <- struct{}{}:
		default:
		}
		time.Sleep(10 * time.Second)
		return &ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     ReplicaErrOK,
			HighWatermark: 10,
			LeaderEpoch:   2,
		}
	})
	defer cleanup()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetcher := NewFollowerFetcher(nil)

	go func() {
		fetcher.Run(ctx, "test-topic", 0, addr, 10, 1, "test-node", pm)
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

func TestFollowerFetcher_TruncationAdoptsEpochAtBoundary(t *testing.T) {
	requestEpochs := make(chan uint64, 2)
	requestOffsets := make(chan uint64, 2)
	doneCh := make(chan struct{})
	var requests int
	var mu sync.Mutex

	addr, cleanup := startReplicationTestServer(t, func(req *ReplicaFetchRequest) *ReplicaFetchResponse {
		mu.Lock()
		requests++
		reqNum := requests
		mu.Unlock()

		requestEpochs <- req.ReplicaEpoch
		requestOffsets <- req.ReplicaOffset

		if reqNum == 1 {
			return &ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ReplicaErrTruncate,
				TruncateTo:    10,
				LeaderEpoch:   2,
				HighWatermark: 10,
			}
		}

		select {
		case doneCh <- struct{}{}:
		default:
		}
		time.Sleep(10 * time.Second)
		return &ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     ReplicaErrOK,
			HighWatermark: 10,
			LeaderEpoch:   2,
		}
	})
	defer cleanup()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetcher := NewFollowerFetcher(nil)
	go func() {
		fetcher.Run(ctx, "test-topic", 0, addr, 20, 1, "test-node", pm)
	}()

	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for second fetch")
	}
	cancel()

	truncated := pm.truncatedOffsets()
	if len(truncated) == 0 || truncated[0] != 10 {
		t.Fatalf("expected TruncateLogFrom(10), got %v", truncated)
	}

	firstEpoch := <-requestEpochs
	firstOffset := <-requestOffsets
	secondEpoch := <-requestEpochs
	secondOffset := <-requestOffsets
	if firstEpoch != 1 || firstOffset != 20 {
		t.Fatalf("first request = epoch %d offset %d, want 1/20", firstEpoch, firstOffset)
	}
	if secondEpoch != 2 || secondOffset != 10 {
		t.Fatalf("second request = epoch %d offset %d, want 2/10", secondEpoch, secondOffset)
	}
}

func TestFollowerFetcher_TruncationCanLowerEpoch(t *testing.T) {
	requestEpochs := make(chan uint64, 2)
	doneCh := make(chan struct{})
	var requests int
	var mu sync.Mutex

	addr, cleanup := startReplicationTestServer(t, func(req *ReplicaFetchRequest) *ReplicaFetchResponse {
		mu.Lock()
		requests++
		reqNum := requests
		mu.Unlock()
		requestEpochs <- req.ReplicaEpoch
		if reqNum == 1 {
			return &ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ReplicaErrTruncate,
				TruncateTo:    0,
				LeaderEpoch:   2,
			}
		}
		select {
		case doneCh <- struct{}{}:
		default:
		}
		time.Sleep(10 * time.Second)
		return &ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     ReplicaErrOK,
			LeaderEpoch:   2,
		}
	})
	defer cleanup()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go NewFollowerFetcher(nil).Run(ctx, "topic", 0, addr, 4, 9, "node", pm)

	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for second fetch")
	}
	cancel()
	if first, second := <-requestEpochs, <-requestEpochs; first != 9 || second != 2 {
		t.Fatalf("request epochs = %d, %d, want 9, 2", first, second)
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

	addr, cleanup := startReplicationTestServer(t, func(req *ReplicaFetchRequest) *ReplicaFetchResponse {
		servedMu.Lock()
		first := !served
		served = true
		servedMu.Unlock()

		if first {
			return &ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ReplicaErrOK,
				HighWatermark: 12,
				LeaderEpoch:   1,
				FlushedOffset: 0,
				BatchData:     raw,
			}
		}
		select {
		case doneCh <- struct{}{}:
		default:
		}
		time.Sleep(10 * time.Second)
		return &ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     ReplicaErrOK,
			HighWatermark: 12,
			LeaderEpoch:   1,
		}
	})
	defer cleanup()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetcher := NewFollowerFetcher(nil)
	go func() {
		fetcher.Run(ctx, "test-topic", 0, addr, 10, 1, "test-node", pm)
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

func TestFollowerFetcher_PartitionNotReadyRetries(t *testing.T) {
	var requests atomic.Int64
	addr, cleanup := startReplicationTestServer(t, func(req *ReplicaFetchRequest) *ReplicaFetchResponse {
		requests.Add(1)
		return &ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     ReplicaErrNotFound,
		}
	})
	defer cleanup()

	pm := &mockPartitionManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		NewFollowerFetcher(nil).Run(ctx, "topic", 0, addr, 0, 1, "node", pm)
	}()
	<-done

	// Should retry without declaring leader down.
	if got := requests.Load(); got < 2 {
		t.Fatalf("expected multiple retries on not-ready, got %d", got)
	}
}
