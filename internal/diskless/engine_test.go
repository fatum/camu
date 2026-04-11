package diskless

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
)

func newTestEngine(t *testing.T, lingerMs int, maxBatchBytes int64) *Engine {
	t.Helper()
	s3 := testS3Client(t)
	meta := NewMemoryMetaStore()
	return NewEngine(s3, meta, "test-node", EngineConfig{
		LingerMs:      lingerMs,
		MaxBatchBytes: maxBatchBytes,
	})
}

func TestEngine_ProduceAndConsume(t *testing.T) {
	e := newTestEngine(t, 50, 8<<20)
	defer e.Close()

	batch := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
	})

	ctx := context.Background()
	result, err := e.Produce(ctx, "t1", 0, batch)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if result.BaseOffset != 0 {
		t.Fatalf("expected BaseOffset=0, got %d", result.BaseOffset)
	}

	data, hw, err := e.Fetch(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if hw != 1 {
		t.Fatalf("expected hw=1, got %d", hw)
	}
	if data == nil {
		t.Fatal("expected non-nil data")
	}
}

func TestEngine_ProduceMultipleBatchesMergedInSingleFlush(t *testing.T) {
	e := newTestEngine(t, 200, 8<<20)
	defer e.Close()

	batch1 := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
	})
	batch2 := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k2"), Value: []byte("v2")},
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	var r1, r2 FlushResult
	var e1, e2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		r1, e1 = e.Produce(ctx, "t1", 0, batch1)
	}()
	go func() {
		defer wg.Done()
		r2, e2 = e.Produce(ctx, "t1", 1, batch2)
	}()
	wg.Wait()

	if e1 != nil {
		t.Fatalf("produce1: %v", e1)
	}
	if e2 != nil {
		t.Fatalf("produce2: %v", e2)
	}
	if r1.BaseOffset != 0 {
		t.Fatalf("expected r1.BaseOffset=0, got %d", r1.BaseOffset)
	}
	if r2.BaseOffset != 0 {
		t.Fatalf("expected r2.BaseOffset=0, got %d", r2.BaseOffset)
	}

	// Both partitions should be readable.
	data1, _, err := e.Fetch(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch p0: %v", err)
	}
	if data1 == nil {
		t.Fatal("expected non-nil data for p0")
	}

	data2, _, err := e.Fetch(ctx, "t1", 1, 0, 1<<20)
	if err != nil {
		t.Fatalf("fetch p1: %v", err)
	}
	if data2 == nil {
		t.Fatal("expected non-nil data for p1")
	}
}

func TestEngine_FlushTriggeredByLingerTimeout(t *testing.T) {
	e := newTestEngine(t, 50, 8<<20)
	defer e.Close()

	batch := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
	})

	ctx := context.Background()
	start := time.Now()
	result, err := e.Produce(ctx, "t1", 0, batch)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if result.BaseOffset != 0 {
		t.Fatalf("expected BaseOffset=0, got %d", result.BaseOffset)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected linger wait >=40ms, got %v", elapsed)
	}
}
