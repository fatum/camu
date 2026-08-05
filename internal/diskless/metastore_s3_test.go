package diskless

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func newTestS3MetaStore(t *testing.T) *S3MetaStore {
	t.Helper()
	s3, err := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("create s3 client: %v", err)
	}
	return NewS3MetaStore(s3)
}

func TestS3MetaStore_AllocateAndRegister(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	results, err := m.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "t1", Partition: 0, Count: 2},
		{Topic: "t1", Partition: 1, Count: 3},
	})
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if results[0].BaseOffset != 0 || results[1].BaseOffset != 0 {
		t.Fatalf("first allocation bases = %+v, want both 0", results)
	}

	head, err := m.GetPartitionHead(ctx, "t1", 0)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head != 2 {
		t.Fatalf("head = %d, want 2", head)
	}

	now := time.Now()
	if err := m.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "_diskless/n1/1.data",
		Batches: []BatchRef{
			{Topic: "t1", Partition: 0, BaseOffset: 0, EndOffset: 2, ByteOffset: 0, ByteLength: 10},
			{Topic: "t1", Partition: 1, BaseOffset: 0, EndOffset: 3, ByteOffset: 10, ByteLength: 20},
		},
		CreatedAt: now,
		SizeBytes: 30,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	refs, err := m.QuerySegments(ctx, "t1", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 1 || refs[0].EndOffset != 2 || refs[0].FileKey != "_diskless/n1/1.data" {
		t.Fatalf("unexpected refs: %+v", refs)
	}

	start, err := m.GetPartitionStart(ctx, "t1", 0)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if start != 0 {
		t.Fatalf("start = %d, want 0", start)
	}
}

// TestS3MetaStore_ConcurrentAllocation verifies the CAS loop: concurrent
// allocations to the same partition yield disjoint, contiguous, non-overlapping
// ranges covering the whole space.
func TestS3MetaStore_ConcurrentAllocation(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	const (
		workers   = 8
		perWorker = 10
		count     = 3
	)
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		ranges []struct{ base, end int64 }
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				res, err := m.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: count}})
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				base := res[0].BaseOffset
				mu.Lock()
				ranges = append(ranges, struct{ base, end int64 }{base, base + count})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	total := workers * perWorker * count
	if len(ranges) != workers*perWorker {
		t.Fatalf("allocated %d ranges, want %d", len(ranges), workers*perWorker)
	}
	seen := make([]bool, total)
	for _, r := range ranges {
		for o := r.base; o < r.end; o++ {
			if seen[o] {
				t.Fatalf("offset %d allocated twice", o)
			}
			seen[o] = true
		}
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("offset %d never allocated (gap)", i)
		}
	}
	head, err := m.GetPartitionHead(ctx, "t", 0)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head != int64(total) {
		t.Fatalf("head = %d, want %d", head, total)
	}
}

func TestS3MetaStore_RegisterIsIdempotent(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	seg := SegmentRecord{
		FileKey:   "_diskless/n1/1.data",
		Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: 0, EndOffset: 2, ByteLength: 10}},
		CreatedAt: time.Now(),
	}
	if err := m.RegisterSegment(ctx, seg); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// A retry of the same deterministic ref must not conflict.
	if err := m.RegisterSegment(ctx, seg); err != nil {
		t.Fatalf("retry register: %v", err)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %d, want 1 (no duplicates from idempotent register)", len(refs))
	}
}

func TestS3MetaStore_Retention(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	old := time.Now().Add(-time.Hour)
	recent := time.Now()
	if err := m.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "old.data",
		Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: 0, EndOffset: 2, ByteLength: 10}},
		CreatedAt: old,
	}); err != nil {
		t.Fatalf("register old: %v", err)
	}
	if err := m.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "recent.data",
		Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: 2, EndOffset: 4, ByteLength: 10}},
		CreatedAt: recent,
	}); err != nil {
		t.Fatalf("register recent: %v", err)
	}

	// The "old" file has only an expired ref within partition 0, but the shared
	// catalog is per-partition; plan for partition 0 should exclude recent.data.
	expired, err := m.PlanExpiredFileDeletes(ctx, "t", 0, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(expired) != 1 || expired[0] != "old.data" {
		t.Fatalf("expired = %v, want [old.data]", expired)
	}

	if err := m.DeleteFileRefs(ctx, "old.data"); err != nil {
		t.Fatalf("delete refs: %v", err)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 1 || refs[0].FileKey != "recent.data" {
		t.Fatalf("refs after delete = %+v, want only recent.data", refs)
	}
}

func TestS3MetaStore_DeleteTopic(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	if _, err := m.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 2}}); err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if err := m.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "f.data",
		Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: 0, EndOffset: 2, ByteLength: 10}},
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := m.DeleteTopic(ctx, "t"); err != nil {
		t.Fatalf("delete topic: %v", err)
	}

	head, err := m.GetPartitionHead(ctx, "t", 0)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head != 0 {
		t.Fatalf("head after delete = %d, want 0", head)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("refs after delete = %d, want 0", len(refs))
	}
}
