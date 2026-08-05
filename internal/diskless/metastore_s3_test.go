package diskless

import (
	"context"
	"fmt"
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

// TestS3MetaStore_CommittedHeadConcurrentRegistration verifies the P1 scenario:
// concurrent writers flushing adjacent allocations in any order never expose a
// gap to readers. After every registration the committed head is checked: every
// offset below it must be covered by a registered ref.
func TestS3MetaStore_CommittedHeadConcurrentRegistration(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	const (
		count     = 3
		perWorker = 20
		workers   = 4
	)
	total := workers * perWorker * count

	// Pre-allocate disjoint adjacent ranges: writer w registers bases
	// w*perWorker*count, then +total, +2*total, ... so ranges from different
	// writers are adjacent and interleave.
	allBases := make([]int64, 0, workers*perWorker)
	for i := 0; i < perWorker; i++ {
		for w := 0; w < workers; w++ {
			allBases = append(allBases, int64(i*count+ w*perWorker*count))
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				base := allBases[i*workers+worker]
				seg := SegmentRecord{
					FileKey:   fmt.Sprintf("f-%d.data", base),
					Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: base, EndOffset: base + count, ByteLength: count}},
					CreatedAt: time.Now(),
				}
				if err := m.RegisterSegment(ctx, seg); err != nil {
					t.Errorf("register [%d,%d): %v", base, base+count, err)
					return
				}
				committed, err := m.GetCommittedHead(ctx, "t", 0)
				if err != nil {
					t.Errorf("committed head: %v", err)
					return
				}
				refs, err := m.partitionSegmentRefs(ctx, "t", 0)
				if err != nil {
					t.Errorf("list refs: %v", err)
					return
				}
				covered := make([]bool, committed)
				for _, r := range refs {
					for o := r.BaseOffset; o < r.EndOffset && o < committed; o++ {
						covered[o] = true
					}
				}
				for o := int64(0); o < committed; o++ {
					if !covered[o] {
						t.Errorf("committed head %d exposes uncovered offset %d (gap)", committed, o)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	committed, err := m.GetCommittedHead(ctx, "t", 0)
	if err != nil {
		t.Fatalf("final committed head: %v", err)
	}
	if committed != int64(total) {
		t.Fatalf("final committed = %d, want %d", committed, total)
	}
}

func TestS3MetaStore_IdempotentAllocation(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	alloc := OffsetAllocation{Topic: "t", Partition: 0, Count: 4, ProducerID: 42, Sequence: 100}
	first, err := m.AllocateOffsets(ctx, []OffsetAllocation{alloc})
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if first[0].BaseOffset != 0 || first[0].Duplicate {
		t.Fatalf("first allocation = %+v, want base 0 non-duplicate", first[0])
	}

	// Exact retry must be deduplicated and must not advance the counter.
	retry, err := m.AllocateOffsets(ctx, []OffsetAllocation{alloc})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !retry[0].Duplicate || retry[0].BaseOffset != 0 {
		t.Fatalf("retry = %+v, want duplicate base 0", retry[0])
	}

	// A new batch from the same producer advances the counter.
	next, err := m.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 4, ProducerID: 42, Sequence: 104}})
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	if next[0].Duplicate || next[0].BaseOffset != 4 {
		t.Fatalf("next batch = %+v, want base 4", next[0])
	}

	// An overlapping retry with a different record count is rejected.
	if _, err := m.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 5, ProducerID: 42, Sequence: 100}}); err == nil {
		t.Fatal("overlapping retry with mismatched count succeeded, want error")
	}

	// Non-idempotent allocations are unaffected.
	plain, err := m.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 2}})
	if err != nil {
		t.Fatalf("plain allocate: %v", err)
	}
	if plain[0].BaseOffset != 8 {
		t.Fatalf("plain base = %d, want 8", plain[0].BaseOffset)
	}

	head, err := m.GetPartitionHead(ctx, "t", 0)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head != 10 {
		t.Fatalf("head = %d, want 10 (4+4+2, retry not counted)", head)
	}
}

// TestS3MetaStore_CommittedHeadOnlyAdvancesContiguously verifies that the
// committed high watermark never advances past an unmaterialized range, even
// when concurrent writers register adjacent ranges out of order.
func TestS3MetaStore_CommittedHeadOnlyAdvancesContiguously(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()

	committed := func() int64 {
		t.Helper()
		h, err := m.GetCommittedHead(ctx, "t", 0)
		if err != nil {
			t.Fatalf("committed head: %v", err)
		}
		return h
	}
	register := func(base, end int64) {
		t.Helper()
		if err := m.RegisterSegment(ctx, SegmentRecord{
			FileKey:   fmt.Sprintf("f-%d-%d.data", base, end),
			Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: base, EndOffset: end, ByteLength: end - base}},
			CreatedAt: time.Now(),
		}); err != nil {
			t.Fatalf("register [%d,%d): %v", base, end, err)
		}
	}

	// A later range registering first must not advertise past the missing
	// prefix: the consumer would otherwise skip the not-yet-materialized
	// [0,10).
	register(10, 20)
	if got := committed(); got != 0 {
		t.Fatalf("committed after [10,20) = %d, want 0 (gap at [0,10))", got)
	}

	// Filling the prefix makes the whole chain readable.
	register(0, 10)
	if got := committed(); got != 20 {
		t.Fatalf("committed after [0,10)+[10,20) = %d, want 20", got)
	}

	// An abandoned range never advances the head past the gap.
	register(30, 40)
	if got := committed(); got != 20 {
		t.Fatalf("committed after [30,40) = %d, want 20 (gap at [20,30))", got)
	}

	// Closing the gap advances through the full contiguous chain.
	register(20, 30)
	if got := committed(); got != 40 {
		t.Fatalf("committed after gap filled = %d, want 40", got)
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
