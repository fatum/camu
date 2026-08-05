//go:build dynamodb

package diskless

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dynamoTestStore(t *testing.T) *DynamoMetaStore {
	t.Helper()
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8000"
	}
	// Use a unique prefix per test to avoid conflicts.
	prefix := "test_" + strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_")
	ctx := context.Background()
	store, err := NewDynamoMetaStore(ctx, DynamoMetaStoreConfig{
		TablePrefix: prefix,
		Region:      "us-east-1",
		Endpoint:    endpoint,
	})
	require.NoError(t, err)
	require.NoError(t, store.EnsureTables(ctx))
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestDynamoMetaStore_AllocateOffsets_SinglePartition(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	results, err := ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 3},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), results[0].BaseOffset)

	results, err = ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 5},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), results[0].BaseOffset)
}

func TestDynamoMetaStore_AllocateOffsets_MultiPartition(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	results, err := ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 10},
		{Topic: "events", Partition: 1, Count: 20},
		{Topic: "events", Partition: 2, Count: 30},
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Equal(t, int64(0), results[0].BaseOffset)
	assert.Equal(t, int64(0), results[1].BaseOffset)
	assert.Equal(t, int64(0), results[2].BaseOffset)
}

func TestDynamoMetaStore_RegisterAndQuery(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	err := ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "seg-001.dat",
		CreatedAt: time.Now(),
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 0, EndOffset: 5, ByteOffset: 0, ByteLength: 500},
		},
	})
	require.NoError(t, err)

	err = ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "seg-002.dat",
		CreatedAt: time.Now(),
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 5, EndOffset: 10, ByteOffset: 0, ByteLength: 600},
			{Topic: "events", Partition: 1, BaseOffset: 0, EndOffset: 3, ByteOffset: 600, ByteLength: 300},
		},
	})
	require.NoError(t, err)

	// Query all from offset 0 - should return both segments for partition 0.
	refs, err := ms.QuerySegments(ctx, "events", 0, 0, 10000)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	assert.Equal(t, "seg-001.dat", refs[0].FileKey)
	assert.Equal(t, "seg-002.dat", refs[1].FileKey)

	// Query from offset 5 - should skip the first segment.
	refs, err = ms.QuerySegments(ctx, "events", 0, 5, 10000)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "seg-002.dat", refs[0].FileKey)

	// Query partition 1 - should only return its data.
	refs, err = ms.QuerySegments(ctx, "events", 1, 0, 10000)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "seg-002.dat", refs[0].FileKey)
	assert.Equal(t, int64(600), refs[0].ByteOffset)
}

// TestDynamoMetaStore_CommittedHeadOnlyAdvancesContiguously verifies that the
// committed high watermark never advances past an unmaterialized range, even
// when concurrent writers register adjacent ranges out of order.
func TestDynamoMetaStore_CommittedHeadOnlyAdvancesContiguously(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	committed := func() int64 {
		t.Helper()
		h, err := ms.GetCommittedHead(ctx, "t", 0)
		require.NoError(t, err)
		return h
	}
	register := func(base, end int64) {
		t.Helper()
		require.NoError(t, ms.RegisterSegment(ctx, SegmentRecord{
			FileKey:   "f.data",
			Batches:   []BatchRef{{Topic: "t", Partition: 0, BaseOffset: base, EndOffset: end, ByteLength: end - base}},
			CreatedAt: time.Now(),
		}))
	}

	register(10, 20)
	assert.Equal(t, int64(0), committed(), "later range alone must not advance past missing prefix")

	register(0, 10)
	assert.Equal(t, int64(20), committed(), "filled prefix must expose the whole chain")

	register(30, 40)
	assert.Equal(t, int64(20), committed(), "abandoned range must not advance past gap")

	register(20, 30)
	assert.Equal(t, int64(40), committed(), "closing the gap advances through the full chain")
}

func TestDynamoMetaStore_GetPartitionHead(t *testing.T) {	ctx := context.Background()
	ms := dynamoTestStore(t)

	head, err := ms.GetPartitionHead(ctx, "events", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), head)

	_, err = ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 5},
	})
	require.NoError(t, err)

	head, err = ms.GetPartitionHead(ctx, "events", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(5), head)
}

func TestDynamoMetaStore_DeleteTopic(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	_, err := ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 5},
	})
	require.NoError(t, err)

	err = ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "seg-001.dat",
		CreatedAt: time.Now(),
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 0, EndOffset: 5, ByteOffset: 0, ByteLength: 500},
		},
	})
	require.NoError(t, err)

	err = ms.DeleteTopic(ctx, "events")
	require.NoError(t, err)

	head, err := ms.GetPartitionHead(ctx, "events", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), head)

	refs, err := ms.QuerySegments(ctx, "events", 0, 0, 10000)
	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestDynamoMetaStore_IdempotentAllocation(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	alloc := OffsetAllocation{Topic: "t", Partition: 0, Count: 3, ProducerID: 7, Sequence: 10}
	first, err := ms.AllocateOffsets(ctx, []OffsetAllocation{alloc})
	require.NoError(t, err)
	assert.Equal(t, int64(0), first[0].BaseOffset)
	assert.False(t, first[0].Duplicate)

	retry, err := ms.AllocateOffsets(ctx, []OffsetAllocation{alloc})
	require.NoError(t, err)
	assert.Equal(t, int64(0), retry[0].BaseOffset)
	assert.True(t, retry[0].Duplicate)

	next, err := ms.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 2, ProducerID: 7, Sequence: 13}})
	require.NoError(t, err)
	assert.Equal(t, int64(3), next[0].BaseOffset)
	assert.False(t, next[0].Duplicate)

	// Gap and out-of-order sequences must be rejected.
	_, err = ms.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 1, ProducerID: 7, Sequence: 16}})
	require.Error(t, err, "gap sequence must be rejected")
	_, err = ms.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 1, ProducerID: 7, Sequence: 12}})
	require.Error(t, err, "out-of-order sequence must be rejected")
}

// TestDynamoMetaStore_IdempotentConcurrentRejection verifies the atomic
// allocation: two concurrent first-batch allocations from the same producer
// with different sequences may not both advance the counter. The conditional
// update pins the producer state, so exactly one succeeds and the other is
// rejected as a gap or out-of-order sequence.
func TestDynamoMetaStore_IdempotentConcurrentRejection(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	var wg sync.WaitGroup
	results := make([]error, 2)
	seqs := []int64{0, 5}
	for i, s := range seqs {
		wg.Add(1)
		go func(idx int, seq int64) {
			defer wg.Done()
			_, results[idx] = ms.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "t", Partition: 0, Count: 1, ProducerID: 7, Sequence: seq}})
		}(i, s)
	}
	wg.Wait()

	successes := 0
	for _, err := range results {
		if err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes, "exactly one of two concurrent first-batch allocations must succeed")
}
