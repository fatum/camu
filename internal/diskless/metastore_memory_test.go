package diskless

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryMetaStore_AllocateOffsets_SinglePartition(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

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

func TestMemoryMetaStore_AllocateOffsets_MultiPartition(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

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

func TestMemoryMetaStore_GetPartitionHead(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

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

func TestMemoryMetaStore_RegisterAndQuery(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

	// Register two segments for partition 0.
	err := ms.RegisterSegment(ctx, SegmentRecord{
		FileKey: "seg-001.dat",
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 0, EndOffset: 5, ByteOffset: 0, ByteLength: 500},
		},
	})
	require.NoError(t, err)

	err = ms.RegisterSegment(ctx, SegmentRecord{
		FileKey: "seg-002.dat",
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 5, EndOffset: 10, ByteOffset: 0, ByteLength: 600},
			{Topic: "events", Partition: 1, BaseOffset: 0, EndOffset: 3, ByteOffset: 600, ByteLength: 300},
		},
	})
	require.NoError(t, err)

	// Query all from offset 0 — should return both segments for partition 0.
	refs, err := ms.QuerySegments(ctx, "events", 0, 0, 10000)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	assert.Equal(t, "seg-001.dat", refs[0].FileKey)
	assert.Equal(t, "seg-002.dat", refs[1].FileKey)

	// Query from offset 5 — should skip the first segment.
	refs, err = ms.QuerySegments(ctx, "events", 0, 5, 10000)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "seg-002.dat", refs[0].FileKey)

	// Query partition 1 — should only return its data.
	refs, err = ms.QuerySegments(ctx, "events", 1, 0, 10000)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "seg-002.dat", refs[0].FileKey)
	assert.Equal(t, int64(600), refs[0].ByteOffset)
}

func TestMemoryMetaStore_DeleteTopic(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

	_, err := ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 5},
	})
	require.NoError(t, err)

	err = ms.RegisterSegment(ctx, SegmentRecord{
		FileKey: "seg-001.dat",
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

func TestMemoryMetaStore_GetPartitionStartUsesHeadWhenNoSegmentsRemain(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

	_, err := ms.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "events", Partition: 0, Count: 5}})
	require.NoError(t, err)

	start, err := ms.GetPartitionStart(ctx, "events", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(5), start)
}

func TestMemoryMetaStore_PlanExpiredFileDeletesAdvancesStartAfterDeleteFileRefs(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

	_, err := ms.AllocateOffsets(ctx, []OffsetAllocation{{Topic: "events", Partition: 0, Count: 10}})
	require.NoError(t, err)

	require.NoError(t, ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "seg-old.dat",
		CreatedAt: time.Now().Add(-2 * time.Hour),
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 0, EndOffset: 5, ByteOffset: 0, ByteLength: 500},
		},
	}))
	require.NoError(t, ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "seg-fresh.dat",
		CreatedAt: time.Now(),
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 5, EndOffset: 10, ByteOffset: 0, ByteLength: 500},
		},
	}))

	deletable, err := ms.PlanExpiredFileDeletes(ctx, "events", 0, time.Now().Add(-1*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, []string{"seg-old.dat"}, deletable)

	require.NoError(t, ms.DeleteFileRefs(ctx, "seg-old.dat"))

	start, err := ms.GetPartitionStart(ctx, "events", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(5), start)
}

func TestMemoryMetaStore_PlanExpiredFileDeletesKeepsSharedFileWithFreshReference(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

	_, err := ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 5},
		{Topic: "events", Partition: 1, Count: 5},
	})
	require.NoError(t, err)

	require.NoError(t, ms.RegisterSegment(ctx, SegmentRecord{
		FileKey: "shared.dat",
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 0, EndOffset: 5, ByteOffset: 0, ByteLength: 500},
			{Topic: "events", Partition: 1, BaseOffset: 0, EndOffset: 5, ByteOffset: 500, ByteLength: 500},
		},
	}))
	require.NoError(t, ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "fresh-shared.dat",
		CreatedAt: time.Now(),
		Batches: []BatchRef{
			{Topic: "events", Partition: 1, BaseOffset: 5, EndOffset: 10, ByteOffset: 0, ByteLength: 500},
		},
	}))

	// Update the shared file with an old timestamp after adding the fresh file so
	// only the fresh file blocks deletion.
	markerTime := time.Now().Add(-2 * time.Hour)
	ms.mu.Lock()
	for key, entries := range ms.segments {
		for i := range entries {
			if entries[i].fileKey == "shared.dat" {
				entries[i].createdAt = markerTime
			}
		}
		ms.segments[key] = entries
	}
	ms.mu.Unlock()

	deletable, err := ms.PlanExpiredFileDeletes(ctx, "events", 0, time.Now().Add(-1*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, []string{"shared.dat"}, deletable)
}

func TestMemoryMetaStore_PlanExpiredFileDeletesSkipsFileWithFreshSharedRef(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()

	_, err := ms.AllocateOffsets(ctx, []OffsetAllocation{
		{Topic: "events", Partition: 0, Count: 5},
		{Topic: "events", Partition: 1, Count: 5},
	})
	require.NoError(t, err)

	require.NoError(t, ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "shared.dat",
		CreatedAt: time.Now().Add(-2 * time.Hour),
		Batches: []BatchRef{
			{Topic: "events", Partition: 0, BaseOffset: 0, EndOffset: 5, ByteOffset: 0, ByteLength: 500},
		},
	}))
	require.NoError(t, ms.RegisterSegment(ctx, SegmentRecord{
		FileKey:   "shared.dat",
		CreatedAt: time.Now(),
		Batches: []BatchRef{
			{Topic: "events", Partition: 1, BaseOffset: 0, EndOffset: 5, ByteOffset: 500, ByteLength: 500},
		},
	}))

	deletable, err := ms.PlanExpiredFileDeletes(ctx, "events", 0, time.Now().Add(-1*time.Hour))
	require.NoError(t, err)
	assert.Empty(t, deletable)
}
