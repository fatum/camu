package diskless

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryMetaStore_CommitUploadedBatches_IsAtomicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()
	// Nothing is visible before the metadata commit (the uploaded object itself
	// is deliberately outside the metastore).
	head, err := ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), head)

	b := UploadedBatch{BatchID: "uploaded:0:10", FileKey: "uploaded", Topic: "t", Partition: 0, Count: 2, ProducerID: 9, Sequence: 0, ByteLength: 10}
	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{b})
	require.NoError(t, err)
	require.Equal(t, int64(0), first[0].BaseOffset)
	retry, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{b})
	require.NoError(t, err)
	require.True(t, retry[0].Duplicate)
	require.Equal(t, int64(0), retry[0].BaseOffset)
	head, err = ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), head)
	refs, err := ms.QuerySegments(ctx, "t", 0, 0, 1024)
	require.NoError(t, err)
	require.Len(t, refs, 1)

	_, err = ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "gap:0:1", FileKey: "gap", Topic: "t", Partition: 0, Count: 1, ProducerID: 9, Sequence: 4}})
	require.ErrorIs(t, err, ErrSequenceGap)
	head, err = ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), head)
}

func TestMemoryMetaStore_CommitIdempotentRetryDeduplicatesAcrossBatches(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()
	// Producer 7 commits sequence 0 from a first physical upload.
	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj1:0:10", FileKey: "obj1", Topic: "t", Partition: 0, Count: 3, ByteLength: 10, ProducerID: 7, Sequence: 0}})
	require.NoError(t, err)
	require.Equal(t, int64(0), first[0].BaseOffset)
	// A client retry re-uploads the same logical batch (new BatchID/file). The
	// producer-sequence history deduplicates it (retroactive tombstone): same
	// base, no new ref, head unchanged.
	retry, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj2:0:10", FileKey: "obj2", Topic: "t", Partition: 0, Count: 3, ByteLength: 10, ProducerID: 7, Sequence: 0}})
	require.NoError(t, err)
	require.True(t, retry[0].Duplicate)
	require.Equal(t, first[0].BaseOffset, retry[0].BaseOffset)
	head, err := ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), head)
}

func TestMemoryMetaStore_CommitDeduplicatesInWindowOldReplayAndRejectsRotatedOut(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()
	// Producer 7 commits sequences 0..5 (count 1 each). History is capped to the
	// last `uploadedProducerHistory` batches, so sequence 0 rotates out.
	for seq := int64(0); seq <= 5; seq++ {
		_, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: fmt.Sprintf("obj%d:0:1", seq), FileKey: fmt.Sprintf("obj%d", seq), Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: seq}})
		require.NoError(t, err)
	}
	// An exact replay of a non-latest but still-recorded sequence (3) is
	// deduplicated: original base, no new ref.
	old, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "replay3:0:1", FileKey: "replay3", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 3}})
	require.NoError(t, err)
	require.True(t, old[0].Duplicate)
	require.Equal(t, int64(3), old[0].BaseOffset)
	// An exact replay that has rotated out of the window is rejected as
	// out-of-order, never silently re-allocated at a fresh offset.
	_, err = ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "replay0:0:1", FileKey: "replay0", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 0}})
	require.ErrorIs(t, err, ErrOutOfOrderSequence)
	// Neither retry advanced the committed head.
	head, err := ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	require.Equal(t, int64(6), head)
}

func TestMemoryMetaStore_CommitBaselinesProducerAtFirstSequence(t *testing.T) {
	ms := NewMemoryMetaStore()
	ctx := context.Background()
	// Kafka records the client's initial sequence and validates contiguity
	// from there; a fresh producer may begin at any sequence.
	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "object:0:10", FileKey: "object", Topic: "t", Partition: 0, Count: 1, ByteLength: 10, ProducerID: 3, Sequence: 7}})
	require.NoError(t, err)
	require.Equal(t, int64(0), first[0].BaseOffset)
	// Contiguity is enforced from the baseline: skipping 8 is a gap.
	_, err = ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "gap:0:10", FileKey: "gap", Topic: "t", Partition: 0, Count: 1, ByteLength: 10, ProducerID: 3, Sequence: 9}})
	require.ErrorIs(t, err, ErrSequenceGap)
	head, err := ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), head)
}

func TestMemoryMetaStore_CommitMultipleBatchesAtomically(t *testing.T) {
	ms := NewMemoryMetaStore()
	ctx := context.Background()
	results, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{
		{BatchID: "a:0:1", FileKey: "a", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 0},
		{BatchID: "b:1:1", FileKey: "b", Topic: "t", Partition: 0, Count: 2, ByteLength: 1, ProducerID: 7, Sequence: 1},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, int64(0), results[0].BaseOffset)
	require.Equal(t, int64(1), results[1].BaseOffset)
	head, err := ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), head)
	refs, err := ms.QuerySegments(ctx, "t", 0, 0, 1024)
	require.NoError(t, err)
	require.Len(t, refs, 2)
}

func TestMemoryMetaStore_CommitRejectsCrossPartitionBatch(t *testing.T) {
	ms := NewMemoryMetaStore()
	_, err := ms.CommitUploadedBatches(context.Background(), []UploadedBatch{
		{BatchID: "a:0:1", FileKey: "a", Topic: "t", Partition: 0, Count: 1, ByteLength: 1},
		{BatchID: "b:0:1", FileKey: "b", Topic: "t", Partition: 1, Count: 1, ByteLength: 1},
	})
	require.Error(t, err)
	head, err := ms.GetCommittedHead(context.Background(), "t", 0)
	require.NoError(t, err)
	require.Zero(t, head)
}

func TestMemoryMetaStore_ProducerBaselineAtNonZeroSequence(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()
	// A producer beginning at sequence 1 records its baseline and commits.
	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "one", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 1}})
	require.NoError(t, err)
	require.Equal(t, int64(0), first[0].BaseOffset)
	// The next contiguous batch commits normally.
	next, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "two", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 2}})
	require.NoError(t, err)
	require.Equal(t, int64(1), next[0].BaseOffset)
	// An exact retry of the baseline is deduplicated.
	retry, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "one-retry", FileKey: "f2", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 1}})
	require.NoError(t, err)
	require.True(t, retry[0].Duplicate)
}
