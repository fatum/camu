package diskless

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
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

func TestMemoryMetaStore_CommitRequiresInitialProducerSequenceZero(t *testing.T) {
	ms := NewMemoryMetaStore()
	_, err := ms.CommitUploadedBatches(context.Background(), []UploadedBatch{{BatchID: "object:0:10", FileKey: "object", Topic: "t", Partition: 0, Count: 1, ByteLength: 10, ProducerID: 3, Sequence: 1}})
	require.ErrorIs(t, err, ErrSequenceGap)
	head, err := ms.GetCommittedHead(context.Background(), "t", 0)
	require.NoError(t, err)
	require.Zero(t, head)
}

func TestMemoryMetaStore_CommitRejectsMultiBatchWithoutMutation(t *testing.T) {
	ms := NewMemoryMetaStore()
	_, err := ms.CommitUploadedBatches(context.Background(), []UploadedBatch{
		{BatchID: "a:0:1", FileKey: "a", Topic: "t", Partition: 0, Count: 1, ByteLength: 1},
		{BatchID: "b:0:1", FileKey: "b", Topic: "t", Partition: 0, Count: 1, ByteLength: 1},
	})
	require.Error(t, err)
	head, err := ms.GetCommittedHead(context.Background(), "t", 0)
	require.NoError(t, err)
	require.Zero(t, head)
}

func TestMemoryMetaStore_ConcurrentProducerCannotCommitSequenceOneFirst(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryMetaStore()
	_, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "one", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 1}})
	require.ErrorIs(t, err, ErrSequenceGap)
	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "zero", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 0}})
	require.NoError(t, err)
	require.Equal(t, int64(0), first[0].BaseOffset)
	next, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "one", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 1}})
	require.NoError(t, err)
	require.Equal(t, int64(1), next[0].BaseOffset)
}
