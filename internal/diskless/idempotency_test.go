package diskless

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIdempotentSequenceValidation exercises exact-retry dedup, contiguous
// next-batch allocation, and rejection of out-of-order and gap sequences.
func TestIdempotentSequenceValidation(t *testing.T) {
	ctx := context.Background()

	for name, meta := range map[string]MetaStore{
		"memory": NewMemoryMetaStore(),
		"s3":     NewS3MetaStore(testS3Client(t)),
	} {
		t.Run(name, func(t *testing.T) {
			commit := func(seq int64, count int) (OffsetResult, error) {
				results, err := meta.CommitUploadedBatches(ctx, []UploadedBatch{
					{BatchID: "batch-" + strconv.FormatInt(seq, 10), FileKey: "uploaded", Topic: "t", Partition: 0, Count: count, ProducerID: 7, Sequence: seq},
				})
				if err != nil {
					return OffsetResult{}, err
				}
				return results[0], nil
			}

			// First batch.
			first, err := commit(0, 3)
			require.NoError(t, err)
			require.Equal(t, int64(0), first.BaseOffset)
			require.False(t, first.Duplicate)

			// Exact retry deduplicates to the original base.
			retry, err := commit(0, 3)
			require.NoError(t, err)
			require.Equal(t, int64(0), retry.BaseOffset)
			require.True(t, retry.Duplicate)

			// Exact next contiguous batch advances.
			next, err := commit(3, 2)
			require.NoError(t, err)
			require.Equal(t, int64(3), next.BaseOffset)
			require.False(t, next.Duplicate)

			// Inside the previous batch (13 + 2 -> [13,15)) is out of order.
			_, err = commit(4, 1)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrOutOfOrderSequence))

			// Beyond the previous batch's end is a gap.
			_, err = commit(6, 1)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrSequenceGap))

			// Below the recorded first sequence is a stale retry.
			_, err = commit(2, 1)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrOutOfOrderSequence))

		})
	}
}
