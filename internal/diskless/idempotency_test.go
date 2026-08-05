package diskless

import (
	"context"
	"errors"
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
			alloc := func(seq int64, count int) (OffsetResult, error) {
				results, err := meta.AllocateOffsets(ctx, []OffsetAllocation{
					{Topic: "t", Partition: 0, Count: count, ProducerID: 7, Sequence: seq},
				})
				if err != nil {
					return OffsetResult{}, err
				}
				return results[0], nil
			}

			// First batch.
			first, err := alloc(10, 3)
			require.NoError(t, err)
			require.Equal(t, int64(0), first.BaseOffset)
			require.False(t, first.Duplicate)

			// Exact retry deduplicates to the original base.
			retry, err := alloc(10, 3)
			require.NoError(t, err)
			require.Equal(t, int64(0), retry.BaseOffset)
			require.True(t, retry.Duplicate)

			// Exact next contiguous batch advances.
			next, err := alloc(13, 2)
			require.NoError(t, err)
			require.Equal(t, int64(3), next.BaseOffset)
			require.False(t, next.Duplicate)

			// Inside the previous batch (13 + 2 -> [13,15)) is out of order.
			_, err = alloc(14, 1)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrOutOfOrderSequence))

			// Beyond the previous batch's end is a gap.
			_, err = alloc(16, 1)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrSequenceGap))

			// Below the recorded first sequence is a stale retry.
			_, err = alloc(12, 1)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrOutOfOrderSequence))

			// A stale retry of an old sequence is rejected once the producer
			// has advanced.
			_, err = alloc(10, 3)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrOutOfOrderSequence))
		})
	}
}
