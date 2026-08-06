//go:build dynamodb

package diskless

import (
	"context"
	"os"
	"strings"
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

// TestDynamoMetaStore_CommitUploadedBatches_IsAtomicAndIdempotent verifies a
// committed batch is assigned a contiguous offset, an exact idempotent retry is
// deduplicated by the producer-sequence history, and the readable head reflects
// only committed batches.
func TestDynamoMetaStore_CommitUploadedBatches_IsAtomicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	b := UploadedBatch{BatchID: "f:0:10", FileKey: "f.data", Topic: "t", Partition: 0, Count: 3, ByteOffset: 0, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()}

	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{b})
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, int64(0), first[0].BaseOffset)
	assert.False(t, first[0].Duplicate)

	// An exact retry of the same physical batch must not append again.
	retry, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{b})
	require.NoError(t, err)
	require.Len(t, retry, 1)
	assert.Equal(t, int64(0), retry[0].BaseOffset)
	assert.True(t, retry[0].Duplicate)

	// A distinct batch continues contiguously.
	second, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "f:10:10", FileKey: "f.data", Topic: "t", Partition: 0, Count: 2, ByteOffset: 10, ByteLength: 10, ProducerID: 7, Sequence: 3, CreatedAt: time.Now()}})
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Equal(t, int64(3), second[0].BaseOffset)

	committed, err := ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(5), committed)

	refs, err := ms.QuerySegments(ctx, "t", 0, 0, 10000)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	assert.Equal(t, int64(0), refs[0].BaseOffset)
	assert.Equal(t, int64(3), refs[0].EndOffset)
	assert.Equal(t, int64(3), refs[1].BaseOffset)
}

// TestDynamoMetaStore_CommitIdempotentRetryDeduplicatesAcrossBatches verifies
// that an idempotent retry (same producer and sequence) is deduplicated by the
// producer-sequence history even when the retry is a new physical upload, and
// that the committed head does not advance.
func TestDynamoMetaStore_CommitIdempotentRetryDeduplicatesAcrossBatches(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj1:0:10", FileKey: "obj1", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()}})
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, int64(0), first[0].BaseOffset)

	retry, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj2:0:10", FileKey: "obj2", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()}})
	require.NoError(t, err)
	require.Len(t, retry, 1)
	assert.Equal(t, int64(0), retry[0].BaseOffset)
	assert.True(t, retry[0].Duplicate)

	head, err := ms.GetCommittedHead(ctx, "t", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(2), head)
}

// TestDynamoMetaStore_CommitRequiresInitialProducerSequenceZero verifies a
// producer's first committed batch must carry sequence 0, which enforces
// cross-node ordering of a single producer's batches.
func TestDynamoMetaStore_CommitRequiresInitialProducerSequenceZero(t *testing.T) {
	ctx := context.Background()
	ms := dynamoTestStore(t)

	_, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "one", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 1}})
	require.Error(t, err, "initial sequence 1 must be rejected")

	first, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "zero", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 0}})
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, int64(0), first[0].BaseOffset)

	next, err := ms.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "one", FileKey: "f", Topic: "t", Partition: 0, Count: 1, ByteLength: 1, ProducerID: 7, Sequence: 1}})
	require.NoError(t, err)
	require.Len(t, next, 1)
	assert.Equal(t, int64(1), next[0].BaseOffset)
}
