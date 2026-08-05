package diskless

import (
	"context"
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

// TestS3MetaStore_CommitUploadedBatchIsReadableAndDeduplicated verifies a
// committed batch is readable and that an idempotent retry is deduplicated by
// the producer-sequence history (retroactive tombstone): the retry, even as a
// new physical upload, returns the original base without a second ref.
func TestS3MetaStore_CommitUploadedBatchIsReadableAndDeduplicated(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	first, err := m.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj1:0:10", FileKey: "obj1", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	// A client retry re-uploads the same logical batch (new BatchID/file). The
	// producer-sequence history deduplicates it: same base, no new ref.
	retry, err := m.CommitUploadedBatches(ctx, []UploadedBatch{{BatchID: "obj2:0:10", FileKey: "obj2", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, ProducerID: 7, Sequence: 0, CreatedAt: time.Now()}})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first[0].BaseOffset != 0 || !retry[0].Duplicate || retry[0].BaseOffset != 0 {
		t.Fatalf("unexpected outcomes: first=%+v retry=%+v", first, retry)
	}
	head, err := m.GetCommittedHead(ctx, "t", 0)
	if err != nil || head != 2 {
		t.Fatalf("committed head = %d, %v; want 2", head, err)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1024)
	if err != nil || len(refs) != 1 || refs[0].BaseOffset != 0 {
		t.Fatalf("refs = %+v, %v", refs, err)
	}
}

// TestS3MetaStore_CommitNonIdempotentRetryAppends verifies non-idempotent
// batches are not deduplicated, matching Kafka semantics: a retried upload is a
// fresh append.
func TestS3MetaStore_CommitNonIdempotentRetryAppends(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	b := UploadedBatch{BatchID: "obj:0:10", FileKey: "obj", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, CreatedAt: time.Now()}
	first, err := m.CommitUploadedBatches(ctx, []UploadedBatch{b})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	retry, err := m.CommitUploadedBatches(ctx, []UploadedBatch{b})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first[0].BaseOffset != 0 || retry[0].Duplicate || retry[0].BaseOffset != 2 {
		t.Fatalf("unexpected outcomes: first=%+v retry=%+v", first, retry)
	}
	refs, err := m.QuerySegments(ctx, "t", 0, 0, 1024)
	if err != nil || len(refs) != 2 {
		t.Fatalf("refs = %+v, %v; want 2", refs, err)
	}
}
