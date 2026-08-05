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

func TestS3MetaStore_CommitUploadedBatchIsReadableAndDeduplicated(t *testing.T) {
	m := newTestS3MetaStore(t)
	ctx := context.Background()
	b := UploadedBatch{BatchID: "object:0:10", FileKey: "object", Topic: "t", Partition: 0, Count: 2, ByteLength: 10, CreatedAt: time.Now()}
	first, err := m.CommitUploadedBatches(ctx, []UploadedBatch{b})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	retry, err := m.CommitUploadedBatches(ctx, []UploadedBatch{b})
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
