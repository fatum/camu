package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/maksim/camu/internal/storage"
)

func newTestS3Client(t *testing.T) *storage.S3Client {
	t.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Region:   "us-east-1",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("failed to create s3 client: %v", err)
	}
	return s3Client
}

func TestISRStore_WriteRead(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewISRStore(s3)
	ctx := context.Background()

	state := ISRState{
		Partition:     2,
		ISR:           []string{"instance-a", "instance-b"},
		Leader:        "instance-a",
		LeaderEpoch:   3,
		HighWatermark: 42,
	}

	if err := store.Write(ctx, "test-topic", state, ""); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, "test-topic", 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got.Partition != 2 {
		t.Errorf("Partition: expected 2, got %d", got.Partition)
	}
	if got.Leader != "instance-a" {
		t.Errorf("Leader: expected instance-a, got %q", got.Leader)
	}
	if got.LeaderEpoch != 3 {
		t.Errorf("LeaderEpoch: expected 3, got %d", got.LeaderEpoch)
	}
	if got.HighWatermark != 42 {
		t.Errorf("HighWatermark: expected 42, got %d", got.HighWatermark)
	}
	if len(got.ISR) != 2 || got.ISR[0] != "instance-a" || got.ISR[1] != "instance-b" {
		t.Errorf("ISR: expected [instance-a instance-b], got %v", got.ISR)
	}
	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be set")
	}
	if got.ETag == "" {
		t.Error("expected non-empty ETag from Read")
	}
}

func TestISRStore_CAS(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewISRStore(s3)
	ctx := context.Background()

	initial := ISRState{
		Partition:     0,
		ISR:           []string{"instance-a"},
		Leader:        "instance-a",
		LeaderEpoch:   1,
		HighWatermark: 10,
	}

	// Initial write (unconditional).
	if err := store.Write(ctx, "cas-topic", initial, ""); err != nil {
		t.Fatalf("initial Write: %v", err)
	}

	// Read to get ETag.
	got, err := store.Read(ctx, "cas-topic", 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	// CAS update with correct ETag succeeds.
	updated := ISRState{
		Partition:     0,
		ISR:           []string{"instance-a", "instance-b"},
		Leader:        "instance-a",
		LeaderEpoch:   1,
		HighWatermark: 20,
	}
	if err := store.Write(ctx, "cas-topic", updated, got.ETag); err != nil {
		t.Fatalf("CAS write with correct ETag: %v", err)
	}

	// CAS update with stale ETag fails with ErrConflict.
	stale := ISRState{
		Partition:     0,
		ISR:           []string{"instance-c"},
		Leader:        "instance-c",
		LeaderEpoch:   2,
		HighWatermark: 30,
	}
	err = store.Write(ctx, "cas-topic", stale, got.ETag) // stale ETag
	if err == nil {
		t.Fatal("CAS write with stale ETag should fail")
	}
	if !errors.Is(err, storage.ErrConflict) {
		t.Errorf("expected ErrConflict, got: %v", err)
	}

	// Final state should reflect the successful CAS update (HighWatermark=20).
	final, err := store.Read(ctx, "cas-topic", 0)
	if err != nil {
		t.Fatalf("final Read: %v", err)
	}
	if final.HighWatermark != 20 {
		t.Errorf("final HighWatermark: expected 20, got %d", final.HighWatermark)
	}
	if len(final.ISR) != 2 {
		t.Errorf("final ISR: expected 2 members, got %v", final.ISR)
	}
}
