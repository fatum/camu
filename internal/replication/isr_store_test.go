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

func TestISRStore_CreateIfAbsentNoClobber(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewISRStore(s3)
	ctx := context.Background()

	first := ISRState{
		Partition:     0,
		ISR:           []string{"leader-a"},
		Leader:        "leader-a",
		LeaderEpoch:   1,
		HighWatermark: 5,
	}
	if err := store.Write(ctx, "no-clobber", first, ""); err != nil {
		t.Fatalf("initial Write: %v", err)
	}

	// A second unconditional "create" must fail: no last-writer-wins overwrite.
	stale := ISRState{
		Partition:     0,
		ISR:           []string{"stale-leader"},
		Leader:        "stale-leader",
		LeaderEpoch:   1,
		HighWatermark: 5,
	}
	if err := store.Write(ctx, "no-clobber", stale, ""); err == nil || !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("second create-if-absent Write: err = %v, want ErrConflict", err)
	}

	got, err := store.Read(ctx, "no-clobber", 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Leader != "leader-a" {
		t.Errorf("Leader: expected leader-a preserved, got %q", got.Leader)
	}
}

func TestISRStore_UpdateCreatesAndMutates(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewISRStore(s3)
	ctx := context.Background()

	// Update on a nonexistent partition creates the ISR state.
	if err := store.Update(ctx, "upd-topic", 0, 2, func(_ ISRState) (ISRState, error) {
		return ISRState{
			ISR:           []string{"leader-b"},
			Leader:        "leader-b",
			HighWatermark: 7,
		}, nil
	}); err != nil {
		t.Fatalf("Update create: %v", err)
	}

	got, err := store.Read(ctx, "upd-topic", 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Leader != "leader-b" || got.LeaderEpoch != 2 || got.HighWatermark != 7 {
		t.Fatalf("unexpected created state: %+v", got)
	}

	// A higher-epoch update overwrites.
	if err := store.Update(ctx, "upd-topic", 0, 3, func(_ ISRState) (ISRState, error) {
		return ISRState{
			ISR:           []string{"leader-c"},
			Leader:        "leader-c",
			HighWatermark: 9,
		}, nil
	}); err != nil {
		t.Fatalf("Update higher epoch: %v", err)
	}
	got, err = store.Read(ctx, "upd-topic", 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Leader != "leader-c" || got.LeaderEpoch != 3 || got.HighWatermark != 9 {
		t.Fatalf("unexpected mutated state: %+v", got)
	}
}

func TestISRStore_UpdateRejectsStaleEpoch(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewISRStore(s3)
	ctx := context.Background()

	if err := store.Update(ctx, "stale-topic", 0, 4, func(_ ISRState) (ISRState, error) {
		return ISRState{ISR: []string{"leader-d"}, Leader: "leader-d"}, nil
	}); err != nil {
		t.Fatalf("initial Update: %v", err)
	}

	// A stale writer with a lower epoch must be rejected.
	err := store.Update(ctx, "stale-topic", 0, 3, func(_ ISRState) (ISRState, error) {
		return ISRState{ISR: []string{"stale-leader"}, Leader: "stale-leader"}, nil
	})
	if err == nil {
		t.Fatal("Update with lower epoch should be rejected")
	}

	got, err := store.Read(ctx, "stale-topic", 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Leader != "leader-d" {
		t.Errorf("Leader: expected leader-d preserved, got %q", got.Leader)
	}
}
