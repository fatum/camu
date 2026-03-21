package storage

import (
	"context"
	"testing"
)

func newTestS3(t *testing.T) *S3Client {
	t.Helper()
	c, err := NewS3Client(S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	return c
}

func TestOffsetStore_CommitAndGet(t *testing.T) {
	s3 := newTestS3(t)
	store := NewOffsetStore(s3)
	ctx := context.Background()

	offsets := map[int]uint64{0: 100, 1: 200, 2: 50}
	if err := store.CommitGroup(ctx, "group-1", "my-topic", offsets); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}

	got, err := store.GetGroup(ctx, "group-1", "my-topic")
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}

	for k, want := range offsets {
		if got[k] != want {
			t.Errorf("partition %d: got %d, want %d", k, got[k], want)
		}
	}

	// Getting a non-existent group returns empty map.
	empty, err := store.GetGroup(ctx, "no-such-group", "my-topic")
	if err != nil {
		t.Fatalf("GetGroup non-existent: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected empty map, got %v", empty)
	}
}

func TestOffsetStore_ConsumerSpecific(t *testing.T) {
	s3 := newTestS3(t)
	store := NewOffsetStore(s3)
	ctx := context.Background()

	offsets := map[int]uint64{0: 42, 3: 99}
	if err := store.CommitConsumer(ctx, "consumer-A", "topic-x", offsets); err != nil {
		t.Fatalf("CommitConsumer: %v", err)
	}

	got, err := store.GetConsumer(ctx, "consumer-A", "topic-x")
	if err != nil {
		t.Fatalf("GetConsumer: %v", err)
	}

	for k, want := range offsets {
		if got[k] != want {
			t.Errorf("partition %d: got %d, want %d", k, got[k], want)
		}
	}

	// Consumer offsets are independent from group offsets.
	groupOffsets, err := store.GetGroup(ctx, "consumer-A", "topic-x")
	if err != nil {
		t.Fatalf("GetGroup for consumer name: %v", err)
	}
	if len(groupOffsets) != 0 {
		t.Errorf("group offsets should be empty, got %v", groupOffsets)
	}
}
