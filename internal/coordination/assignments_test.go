package coordination

import (
	"context"
	"testing"
)

func TestAssignmentStore_WriteRead(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewAssignmentStore(s3)
	ctx := context.Background()

	assignments := TopicAssignments{
		Partitions: map[int]string{
			0: "instance-a",
			1: "instance-b",
			2: "instance-a",
			3: "instance-b",
		},
		Version: 1,
	}

	if err := store.Write(ctx, "test-topic", assignments, ""); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got.Version != 1 {
		t.Errorf("expected version 1, got %d", got.Version)
	}
	if len(got.Partitions) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(got.Partitions))
	}
	if got.ETag == "" {
		t.Error("expected non-empty ETag from Read")
	}
}

func TestAssignmentStore_CASOverwrite(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewAssignmentStore(s3)
	ctx := context.Background()

	v1 := TopicAssignments{
		Partitions: map[int]string{0: "a", 1: "a"},
		Version:    1,
	}
	if err := store.Write(ctx, "topic", v1, ""); err != nil {
		t.Fatalf("Write v1: %v", err)
	}

	// Read to get ETag.
	got, err := store.Read(ctx, "topic")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	// Overwrite with correct ETag succeeds.
	v2 := TopicAssignments{
		Partitions: map[int]string{0: "b", 1: "a"},
		Version:    2,
	}
	if err := store.Write(ctx, "topic", v2, got.ETag); err != nil {
		t.Fatalf("Write v2 with correct ETag: %v", err)
	}

	// Overwrite with stale ETag fails.
	v3 := TopicAssignments{
		Partitions: map[int]string{0: "c", 1: "c"},
		Version:    3,
	}
	err = store.Write(ctx, "topic", v3, got.ETag) // stale ETag
	if err == nil {
		t.Fatal("Write v3 with stale ETag should fail")
	}

	// Final state should be v2.
	final, err := store.Read(ctx, "topic")
	if err != nil {
		t.Fatalf("Final read: %v", err)
	}
	if final.Version != 2 {
		t.Errorf("expected version 2, got %d", final.Version)
	}
	if final.Partitions[0] != "b" {
		t.Errorf("partition 0: expected b, got %q", final.Partitions[0])
	}
}
