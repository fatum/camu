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

	if err := store.Write(ctx, "test-topic", assignments); err != nil {
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
	if got.Partitions[0] != "instance-a" {
		t.Errorf("partition 0: expected instance-a, got %q", got.Partitions[0])
	}
	if got.Partitions[1] != "instance-b" {
		t.Errorf("partition 1: expected instance-b, got %q", got.Partitions[1])
	}
}

func TestAssignmentStore_Overwrite(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewAssignmentStore(s3)
	ctx := context.Background()

	v1 := TopicAssignments{
		Partitions: map[int]string{0: "a", 1: "a"},
		Version:    1,
	}
	if err := store.Write(ctx, "topic", v1); err != nil {
		t.Fatalf("Write v1: %v", err)
	}

	v2 := TopicAssignments{
		Partitions: map[int]string{0: "b", 1: "a"},
		Version:    2,
	}
	if err := store.Write(ctx, "topic", v2); err != nil {
		t.Fatalf("Write v2: %v", err)
	}

	got, err := store.Read(ctx, "topic")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Version != 2 {
		t.Errorf("expected version 2, got %d", got.Version)
	}
	if got.Partitions[0] != "b" {
		t.Errorf("partition 0: expected b, got %q", got.Partitions[0])
	}
}
