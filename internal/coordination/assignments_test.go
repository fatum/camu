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
		Partitions: map[int]PartitionAssignment{
			0: {Replicas: []string{"instance-a"}, Leader: "instance-a", LeaderEpoch: 1},
			1: {Replicas: []string{"instance-b"}, Leader: "instance-b", LeaderEpoch: 1},
			2: {Replicas: []string{"instance-a"}, Leader: "instance-a", LeaderEpoch: 1},
			3: {Replicas: []string{"instance-b"}, Leader: "instance-b", LeaderEpoch: 1},
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

func TestAssignmentStore_ReplicaSets(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewAssignmentStore(s3)
	ctx := context.Background()

	assignments := TopicAssignments{
		Partitions: map[int]PartitionAssignment{
			0: {
				Replicas:    []string{"instance-a", "instance-b"},
				Leader:      "instance-a",
				LeaderEpoch: 3,
			},
			1: {
				Replicas:    []string{"instance-b", "instance-a"},
				Leader:      "instance-b",
				LeaderEpoch: 5,
			},
		},
		Version: 2,
	}

	if err := store.Write(ctx, "replica-topic", assignments, ""); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, "replica-topic")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got.Version != 2 {
		t.Errorf("expected version 2, got %d", got.Version)
	}
	if len(got.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(got.Partitions))
	}

	p0 := got.Partitions[0]
	if p0.Leader != "instance-a" {
		t.Errorf("partition 0 leader: expected instance-a, got %q", p0.Leader)
	}
	if p0.LeaderEpoch != 3 {
		t.Errorf("partition 0 leader_epoch: expected 3, got %d", p0.LeaderEpoch)
	}
	if len(p0.Replicas) != 2 || p0.Replicas[0] != "instance-a" || p0.Replicas[1] != "instance-b" {
		t.Errorf("partition 0 replicas: expected [instance-a instance-b], got %v", p0.Replicas)
	}

	p1 := got.Partitions[1]
	if p1.Leader != "instance-b" {
		t.Errorf("partition 1 leader: expected instance-b, got %q", p1.Leader)
	}
	if p1.LeaderEpoch != 5 {
		t.Errorf("partition 1 leader_epoch: expected 5, got %d", p1.LeaderEpoch)
	}
	if len(p1.Replicas) != 2 || p1.Replicas[0] != "instance-b" || p1.Replicas[1] != "instance-a" {
		t.Errorf("partition 1 replicas: expected [instance-b instance-a], got %v", p1.Replicas)
	}

	if got.ETag == "" {
		t.Error("expected non-empty ETag from Read")
	}
}

func TestAssignmentStore_CASOverwrite(t *testing.T) {
	s3 := newTestS3Client(t)
	store := NewAssignmentStore(s3)
	ctx := context.Background()

	pa := func(leader string) PartitionAssignment {
		return PartitionAssignment{Replicas: []string{leader}, Leader: leader, LeaderEpoch: 1}
	}

	v1 := TopicAssignments{
		Partitions: map[int]PartitionAssignment{0: pa("a"), 1: pa("a")},
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
		Partitions: map[int]PartitionAssignment{0: pa("b"), 1: pa("a")},
		Version:    2,
	}
	if err := store.Write(ctx, "topic", v2, got.ETag); err != nil {
		t.Fatalf("Write v2 with correct ETag: %v", err)
	}

	// Overwrite with stale ETag fails.
	v3 := TopicAssignments{
		Partitions: map[int]PartitionAssignment{0: pa("c"), 1: pa("c")},
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
	if final.Partitions[0].Leader != "b" {
		t.Errorf("partition 0: expected leader b, got %q", final.Partitions[0].Leader)
	}
}
