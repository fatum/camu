package coordination

import (
	"context"
	"reflect"
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

// TestAssignReplicated_PrunesInactiveReplicasWhenSurvivorExists verifies F5:
// when there are enough active instances to maintain RF but some replicas have
// vanished, the inactive replicas are pruned from the assignment (so their
// registrations can be garbage-collected) and a surviving replica remains.
func TestAssignReplicated_PrunesInactiveReplicasWhenSurvivorExists(t *testing.T) {
	current := map[int]PartitionAssignment{
		0: {
			Replicas:    []string{"n1", "n2", "n3"},
			Leader:      "n1",
			LeaderEpoch: 7,
		},
	}

	// n2 and n3 are gone; n1, n4, n5 are active (enough for RF=3).
	got := AssignReplicated([]string{"n1", "n4", "n5"}, 1, 3, current)
	partition, ok := got[0]
	if !ok {
		t.Fatal("missing partition 0 assignment")
	}
	if containsReplica(partition.Replicas, "n2") || containsReplica(partition.Replicas, "n3") {
		t.Fatalf("inactive replicas must be pruned, replicas = %v", partition.Replicas)
	}
	if !containsReplica(partition.Replicas, "n1") {
		t.Fatalf("native survivor n1 must remain, replicas = %v", partition.Replicas)
	}
	if partition.Leader != "n1" {
		t.Fatalf("leader = %q, want n1 (native survivor)", partition.Leader)
	}
	if partition.LeaderEpoch != 7 {
		t.Fatalf("leader_epoch = %d, want 7 (leader unchanged)", partition.LeaderEpoch)
	}
}

func TestAssignReplicated_KeepsAssignmentWhenNoReplicaIsActive(t *testing.T) {
	current := map[int]PartitionAssignment{
		0: {
			Replicas:    []string{"n1", "n2", "n3"},
			Leader:      "n1",
			LeaderEpoch: 4,
		},
	}

	got := AssignReplicated([]string{"n5"}, 1, 3, current)
	partition, ok := got[0]
	if !ok {
		t.Fatal("missing partition 0 assignment")
	}
	if !reflect.DeepEqual(partition.Replicas, []string{"n1", "n2", "n3"}) {
		t.Fatalf("replicas = %v, want [n1 n2 n3]", partition.Replicas)
	}
	if partition.Leader != "n1" {
		t.Fatalf("leader = %q, want %q", partition.Leader, "n1")
	}
	if partition.LeaderEpoch != 4 {
		t.Fatalf("leader_epoch = %d, want 4", partition.LeaderEpoch)
	}
}

// TestAssignReplicated_PrunesDeadReplicaBackfillsFollower verifies F5: a
// replica that has vanished is pruned from the preserved set so its instance
// registration is no longer referenced by the assignment and can be
// garbage-collected. The freed slot is backfilled with an active instance so
// routing keeps advertising full RF, but the backfilled node is a follower
// only — it never becomes leader, because it has not held the partition's
// committed prefix.
func TestAssignReplicated_PrunesDeadReplicaBackfillsFollower(t *testing.T) {
	current := map[int]PartitionAssignment{
		0: {
			Replicas:    []string{"n1", "n2", "n3"},
			Leader:      "n1",
			LeaderEpoch: 7,
		},
	}

	// n3 is gone; active instances n1, n2, n4 remain.
	got := AssignReplicated([]string{"n1", "n2", "n4"}, 1, 3, current)
	partition, ok := got[0]
	if !ok {
		t.Fatal("missing partition 0 assignment")
	}
	if containsReplica(partition.Replicas, "n3") {
		t.Fatalf("dead replica n3 must be pruned, replicas = %v", partition.Replicas)
	}
	if len(partition.Replicas) != 3 {
		t.Fatalf("replicas = %v, want 3 (RF maintained by backfill)", partition.Replicas)
	}
	if !containsReplica(partition.Replicas, "n4") {
		t.Fatalf("expected active instance n4 to backfill the freed slot, replicas = %v", partition.Replicas)
	}
	if partition.Leader != "n1" {
		t.Fatalf("leader = %q, want n1 (native survivor, never a backfilled node)", partition.Leader)
	}
	if partition.LeaderEpoch != 7 {
		t.Fatalf("leader_epoch = %d, want 7 (leader unchanged)", partition.LeaderEpoch)
	}
}

// TestAssignReplicated_BackfillStaysNonNativeAcrossCycles verifies the native
// set is persisted so a backfilled follower is never mistaken for a native
// survivor on a later cycle and promoted. Reconstructing native-ness from the
// replica set would mark the backfill native the cycle after it was added, and
// a later rebalance (or the all-natives-dead rescue) could then promote a node
// that never held the committed prefix.
func TestAssignReplicated_BackfillStaysNonNativeAcrossCycles(t *testing.T) {
	current := map[int]PartitionAssignment{
		0: {
			Replicas:    []string{"n1", "n2", "n3"},
			Leader:      "n1",
			LeaderEpoch: 7,
		},
	}

	// Cycle 1: n3 dies, n4 is backfilled as a follower-only slot.
	got1 := AssignReplicated([]string{"n1", "n2", "n4"}, 1, 3, current)
	p1 := got1[0]
	if !containsReplica(p1.Replicas, "n4") {
		t.Fatalf("replicas = %v, want n4 backfilled", p1.Replicas)
	}
	if containsReplica(p1.Native, "n4") {
		t.Fatalf("backfilled n4 must not be native, native = %v", p1.Native)
	}

	// Cycle 2: all replicas active. The backfill must STILL be non-native, so
	// rebalance never promotes it.
	got2 := AssignReplicated([]string{"n1", "n2", "n4"}, 1, 3, got1)
	p2 := got2[0]
	if containsReplica(p2.Native, "n4") {
		t.Fatalf("backfilled n4 became native on a later cycle, native = %v", p2.Native)
	}
	if p2.Leader == "n4" {
		t.Fatalf("backfilled n4 promoted as leader on a healthy cycle, leader = %q", p2.Leader)
	}

	// Cycle 3: the natives die, leaving only the backfill active. It must not be
	// promoted; the assignment keeps the (returning) native replicas instead.
	got3 := AssignReplicated([]string{"n4"}, 1, 3, got2)
	p3 := got3[0]
	if p3.Leader == "n4" {
		t.Fatalf("backfilled n4 promoted when all natives are gone, leader = %q", p3.Leader)
	}
	if containsReplica(p3.Native, "n4") {
		t.Fatalf("backfilled n4 marked native in the rescue path, native = %v", p3.Native)
	}
}

// TestAssignReplicated_PrunesDeadLeaderPromotesNativeSurvivor verifies that a
// dead leader is replaced by an active native (pre-existing) survivor. The
// freed slot is backfilled to maintain RF, but the backfilled node is a
// follower only and never becomes leader.
func TestAssignReplicated_PrunesDeadLeaderPromotesNativeSurvivor(t *testing.T) {
	current := map[int]PartitionAssignment{
		0: {
			Replicas:    []string{"n1", "n2", "n3"},
			Leader:      "n1",
			LeaderEpoch: 4,
		},
	}

	got := AssignReplicated([]string{"n2", "n3", "n4"}, 1, 3, current)
	partition, ok := got[0]
	if !ok {
		t.Fatal("missing partition 0 assignment")
	}
	if containsReplica(partition.Replicas, "n1") {
		t.Fatalf("dead leader n1 must be pruned, replicas = %v", partition.Replicas)
	}
	if len(partition.Replicas) != 3 {
		t.Fatalf("replicas = %v, want 3 (RF maintained by backfill)", partition.Replicas)
	}
	if partition.Leader == "n1" {
		t.Fatalf("dead leader must be replaced, leader = %q", partition.Leader)
	}
	if partition.Leader == "n4" {
		t.Fatalf("backfilled node n4 must not become leader, leader = %q", partition.Leader)
	}
	if partition.Leader != "n2" && partition.Leader != "n3" {
		t.Fatalf("leader = %q, want a native survivor (n2 or n3)", partition.Leader)
	}
	if partition.LeaderEpoch <= 4 {
		t.Fatalf("leader_epoch = %d, want > 4 after leader change", partition.LeaderEpoch)
	}
}

// TestAssignReplicated_TransientShortagePromotesSurvivor verifies that when the
// active set is smaller than RF (transient outage) but at least one existing
// replica survives, the assignment keeps the surviving replica as leader rather
// than leaving the partition without a present leader.
func TestAssignReplicated_TransientShortagePromotesSurvivor(t *testing.T) {
	current := map[int]PartitionAssignment{
		0: {
			Replicas:    []string{"n1", "n2", "n3"},
			Leader:      "n1",
			LeaderEpoch: 4,
		},
	}

	// Only n2 is active; n1 (leader) is gone but n2 is an existing replica.
	got := AssignReplicated([]string{"n2"}, 1, 3, current)
	partition, ok := got[0]
	if !ok {
		t.Fatal("missing partition 0 assignment")
	}
	if partition.Leader != "n2" {
		t.Fatalf("leader = %q, want n2 (promote surviving replica)", partition.Leader)
	}
	if partition.LeaderEpoch <= 4 {
		t.Fatalf("leader_epoch = %d, want > 4 after leader change", partition.LeaderEpoch)
	}
	if !containsReplica(partition.Replicas, "n2") {
		t.Fatalf("replicas = %v, want n2 present", partition.Replicas)
	}
}
