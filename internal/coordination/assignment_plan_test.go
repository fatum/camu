package coordination

import "testing"

func TestRebalancer_RoundRobin(t *testing.T) {
	got := Assign([]string{"n2", "n1", "n3"}, 7)

	want := map[string][]int{
		"n1": {0, 3, 6},
		"n2": {1, 4},
		"n3": {2, 5},
	}

	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d", len(got), len(want))
	}
	for inst, wantParts := range want {
		gotParts := got[inst]
		if len(gotParts) != len(wantParts) {
			t.Fatalf("%s len = %d, want %d", inst, len(gotParts), len(wantParts))
		}
		for i := range wantParts {
			if gotParts[i] != wantParts[i] {
				t.Fatalf("%s[%d] = %d, want %d", inst, i, gotParts[i], wantParts[i])
			}
		}
	}
}

func TestRebalancer_UnevenDistribution(t *testing.T) {
	got := Assign([]string{"n1", "n2"}, 3)
	if len(got["n1"]) != 2 || len(got["n2"]) != 1 {
		t.Fatalf("unexpected distribution: %#v", got)
	}
}

func TestRebalancer_SingleInstance(t *testing.T) {
	got := Assign([]string{"n1"}, 4)
	if len(got["n1"]) != 4 {
		t.Fatalf("got %#v, want all partitions on n1", got)
	}
}

func TestRebalancer_Empty(t *testing.T) {
	got := Assign(nil, 4)
	if len(got) != 0 {
		t.Fatalf("got %#v, want empty", got)
	}
}

func TestAssignReplicated_DeadReplicaSetReassigned(t *testing.T) {
	// When all replicas in the existing assignment are dead (not in active instances),
	// the partition should be reassigned to active instances.
	current := map[int]PartitionAssignment{
		0: {Replicas: []string{"dead-1"}, Leader: "dead-1", LeaderEpoch: 1},
		1: {Replicas: []string{"dead-1"}, Leader: "dead-1", LeaderEpoch: 1},
	}
	result := AssignReplicated([]string{"alive-1"}, 2, 1, current)

	for pid, pa := range result {
		if pa.Leader != "alive-1" {
			t.Fatalf("partition %d: leader = %q, want alive-1", pid, pa.Leader)
		}
		if len(pa.Replicas) != 1 || pa.Replicas[0] != "alive-1" {
			t.Fatalf("partition %d: replicas = %v, want [alive-1]", pid, pa.Replicas)
		}
		if pa.LeaderEpoch != 2 {
			t.Fatalf("partition %d: epoch = %d, want 2", pid, pa.LeaderEpoch)
		}
	}
}

func TestAssignReplicated_PartialDeadReplicas(t *testing.T) {
	// When some replicas are dead but one is alive, leader should move to the alive one.
	current := map[int]PartitionAssignment{
		0: {Replicas: []string{"dead-1", "alive-1"}, Leader: "dead-1", LeaderEpoch: 1},
	}
	result := AssignReplicated([]string{"alive-1"}, 1, 2, current)
	pa := result[0]
	if pa.Leader != "alive-1" {
		t.Fatalf("leader = %q, want alive-1", pa.Leader)
	}
	if pa.LeaderEpoch != 2 {
		t.Fatalf("epoch = %d, want 2", pa.LeaderEpoch)
	}
}

func TestAssignReplicated_RebalancesLeadersAcrossFullyActiveReplicas(t *testing.T) {
	current := map[int]PartitionAssignment{
		0: {Replicas: []string{"n1", "n2", "n3", "n4", "n5"}, Leader: "n1", LeaderEpoch: 1},
		1: {Replicas: []string{"n2", "n3", "n4", "n5", "n1"}, Leader: "n1", LeaderEpoch: 1},
		2: {Replicas: []string{"n3", "n4", "n5", "n1", "n2"}, Leader: "n1", LeaderEpoch: 1},
		3: {Replicas: []string{"n4", "n5", "n1", "n2", "n3"}, Leader: "n1", LeaderEpoch: 1},
	}

	got := AssignReplicated([]string{"n1", "n2", "n3", "n4", "n5"}, 4, 5, current)
	for pid, wantLeader := range []string{"n1", "n2", "n3", "n4"} {
		assignment := got[pid]
		if assignment.Leader != wantLeader {
			t.Fatalf("partition %d leader = %q, want %q", pid, assignment.Leader, wantLeader)
		}
		wantEpoch := uint64(1)
		if pid > 0 {
			wantEpoch++
		}
		if assignment.LeaderEpoch != wantEpoch {
			t.Fatalf("partition %d epoch = %d, want %d", pid, assignment.LeaderEpoch, wantEpoch)
		}
	}

	if again := AssignReplicated([]string{"n1", "n2", "n3", "n4", "n5"}, 4, 5, got); !sameAssignments(got, again) {
		t.Fatalf("rebalanced assignments changed on the next cycle: %#v", again)
	}
}

func sameAssignments(a, b map[int]PartitionAssignment) bool {
	if len(a) != len(b) {
		return false
	}
	for pid, assignment := range a {
		other, ok := b[pid]
		if !ok || assignment.Leader != other.Leader || assignment.LeaderEpoch != other.LeaderEpoch || len(assignment.Replicas) != len(other.Replicas) {
			return false
		}
		for i := range assignment.Replicas {
			if assignment.Replicas[i] != other.Replicas[i] {
				return false
			}
		}
	}
	return true
}

func TestRebalancer_Deterministic(t *testing.T) {
	a := AssignReplicated([]string{"n3", "n1", "n2"}, 4, 3, nil)
	b := AssignReplicated([]string{"n2", "n3", "n1"}, 4, 3, nil)
	if len(a) != len(b) {
		t.Fatalf("len mismatch: %d vs %d", len(a), len(b))
	}
	for pid, pa := range a {
		pb := b[pid]
		if pa.Leader != pb.Leader || pa.LeaderEpoch != pb.LeaderEpoch || len(pa.Replicas) != len(pb.Replicas) {
			t.Fatalf("partition %d mismatch: %#v vs %#v", pid, pa, pb)
		}
		for i := range pa.Replicas {
			if pa.Replicas[i] != pb.Replicas[i] {
				t.Fatalf("partition %d replica %d mismatch: %#v vs %#v", pid, i, pa, pb)
			}
		}
	}
}
