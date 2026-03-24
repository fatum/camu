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
