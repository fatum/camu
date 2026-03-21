package coordination

import (
	"testing"
)

func TestRebalancer_RoundRobin(t *testing.T) {
	assignments := Assign([]string{"i1", "i2", "i3"}, 9)

	for _, inst := range []string{"i1", "i2", "i3"} {
		if len(assignments[inst]) != 3 {
			t.Errorf("instance %q: expected 3 partitions, got %d", inst, len(assignments[inst]))
		}
	}

	total := 0
	for _, parts := range assignments {
		total += len(parts)
	}
	if total != 9 {
		t.Errorf("expected total 9 partitions, got %d", total)
	}
}

func TestRebalancer_UnevenDistribution(t *testing.T) {
	assignments := Assign([]string{"i1", "i2"}, 5)

	total := 0
	for _, parts := range assignments {
		total += len(parts)
	}
	if total != 5 {
		t.Errorf("expected total 5 partitions, got %d", total)
	}

	counts := map[int]int{}
	for _, parts := range assignments {
		counts[len(parts)]++
	}
	// One instance gets 3, one gets 2.
	if counts[3] != 1 || counts[2] != 1 {
		t.Errorf("expected distribution of [3,2], got counts: %v", counts)
	}
}

func TestRebalancer_SingleInstance(t *testing.T) {
	assignments := Assign([]string{"i1"}, 4)

	if len(assignments["i1"]) != 4 {
		t.Errorf("expected 4 partitions for i1, got %d", len(assignments["i1"]))
	}
}

func TestRebalancer_Empty(t *testing.T) {
	if len(Assign([]string{}, 5)) != 0 {
		t.Error("expected empty assignment for no instances")
	}
	if len(Assign([]string{"i1"}, 0)) != 1 {
		t.Error("expected map with instance key but no partitions for 0 partitions")
	}
	if len(Assign([]string{"i1"}, 0)["i1"]) != 0 {
		t.Error("expected 0 partitions for 0 numPartitions")
	}
}

func TestRebalancer_Deterministic(t *testing.T) {
	a := Assign([]string{"z1", "a1", "m1"}, 6)
	b := Assign([]string{"m1", "z1", "a1"}, 6)

	for inst, partsA := range a {
		partsB := b[inst]
		if len(partsA) != len(partsB) {
			t.Errorf("instance %q: got different lengths %d vs %d", inst, len(partsA), len(partsB))
			continue
		}
		for i := range partsA {
			if partsA[i] != partsB[i] {
				t.Errorf("instance %q partition[%d]: %d vs %d", inst, i, partsA[i], partsB[i])
			}
		}
	}
}
