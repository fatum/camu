package consumer

import (
	"sort"
	"testing"
	"time"
)

func TestGroupCoordinator_JoinAndAssign(t *testing.T) {
	gc := NewGroupCoordinator()

	// First consumer joins — should get all 4 partitions.
	parts, err := gc.Join("g1", "topic-a", "c1", 4)
	if err != nil {
		t.Fatalf("Join c1: %v", err)
	}
	sort.Ints(parts)
	if len(parts) != 4 {
		t.Fatalf("c1 expected 4 partitions, got %d: %v", len(parts), parts)
	}
	for i := 0; i < 4; i++ {
		if parts[i] != i {
			t.Errorf("c1 partition %d: got %d, want %d", i, parts[i], i)
		}
	}

	// Second consumer joins — partitions should be split.
	parts2, err := gc.Join("g1", "topic-a", "c2", 4)
	if err != nil {
		t.Fatalf("Join c2: %v", err)
	}

	// After rebalance, c1 should also have updated assignment.
	parts1 := gc.GetAssignment("g1", "c1")

	// Each should get ~2 partitions, total should be 4.
	all := append(parts1, parts2...)
	sort.Ints(all)
	if len(all) != 4 {
		t.Fatalf("total partitions: got %d, want 4", len(all))
	}
	for i := 0; i < 4; i++ {
		if all[i] != i {
			t.Errorf("all[%d] = %d, want %d", i, all[i], i)
		}
	}

	if len(parts1) < 1 || len(parts1) > 3 {
		t.Errorf("c1 should have 1-3 partitions, got %d", len(parts1))
	}
	if len(parts2) < 1 || len(parts2) > 3 {
		t.Errorf("c2 should have 1-3 partitions, got %d", len(parts2))
	}
}

func TestGroupCoordinator_Leave(t *testing.T) {
	gc := NewGroupCoordinator()

	gc.Join("g1", "topic-a", "c1", 4)
	gc.Join("g1", "topic-a", "c2", 4)

	// c2 leaves.
	gc.Leave("g1", "c2")

	// c1 should now have all 4 partitions again.
	parts := gc.GetAssignment("g1", "c1")
	sort.Ints(parts)
	if len(parts) != 4 {
		t.Fatalf("after leave: c1 expected 4 partitions, got %d", len(parts))
	}
	for i := 0; i < 4; i++ {
		if parts[i] != i {
			t.Errorf("partition %d: got %d", i, parts[i])
		}
	}

	// c2 should have no assignment.
	parts2 := gc.GetAssignment("g1", "c2")
	if len(parts2) != 0 {
		t.Errorf("c2 after leave should have 0 partitions, got %d", len(parts2))
	}
}

func TestGroupCoordinator_Heartbeat(t *testing.T) {
	gc := NewGroupCoordinator()

	// Heartbeat on non-existent group should fail.
	if err := gc.Heartbeat("no-group", "c1"); err == nil {
		t.Error("expected error for heartbeat on non-existent group")
	}

	gc.Join("g1", "topic-a", "c1", 4)

	if err := gc.Heartbeat("g1", "c1"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Heartbeat for non-member should fail.
	if err := gc.Heartbeat("g1", "c99"); err == nil {
		t.Error("expected error for heartbeat from non-member")
	}
}

func TestGroupCoordinator_CheckExpired(t *testing.T) {
	gc := NewGroupCoordinator()
	gc.Join("g1", "topic-a", "c1", 4)

	// With a very short timeout, the member should not yet be expired.
	expired := gc.CheckExpired(1 * time.Hour)
	if len(expired) != 0 {
		t.Errorf("expected 0 expired, got %d", len(expired))
	}

	// With 0 timeout, everything is expired.
	expired = gc.CheckExpired(0)
	if len(expired) != 1 {
		t.Errorf("expected 1 expired, got %d", len(expired))
	}
}
