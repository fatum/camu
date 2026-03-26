package replication

import (
	"testing"
	"time"
)

// TestReplicaState_AdvanceHW verifies that HW is the minimum of leaderOffset
// and all ISR follower offsets.
func TestReplicaState_AdvanceHW(t *testing.T) {
	rs := NewReplicaState("a", 0, 1, 1000)
	rs.AddFollower("b")
	rs.AddFollower("c")

	// Both followers catch up at offset 10 first (join ISR).
	rs.SetLeaderOffset(10)
	rs.UpdateFollower("b", 10)
	rs.UpdateFollower("c", 10)
	// ISR = {a, b, c}, HW = 10

	// Now leader advances, followers at different offsets.
	rs.SetLeaderOffset(105)
	rs.UpdateFollower("b", 103)
	rs.UpdateFollower("c", 101)

	// HW = min(105, 103, 101) = 101
	hw := rs.HighWatermark()
	if hw != 101 {
		t.Fatalf("expected HW=101, got %d", hw)
	}
}

// TestReplicaState_ISRShrink verifies that removing a member reduces ISR size.
func TestReplicaState_ISRShrink(t *testing.T) {
	rs := NewReplicaState("a", 0, 1, 1000)
	rs.AddFollower("b")
	rs.AddFollower("c")

	// Get followers into ISR by catching up.
	rs.SetLeaderOffset(100)
	rs.UpdateFollower("b", 100)
	rs.UpdateFollower("c", 100)

	if rs.ISRSize() != 3 {
		t.Fatalf("expected ISR size 3, got %d", rs.ISRSize())
	}

	rs.RemoveFromISR("c")

	if rs.ISRSize() != 2 {
		t.Fatalf("expected ISR size 2 after removal, got %d", rs.ISRSize())
	}
	if rs.IsInISR("c") {
		t.Fatal("expected c to be removed from ISR")
	}
}

// TestReplicaState_NotifyNewData verifies the broadcast channel pattern.
func TestReplicaState_NotifyNewData(t *testing.T) {
	rs := NewReplicaState("a", 0, 1, 1000)

	notified := make(chan bool, 1)
	go func() {
		notified <- rs.WaitForData(2 * time.Second)
	}()

	time.Sleep(10 * time.Millisecond)
	rs.NotifyNewData()

	select {
	case got := <-notified:
		if !got {
			t.Fatal("expected WaitForData to return true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for notification")
	}
}

// TestReplicaState_HWOnlyISR verifies that a follower removed from the ISR
// does not constrain the HW calculation.
func TestReplicaState_HWOnlyISR(t *testing.T) {
	rs := NewReplicaState("a", 0, 1, 1000)
	rs.AddFollower("b")
	rs.AddFollower("c")

	// Both followers catch up together so ISR forms with both.
	rs.SetLeaderOffset(50)
	rs.UpdateFollower("b", 50)
	rs.UpdateFollower("c", 50)
	// HW = min(50, 50, 50) = 50

	// Leader advances, b follows, c falls behind.
	rs.SetLeaderOffset(110)
	rs.UpdateFollower("b", 108)
	// HW = min(110, 108, 50) = 50 — c is still at 50.
	if rs.HighWatermark() != 50 {
		t.Fatalf("expected HW=50 while c lags, got %d", rs.HighWatermark())
	}

	// Remove c from ISR; now HW should advance to min(110, 108) = 108.
	rs.RemoveFromISR("c")
	rs.SetLeaderOffset(110)

	if rs.HighWatermark() != 108 {
		t.Fatalf("expected HW=108 after removing c from ISR, got %d", rs.HighWatermark())
	}
}

// TestReplicaState_LeaderOnlyISR verifies that when the ISR contains only the
// leader, HW equals leaderOffset.
func TestReplicaState_LeaderOnlyISR(t *testing.T) {
	// With minISR=1, leader-only ISR can advance HW.
	rs := NewReplicaState("a", 0, 1, 1000)
	rs.SetLeaderOffset(77)
	if rs.HighWatermark() != 77 {
		t.Fatalf("expected HW=77 with leader-only ISR (minISR=1), got %d", rs.HighWatermark())
	}
}

func TestReplicaState_LeaderOnlyISR_MinISR2(t *testing.T) {
	// With minISR=2, leader-only ISR cannot advance HW.
	rs := NewReplicaState("a", 0, 2, 1000)
	rs.SetLeaderOffset(77)
	if rs.HighWatermark() != 0 {
		t.Fatalf("expected HW=0 with leader-only ISR (minISR=2), got %d", rs.HighWatermark())
	}

	// After a follower joins ISR, HW advances.
	rs.AddFollower("b")
	rs.UpdateFollower("b", 77) // catches up, joins ISR
	if rs.HighWatermark() != 77 {
		t.Fatalf("expected HW=77 after follower joins, got %d", rs.HighWatermark())
	}
}

// TestReplicaState_FollowerStartsOutsideISR verifies that AddFollower does NOT
// add to ISR — followers must catch up first.
func TestReplicaState_FollowerStartsOutsideISR(t *testing.T) {
	rs := NewReplicaState("a", 0, 2, 1000)
	rs.AddFollower("b")

	// ISR should only contain leader.
	if rs.ISRSize() != 1 {
		t.Fatalf("expected ISR size 1 (leader only), got %d", rs.ISRSize())
	}
	if rs.IsInISR("b") {
		t.Fatal("follower should not be in ISR before catching up")
	}

	// After catching up, follower joins ISR.
	rs.SetLeaderOffset(50)
	rs.UpdateFollower("b", 50)

	if rs.ISRSize() != 2 {
		t.Fatalf("expected ISR size 2 after catch-up, got %d", rs.ISRSize())
	}
	if !rs.IsInISR("b") {
		t.Fatal("follower should be in ISR after catching up")
	}
}
