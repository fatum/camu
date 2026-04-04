package coordination

import (
	"testing"
)

func newTestMeta(leader string, epoch uint64, replicas, isr []string, hw uint64) *PartitionMeta {
	return &PartitionMeta{
		Leader:       leader,
		Epoch:        epoch,
		Replicas:     replicas,
		ISR:          isr,
		HW:           hw,
		EpochHistory: []EpochEntry{},
	}
}

func TestControllerState_AddPartition(t *testing.T) {
	cs := NewControllerState()
	meta := newTestMeta("node1", 1, []string{"node1", "node2"}, []string{"node1", "node2"}, 0)

	cs.SetPartition("my-topic", 0, meta)

	got := cs.GetPartition("my-topic", 0)
	if got == nil {
		t.Fatal("expected non-nil partition meta")
	}
	if got.Leader != "node1" {
		t.Errorf("leader: got %q, want %q", got.Leader, "node1")
	}
	if got.Epoch != 1 {
		t.Errorf("epoch: got %d, want 1", got.Epoch)
	}
}

func TestControllerState_GetPartition_NotFound(t *testing.T) {
	cs := NewControllerState()

	got := cs.GetPartition("missing-topic", 0)
	if got != nil {
		t.Errorf("expected nil for unknown partition, got %+v", got)
	}

	cs.SetPartition("my-topic", 0, newTestMeta("node1", 1, nil, nil, 0))
	got = cs.GetPartition("my-topic", 99)
	if got != nil {
		t.Errorf("expected nil for unknown partition id, got %+v", got)
	}
}

func TestControllerState_ElectLeader(t *testing.T) {
	cs := NewControllerState()
	meta := newTestMeta("node1", 1,
		[]string{"node1", "node2", "node3"},
		[]string{"node1", "node2", "node3"},
		42,
	)
	cs.SetPartition("topic", 0, meta)

	vBefore := cs.Version()

	newLeader, err := cs.ElectLeader("topic", 0, "node1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newLeader != "node2" {
		t.Errorf("new leader: got %q, want %q", newLeader, "node2")
	}

	got := cs.GetPartition("topic", 0)
	if got.Leader != "node2" {
		t.Errorf("stored leader: got %q, want %q", got.Leader, "node2")
	}
	if got.Epoch != 2 {
		t.Errorf("epoch after elect: got %d, want 2", got.Epoch)
	}
	if len(got.ISR) != 1 || got.ISR[0] != "node2" {
		t.Errorf("ISR after elect: got %v, want [node2]", got.ISR)
	}

	// Epoch history should have an entry with StartOffset = HW at time of election.
	if len(got.EpochHistory) != 1 {
		t.Fatalf("epoch history length: got %d, want 1", len(got.EpochHistory))
	}
	if got.EpochHistory[0].Epoch != 2 {
		t.Errorf("history epoch: got %d, want 2", got.EpochHistory[0].Epoch)
	}
	if got.EpochHistory[0].StartOffset != 42 {
		t.Errorf("history start offset: got %d, want 42 (HW)", got.EpochHistory[0].StartOffset)
	}

	if cs.Version() <= vBefore {
		t.Errorf("version should have incremented after ElectLeader")
	}
}

func TestControllerState_ElectLeader_NoEligible(t *testing.T) {
	cs := NewControllerState()
	// Only the failed leader is in the ISR.
	meta := newTestMeta("node1", 1,
		[]string{"node1"},
		[]string{"node1"},
		0,
	)
	cs.SetPartition("topic", 0, meta)

	_, err := cs.ElectLeader("topic", 0, "node1")
	if err == nil {
		t.Fatal("expected error when no eligible ISR member, got nil")
	}
}

func TestControllerState_ElectLeader_NotFound(t *testing.T) {
	cs := NewControllerState()

	_, err := cs.ElectLeader("no-topic", 0, "node1")
	if err == nil {
		t.Fatal("expected error for unknown partition, got nil")
	}
}

func TestControllerState_UpdateISR(t *testing.T) {
	cs := NewControllerState()
	cs.SetPartition("topic", 0, newTestMeta("node1", 1,
		[]string{"node1", "node2", "node3"},
		[]string{"node1", "node2", "node3"},
		0,
	))

	vBefore := cs.Version()
	cs.UpdateISR("topic", 0, []string{"node1", "node2"})

	got := cs.GetPartition("topic", 0)
	if len(got.ISR) != 2 {
		t.Errorf("ISR length: got %d, want 2", len(got.ISR))
	}
	if got.ISR[0] != "node1" || got.ISR[1] != "node2" {
		t.Errorf("ISR: got %v, want [node1 node2]", got.ISR)
	}
	if cs.Version() <= vBefore {
		t.Errorf("version should have incremented after UpdateISR")
	}
}

func TestControllerState_UpdateHW(t *testing.T) {
	cs := NewControllerState()
	cs.SetPartition("topic", 0, newTestMeta("node1", 1, nil, nil, 10))

	// Higher HW should be accepted.
	vBefore := cs.Version()
	cs.UpdateHW("topic", 0, 20)
	got := cs.GetPartition("topic", 0)
	if got.HW != 20 {
		t.Errorf("HW after forward update: got %d, want 20", got.HW)
	}
	if cs.Version() <= vBefore {
		t.Errorf("version should increment when HW advances")
	}

	// Lower HW should be rejected (monotonic).
	vBefore = cs.Version()
	cs.UpdateHW("topic", 0, 5)
	got = cs.GetPartition("topic", 0)
	if got.HW != 20 {
		t.Errorf("HW after backward attempt: got %d, want 20 (monotonic)", got.HW)
	}
	if cs.Version() != vBefore {
		t.Errorf("version should NOT increment when HW does not advance")
	}

	// Equal HW should also be a no-op.
	cs.UpdateHW("topic", 0, 20)
	if cs.Version() != vBefore {
		t.Errorf("version should NOT increment when HW is equal")
	}
}

func TestControllerState_AllPartitions(t *testing.T) {
	cs := NewControllerState()
	cs.SetPartition("topic-a", 0, newTestMeta("node1", 1, nil, nil, 0))
	cs.SetPartition("topic-a", 1, newTestMeta("node2", 1, nil, nil, 0))
	cs.SetPartition("topic-b", 0, newTestMeta("node3", 1, nil, nil, 0))

	snap := cs.AllPartitions()

	// Verify structure.
	if len(snap) != 2 {
		t.Errorf("topic count: got %d, want 2", len(snap))
	}
	if len(snap["topic-a"]) != 2 {
		t.Errorf("topic-a partition count: got %d, want 2", len(snap["topic-a"]))
	}

	// Modify the copy — original should be unchanged.
	snap["topic-a"][0].Leader = "mutated"
	snap["new-topic"] = map[int]*PartitionMeta{}

	orig := cs.GetPartition("topic-a", 0)
	if orig.Leader == "mutated" {
		t.Error("AllPartitions returned a shallow copy; mutating it affected the original")
	}
	if cs.AllPartitions()["new-topic"] != nil {
		t.Error("AllPartitions snapshot mutation leaked into controller state")
	}
}

func TestControllerState_TopicPartitions(t *testing.T) {
	cs := NewControllerState()
	cs.SetPartition("topic-a", 0, newTestMeta("node1", 1, nil, nil, 0))
	cs.SetPartition("topic-a", 1, newTestMeta("node2", 1, nil, nil, 0))

	snap := cs.TopicPartitions("topic-a")
	if len(snap) != 2 {
		t.Errorf("partition count: got %d, want 2", len(snap))
	}

	// Missing topic returns empty map.
	empty := cs.TopicPartitions("no-topic")
	if len(empty) != 0 {
		t.Errorf("expected empty map for unknown topic, got %v", empty)
	}

	// Modify copy — original unchanged.
	snap[0].Leader = "mutated"
	orig := cs.GetPartition("topic-a", 0)
	if orig.Leader == "mutated" {
		t.Error("TopicPartitions returned a shallow copy; mutation affected original")
	}
}

func TestControllerState_Version(t *testing.T) {
	cs := NewControllerState()
	if cs.Version() != 0 {
		t.Errorf("initial version: got %d, want 0", cs.Version())
	}

	cs.SetPartition("topic", 0, newTestMeta("node1", 1, []string{"node1"}, []string{"node1"}, 0))
	if cs.Version() != 1 {
		t.Errorf("after SetPartition: got %d, want 1", cs.Version())
	}

	cs.UpdateISR("topic", 0, []string{"node1"})
	if cs.Version() != 2 {
		t.Errorf("after UpdateISR: got %d, want 2", cs.Version())
	}

	cs.UpdateHW("topic", 0, 100)
	if cs.Version() != 3 {
		t.Errorf("after UpdateHW: got %d, want 3", cs.Version())
	}

	_, err := cs.ElectLeader("topic", 0, "node1")
	// node1 is the only ISR member — expect error, no version bump.
	if err == nil {
		t.Fatal("expected error electing with sole ISR member")
	}
	if cs.Version() != 3 {
		t.Errorf("version after failed ElectLeader: got %d, want 3", cs.Version())
	}
}
