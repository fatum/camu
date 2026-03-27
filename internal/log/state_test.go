package log

import "testing"

func TestPartitionState_RoundTrip(t *testing.T) {
	s := PartitionState{
		HighWatermark: 1000,
		EpochHistory:  []EpochEntry{{Epoch: 1, StartOffset: 0}, {Epoch: 2, StartOffset: 500}},
	}
	data, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var s2 PartitionState
	if err := s2.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	if s2.HighWatermark != 1000 {
		t.Fatalf("hw=%d", s2.HighWatermark)
	}
	if len(s2.EpochHistory) != 2 || s2.EpochHistory[1].Epoch != 2 {
		t.Fatalf("epochs=%v", s2.EpochHistory)
	}
}

func TestPartitionState_Empty(t *testing.T) {
	var s PartitionState
	data, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var s2 PartitionState
	if err := s2.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	if s2.HighWatermark != 0 || len(s2.EpochHistory) != 0 {
		t.Fatalf("expected empty state, got hw=%d epochs=%v", s2.HighWatermark, s2.EpochHistory)
	}
}

func TestStateKey(t *testing.T) {
	key := StateKey("orders", 0)
	if key != "orders/0/state.json" {
		t.Fatalf("got %q", key)
	}
}
