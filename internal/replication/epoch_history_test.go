package replication

import (
	"os"
	"path/filepath"
	"testing"
)

// TestEpochHistory_DivergencePoint verifies that a follower at epoch 5, offset
// 250 is detected as diverged when the leader history has epochs [5@100,
// 6@200, 7@300]. Epoch 6 starts at offset 200, so the follower's offset of 250
// crosses into epoch 6's territory → truncate to 200.
func TestEpochHistory_DivergencePoint(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})
	eh.Append(EpochEntry{Epoch: 7, StartOffset: 300})

	truncateTo, diverged := eh.CheckDivergence(5, 250)
	if !diverged {
		t.Fatal("expected divergence, got none")
	}
	if truncateTo != 200 {
		t.Fatalf("expected truncateTo=200, got %d", truncateTo)
	}
}

func TestEpochHistory_EpochAt(t *testing.T) {
	eh := &EpochHistory{Entries: []EpochEntry{
		{Epoch: 5, StartOffset: 100},
		{Epoch: 6, StartOffset: 200},
		{Epoch: 7, StartOffset: 300},
	}}

	tests := []struct {
		offset uint64
		epoch  uint64
		found  bool
	}{
		{offset: 99},
		{offset: 100, epoch: 5, found: true},
		{offset: 199, epoch: 5, found: true},
		{offset: 200, epoch: 6, found: true},
		{offset: 299, epoch: 6, found: true},
		{offset: 300, epoch: 7, found: true},
	}
	for _, tt := range tests {
		got, found := eh.EpochAt(tt.offset)
		if got != tt.epoch || found != tt.found {
			t.Errorf("EpochAt(%d) = (%d, %t), want (%d, %t)", tt.offset, got, found, tt.epoch, tt.found)
		}
	}
}

func TestEpochHistoryEnsureRejectsConflictingOrOutOfOrderEntries(t *testing.T) {
	eh := &EpochHistory{Entries: []EpochEntry{{Epoch: 1, StartOffset: 0}}}
	if err := eh.Ensure(EpochEntry{Epoch: 1, StartOffset: 0}); err != nil {
		t.Fatalf("Ensure same boundary: %v", err)
	}
	if err := eh.Ensure(EpochEntry{Epoch: 1, StartOffset: 1}); err == nil {
		t.Fatal("expected conflicting boundary error")
	}
	if err := eh.Ensure(EpochEntry{Epoch: 2, StartOffset: 10}); err != nil {
		t.Fatalf("Ensure next boundary: %v", err)
	}
	if err := eh.Ensure(EpochEntry{Epoch: 3, StartOffset: 9}); err == nil {
		t.Fatal("expected out-of-order boundary error")
	}
}

// TestEpochHistory_NoDivergence verifies that a follower at epoch 5, offset
// 150 is NOT detected as diverged (150 < 200, the start of epoch 6).
func TestEpochHistory_NoDivergence(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})
	eh.Append(EpochEntry{Epoch: 7, StartOffset: 300})

	truncateTo, diverged := eh.CheckDivergence(5, 150)
	if diverged {
		t.Fatalf("expected no divergence, got truncateTo=%d", truncateTo)
	}
}

// TestEpochHistory_NoDivergenceAtBoundary verifies that a follower whose next
// fetch offset exactly equals the start of the next epoch is treated as
// aligned, not divergent. followerOffset is exclusive — at the boundary the
// follower has all of epoch N (offsets 100..199) and nothing yet from epoch
// N+1. A >= check would treat this as divergence and ask the follower to
// truncate to the same offset it is already at, producing an infinite
// fetch/truncate loop that surfaces as n-GB/minute log spam.
func TestEpochHistory_NoDivergenceAtBoundary(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})
	eh.Append(EpochEntry{Epoch: 7, StartOffset: 300})

	truncateTo, diverged := eh.CheckDivergence(5, 200)
	if diverged {
		t.Fatalf("expected no divergence at boundary, got truncateTo=%d", truncateTo)
	}
}

// TestEpochHistory_DivergenceOneOffBoundary verifies that a follower one
// offset past the next epoch's start *is* divergent (the follower received
// one record labeled as its own epoch that the leader recorded as belonging
// to the next epoch) — truncate back to the boundary.
func TestEpochHistory_DivergenceOneOffBoundary(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})
	eh.Append(EpochEntry{Epoch: 7, StartOffset: 300})

	truncateTo, diverged := eh.CheckDivergence(5, 201)
	if !diverged {
		t.Fatal("expected divergence one offset past boundary, got none")
	}
	if truncateTo != 200 {
		t.Fatalf("expected truncateTo=200, got %d", truncateTo)
	}
}

// Partition 0 in the live bench2 bucket contained this history after a node
// restart. Duplicate boundaries for epoch 1 are corrupt metadata, not leader
// changes; treating them as transitions causes a follower to truncate and
// re-fetch the same tail forever.
func TestEpochHistory_LiveDuplicateEpochBoundariesDoNotDiverge(t *testing.T) {
	eh := &EpochHistory{Entries: []EpochEntry{
		{Epoch: 1, StartOffset: 0},
		{Epoch: 1, StartOffset: 262144},
		{Epoch: 1, StartOffset: 262644},
		{Epoch: 1, StartOffset: 262644},
		{Epoch: 1, StartOffset: 262644},
	}}

	if truncateTo, diverged := eh.CheckDivergence(1, 263644); diverged {
		t.Fatalf("duplicate epoch history requested truncation to %d", truncateTo)
	}
}

// TestEpochHistory_Persistence verifies that SaveToFile / LoadEpochHistory
// round-trip correctly.
func TestEpochHistory_Persistence(t *testing.T) {
	original := &EpochHistory{}
	original.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	original.Append(EpochEntry{Epoch: 6, StartOffset: 200})
	original.Append(EpochEntry{Epoch: 7, StartOffset: 300})

	path := filepath.Join(t.TempDir(), "epoch_history.txt")
	if err := original.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile: %v", err)
	}

	loaded, err := LoadEpochHistory(path)
	if err != nil {
		t.Fatalf("LoadEpochHistory: %v", err)
	}

	if len(loaded.Entries) != len(original.Entries) {
		t.Fatalf("entry count mismatch: want %d, got %d", len(original.Entries), len(loaded.Entries))
	}
	for i, want := range original.Entries {
		got := loaded.Entries[i]
		if got.Epoch != want.Epoch || got.StartOffset != want.StartOffset {
			t.Errorf("entry[%d]: want {%d %d}, got {%d %d}",
				i, want.Epoch, want.StartOffset, got.Epoch, got.StartOffset)
		}
	}
}

// TestEpochHistory_LoadMissing verifies that loading from a non-existent path
// returns an empty history without an error.
func TestEpochHistory_LoadMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does_not_exist.txt")
	eh, err := LoadEpochHistory(path)
	if err != nil {
		t.Fatalf("expected no error for missing file, got: %v", err)
	}
	if len(eh.Entries) != 0 {
		t.Fatalf("expected empty entries, got %d", len(eh.Entries))
	}
}

// TestEpochHistory_TruncateAfter verifies that TruncateAfter removes entries
// with epoch > the given value.
func TestEpochHistory_TruncateAfter(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})
	eh.Append(EpochEntry{Epoch: 7, StartOffset: 300})

	eh.TruncateAfter(6)
	if len(eh.Entries) != 2 {
		t.Fatalf("expected 2 entries after truncate, got %d", len(eh.Entries))
	}
	if eh.Entries[1].Epoch != 6 {
		t.Fatalf("expected last entry epoch=6, got %d", eh.Entries[1].Epoch)
	}
}

// TestEpochHistory_LatestEpochNoDivergence verifies that a follower on the
// latest epoch is never reported as diverged (there is no next entry).
func TestEpochHistory_LatestEpochNoDivergence(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})

	// Follower is on epoch 6 (the latest) with a very high offset.
	truncateTo, diverged := eh.CheckDivergence(6, 9999)
	if diverged {
		t.Fatalf("expected no divergence for latest epoch, got truncateTo=%d", truncateTo)
	}
}

// TestEpochHistory_UnknownOlderEpochDiverges verifies that a follower reporting
// an epoch older than anything in the leader's history is treated as diverged.
func TestEpochHistory_UnknownOlderEpochDiverges(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})

	truncateTo, diverged := eh.CheckDivergence(3, 50)
	if !diverged {
		t.Fatal("expected divergence for unknown older epoch, got none")
	}
	if truncateTo != 100 {
		t.Fatalf("expected truncateTo=100 (earliest known), got %d", truncateTo)
	}
}

// TestEpochHistory_UnknownNewerEpochDiverges verifies that a follower reporting
// an epoch newer than the leader's latest is treated as diverged.
func TestEpochHistory_UnknownNewerEpochDiverges(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 6, StartOffset: 200})

	truncateTo, diverged := eh.CheckDivergence(99, 500)
	if !diverged {
		t.Fatal("expected divergence for unknown newer epoch, got none")
	}
	if truncateTo != 100 {
		t.Fatalf("expected truncateTo=100 (earliest known), got %d", truncateTo)
	}
}

// TestEpochHistory_EmptyHistoryUnknownEpoch verifies that an empty leader
// history does not report divergence (no information to fence against).
func TestEpochHistory_EmptyHistoryUnknownEpoch(t *testing.T) {
	eh := &EpochHistory{}
	_, diverged := eh.CheckDivergence(3, 50)
	if diverged {
		t.Fatal("expected no divergence with empty leader history")
	}
}

// TestEpochHistory_EmptyFile verifies that an empty saved file loads cleanly.
func TestEpochHistory_EmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.txt")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	eh, err := LoadEpochHistory(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(eh.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(eh.Entries))
	}
}

// TestEpochHistory_GapEpochDiverges verifies that a follower reporting an epoch
// that falls between known epochs (but is not in the leader's history) diverges.
func TestEpochHistory_GapEpochDiverges(t *testing.T) {
	eh := &EpochHistory{}
	eh.Append(EpochEntry{Epoch: 5, StartOffset: 100})
	eh.Append(EpochEntry{Epoch: 7, StartOffset: 300})

	truncateTo, diverged := eh.CheckDivergence(6, 200)
	if !diverged {
		t.Fatal("expected divergence for gap epoch, got none")
	}
	if truncateTo != 100 {
		t.Fatalf("expected truncateTo=100 (earliest known), got %d", truncateTo)
	}
}

// TestEpochHistory_EnsureBoundaryHealsRegressedBoundary verifies the recurring
// wedge scenario: a history whose recorded boundary lies above the durable log
// end (a promoted node's local tail lagged the committed prefix). A plain
// Ensure rejects the new boundary forever; EnsureBoundary drops the bogus
// entry and records the new epoch at the durable end.
func TestEpochHistory_EnsureBoundaryHealsRegressedBoundary(t *testing.T) {
	eh := &EpochHistory{Entries: []EpochEntry{{Epoch: 1, StartOffset: 1008506}}}
	// Durable log end is 385160 — below the recorded epoch-1 start. This is
	// the exact "epoch boundary 2@385160 is not after 1@1008506" failure.
	if err := eh.Ensure(EpochEntry{Epoch: 2, StartOffset: 385160}); err == nil {
		t.Fatal("plain Ensure must reject the regressed boundary")
	}
	if err := eh.EnsureBoundary(EpochEntry{Epoch: 2, StartOffset: 385160}); err != nil {
		t.Fatalf("EnsureBoundary must heal and accept: %v", err)
	}
	want := []EpochEntry{{Epoch: 2, StartOffset: 385160}}
	if len(eh.Entries) != 1 || eh.Entries[0] != want[0] {
		t.Fatalf("entries = %+v, want %+v", eh.Entries, want)
	}
}

// TestEpochHistory_EnsureBoundaryPreservesValidPrefix verifies that a clean
// history is extended normally and that entries below the durable end survive.
func TestEpochHistory_EnsureBoundaryPreservesValidPrefix(t *testing.T) {
	eh := &EpochHistory{Entries: []EpochEntry{
		{Epoch: 1, StartOffset: 0},
		{Epoch: 2, StartOffset: 100},
	}}
	if err := eh.EnsureBoundary(EpochEntry{Epoch: 3, StartOffset: 200}); err != nil {
		t.Fatalf("EnsureBoundary: %v", err)
	}
	want := []EpochEntry{
		{Epoch: 1, StartOffset: 0},
		{Epoch: 2, StartOffset: 100},
		{Epoch: 3, StartOffset: 200},
	}
	if len(eh.Entries) != len(want) {
		t.Fatalf("entries = %+v, want %+v", eh.Entries, want)
	}
	for i := range want {
		if eh.Entries[i] != want[i] {
			t.Fatalf("entries = %+v, want %+v", eh.Entries, want)
		}
	}
}

// TestEpochHistory_EnsureBoundaryDropsStaleGeneration verifies the
// deleted-and-recreated-topic case: stale epochs from a previous topic
// generation are dropped rather than nuking the whole history.
func TestEpochHistory_EnsureBoundaryDropsStaleGeneration(t *testing.T) {
	eh := &EpochHistory{Entries: []EpochEntry{
		{Epoch: 1, StartOffset: 0},
		{Epoch: 5, StartOffset: 100}, // stale generation: epoch >= new epoch
	}}
	if err := eh.EnsureBoundary(EpochEntry{Epoch: 3, StartOffset: 200}); err != nil {
		t.Fatalf("EnsureBoundary: %v", err)
	}
	want := []EpochEntry{
		{Epoch: 1, StartOffset: 0},
		{Epoch: 3, StartOffset: 200},
	}
	if len(eh.Entries) != len(want) {
		t.Fatalf("entries = %+v, want %+v", eh.Entries, want)
	}
	for i := range want {
		if eh.Entries[i] != want[i] {
			t.Fatalf("entries = %+v, want %+v", eh.Entries, want)
		}
	}
}

// TestEpochHistory_EnsureBoundaryIdempotent verifies recording the same
// boundary twice is a no-op (matching Ensure).
func TestEpochHistory_EnsureBoundaryIdempotent(t *testing.T) {
	eh := &EpochHistory{}
	if err := eh.EnsureBoundary(EpochEntry{Epoch: 1, StartOffset: 0}); err != nil {
		t.Fatalf("first EnsureBoundary: %v", err)
	}
	if err := eh.EnsureBoundary(EpochEntry{Epoch: 1, StartOffset: 0}); err != nil {
		t.Fatalf("second EnsureBoundary: %v", err)
	}
	if len(eh.Entries) != 1 {
		t.Fatalf("entries = %+v, want one entry", eh.Entries)
	}
}
