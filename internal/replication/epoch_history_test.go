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
