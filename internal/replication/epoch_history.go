package replication

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/maksim/camu/internal/fsutil"
)

// EpochEntry records the start offset of a leader epoch.
type EpochEntry struct {
	Epoch       uint64
	StartOffset uint64
}

// EpochHistory tracks the sequence of leader epochs and their starting offsets.
// It is used to detect log divergence between a leader and a follower.
//
// Note: EpochHistory is not safe for concurrent use. Callers must ensure that
// all mutations (Append, TruncateAfter) and reads (CheckDivergence) are serialized.
// In practice, this is guaranteed by:
//   - Append/TruncateAfter are only called during leadership acquisition, which is
//     serialized by the partition's leadership state machine
//   - CheckDivergence is called under ReplicaState.mu lock
type EpochHistory struct {
	Entries []EpochEntry
}

// Append adds a new entry to the epoch history.
func (eh *EpochHistory) Append(entry EpochEntry) {
	eh.Entries = append(eh.Entries, entry)
}

// EpochAt returns the leader epoch containing offset. An epoch begins at its
// StartOffset and remains current until the next entry begins.
func (eh *EpochHistory) EpochAt(offset uint64) (uint64, bool) {
	var epoch uint64
	found := false
	for _, entry := range eh.Entries {
		if entry.StartOffset > offset {
			break
		}
		epoch = entry.Epoch
		found = true
	}
	return epoch, found
}

// CheckDivergence determines whether a follower has divergent data relative to
// this (leader) epoch history.
//
// followerOffset is the "next offset to fetch" — the follower holds all
// records strictly below followerOffset. The check finds the entry matching
// followerEpoch, then examines the next entry's StartOffset:
//
//   - followerOffset <= next.StartOffset: the follower has at most all of
//     epoch N (offsets < next.StartOffset) and nothing yet from epoch N+1 —
//     no divergence. Equality is *alignment at the boundary*: follower has
//     everything epoch N produced and is ready to fetch epoch N+1.
//   - followerOffset > next.StartOffset: the follower has some offsets in
//     epoch N+1's range labeled as its own epoch N — divergent, truncate
//     back to next.StartOffset.
//
// The strict-greater comparison is important: using >= causes an infinite
// loop when a follower sits exactly on the boundary — the leader tells it
// to truncate to next.StartOffset, which equals its current offset (no
// actual truncation), so it re-fetches with the same offset and receives
// the same response forever.
//
// Returns (truncateTo, true) when divergence is detected, or (0, false) when
// the follower is consistent with the leader.
func (eh *EpochHistory) CheckDivergence(followerEpoch uint64, followerOffset uint64) (truncateTo uint64, diverged bool) {
	for i, entry := range eh.Entries {
		if entry.Epoch != followerEpoch {
			continue
		}
		// Found the matching epoch. Check whether there is a next entry.
		if i+1 >= len(eh.Entries) {
			// followerEpoch is the latest epoch — no divergence possible.
			return 0, false
		}
		next := eh.Entries[i+1]
		if followerOffset > next.StartOffset {
			return next.StartOffset, true
		}
		return 0, false
	}
	// followerEpoch not found in leader's history.
	if len(eh.Entries) > 0 {
		if followerEpoch < eh.Entries[0].Epoch {
			// Follower has an epoch older than anything the leader knows —
			// truncate to the start of the earliest known epoch.
			return eh.Entries[0].StartOffset, true
		}
		// Follower has an epoch newer than the leader's latest — logic error,
		// force full resync from earliest known offset.
		return eh.Entries[0].StartOffset, true
	}
	return 0, false
}

// TruncateAfter removes all entries whose Epoch is strictly greater than epoch.
func (eh *EpochHistory) TruncateAfter(epoch uint64) {
	cutoff := len(eh.Entries)
	for i, entry := range eh.Entries {
		if entry.Epoch > epoch {
			cutoff = i
			break
		}
	}
	eh.Entries = eh.Entries[:cutoff]
}

// SaveToFile writes the epoch history to the given file path.
// Format: one line per entry — "{epoch} {start_offset}\n"
func (eh *EpochHistory) SaveToFile(path string) error {
	var b strings.Builder
	for _, entry := range eh.Entries {
		if _, err := fmt.Fprintf(&b, "%d %d\n", entry.Epoch, entry.StartOffset); err != nil {
			return fmt.Errorf("epoch history write: %w", err)
		}
	}
	if err := fsutil.AtomicWriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("epoch history save: %w", err)
	}
	return nil
}

// LoadEpochHistory reads an epoch history from path.
// If the file does not exist, an empty EpochHistory is returned without error.
func LoadEpochHistory(path string) (*EpochHistory, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &EpochHistory{}, nil
		}
		return nil, fmt.Errorf("epoch history load: %w", err)
	}
	defer f.Close()

	eh := &EpochHistory{}
	for {
		var entry EpochEntry
		_, err := fmt.Fscanf(f, "%d %d\n", &entry.Epoch, &entry.StartOffset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("epoch history parse: %w", err)
		}
		eh.Entries = append(eh.Entries, entry)
	}
	return eh, nil
}
