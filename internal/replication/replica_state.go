package replication

import (
	"log/slog"
	"sync"
	"time"
)

// ReplicaState tracks follower offsets, computes the high watermark, manages
// the ISR set, and holds a Purgatory for pending produce acks.
type ReplicaState struct {
	mu            sync.Mutex
	leaderID      string
	followers     map[string]*FollowerState
	isrSet        map[string]bool // includes leader
	highWatermark uint64
	leaderOffset  uint64
	minISR        int
	purgatory     *Purgatory
	newDataCh     chan struct{}
	epochHistory  *EpochHistory
}

// FollowerState holds the last known state for a single follower replica.
type FollowerState struct {
	ReplicaID     string
	LastOffset    uint64
	LastContactAt time.Time
}

// NewReplicaState creates a ReplicaState with the leader already in the ISR set.
func NewReplicaState(leaderID string, initialHW uint64, minISR int) *ReplicaState {
	return &ReplicaState{
		leaderID:      leaderID,
		followers:     make(map[string]*FollowerState),
		isrSet:        map[string]bool{leaderID: true},
		highWatermark: initialHW,
		leaderOffset:  initialHW,
		minISR:        minISR,
		purgatory:     NewPurgatory(),
		newDataCh:     make(chan struct{}),
		epochHistory:  &EpochHistory{},
	}
}

// AddFollower registers a new follower. It starts OUTSIDE the ISR set
// and must catch up to be added via AddToISR or the ISR expand check.
func (rs *ReplicaState) AddFollower(id string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.followers[id] = &FollowerState{ReplicaID: id}
}

// UpdateFollower records the latest offset acknowledged by a follower and
// recalculates the high watermark.
func (rs *ReplicaState) UpdateFollower(id string, offset uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	fs, ok := rs.followers[id]
	if !ok {
		fs = &FollowerState{ReplicaID: id}
		rs.followers[id] = fs
	}
	fs.LastOffset = offset
	fs.LastContactAt = time.Now()

	// ISR expansion: if follower is not in ISR but has caught up to within
	// 1000 offsets of the leader, add it to ISR.
	if !rs.isrSet[id] && rs.leaderOffset > 0 && offset > 0 {
		lag := rs.leaderOffset - offset
		if lag <= 1000 {
			rs.isrSet[id] = true
			slog.Info("isr_expand: follower caught up",
				"leader", rs.leaderID, "follower", id,
				"follower_offset", offset, "leader_offset", rs.leaderOffset,
				"isr_size", len(rs.isrSet))
		}
	}

	rs.advanceHW()
}

// SetLeaderOffset updates the leader's own log end offset and recalculates HW.
func (rs *ReplicaState) SetLeaderOffset(offset uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.leaderOffset = offset
	rs.advanceHW()
}

// HighWatermark returns the current high watermark.
func (rs *ReplicaState) HighWatermark() uint64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.highWatermark
}

// Purgatory returns the purgatory held by this ReplicaState.
func (rs *ReplicaState) Purgatory() *Purgatory {
	return rs.purgatory
}

// ISRSize returns the number of replicas currently in the ISR set.
func (rs *ReplicaState) ISRSize() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.isrSet)
}

// IsInISR reports whether the given replica ID is a member of the ISR set.
func (rs *ReplicaState) IsInISR(id string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.isrSet[id]
}

// RemoveFromISR removes a replica from the ISR set without removing it from
// the followers map.
func (rs *ReplicaState) RemoveFromISR(id string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.isrSet, id)
	rs.advanceHW()
}

// AddToISR adds a replica to the ISR set.
func (rs *ReplicaState) AddToISR(id string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.isrSet[id] = true
	rs.advanceHW()
}

// NotifyNewData broadcasts to all current waiters that new data is available
// by closing the current channel and replacing it with a fresh one.
func (rs *ReplicaState) NotifyNewData() {
	rs.mu.Lock()
	ch := rs.newDataCh
	rs.newDataCh = make(chan struct{})
	rs.mu.Unlock()
	close(ch)
}

// WaitForData blocks until new data is signalled or the timeout elapses.
// Returns true if data was signalled, false on timeout.
func (rs *ReplicaState) WaitForData(timeout time.Duration) bool {
	rs.mu.Lock()
	ch := rs.newDataCh
	rs.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// CheckDivergence delegates to the epoch history to detect log divergence.
func (rs *ReplicaState) CheckDivergence(followerEpoch, followerOffset uint64) (truncateTo uint64, diverged bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.epochHistory.CheckDivergence(followerEpoch, followerOffset)
}

// CheckISRLag removes any ISR follower whose last contact is older than
// lagTimeout. Returns true if the ISR set changed.
func (rs *ReplicaState) CheckISRLag(lagTimeout time.Duration) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	now := time.Now()
	changed := false
	for id := range rs.isrSet {
		if id == rs.leaderID {
			continue
		}
		fs, ok := rs.followers[id]
		if !ok {
			continue
		}
		if !fs.LastContactAt.IsZero() && now.Sub(fs.LastContactAt) > lagTimeout {
			slog.Warn("isr_shrink: removing lagging replica",
				"leader", rs.leaderID, "replica", id,
				"last_contact", fs.LastContactAt,
				"lag", now.Sub(fs.LastContactAt),
				"last_offset", fs.LastOffset)
			delete(rs.isrSet, id)
			changed = true
		}
	}
	if changed {
		rs.advanceHW()
	}
	return changed
}

// GetISRMembers returns a snapshot of the current ISR member IDs.
func (rs *ReplicaState) GetISRMembers() []string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	members := make([]string, 0, len(rs.isrSet))
	for id := range rs.isrSet {
		members = append(members, id)
	}
	return members
}

// advanceHW recalculates the high watermark as the minimum of leaderOffset and
// the LastOffset of every follower that is currently in the ISR set. If no
// followers are in the ISR set, HW equals leaderOffset.
//
// Must be called with rs.mu held.
func (rs *ReplicaState) advanceHW() {
	// Don't advance HW until ISR has enough members. Without this,
	// ISR={leader only} would set HW=leaderOffset, causing purgatory
	// to complete with only 1 copy of the data.
	if len(rs.isrSet) < rs.minISR {
		return
	}

	hw := rs.leaderOffset
	for id := range rs.isrSet {
		if id == rs.leaderID {
			continue
		}
		fs, ok := rs.followers[id]
		if !ok {
			continue
		}
		if fs.LastOffset < hw {
			hw = fs.LastOffset
		}
	}
	if hw > rs.highWatermark {
		oldHW := rs.highWatermark
		rs.highWatermark = hw
		rs.purgatory.Complete(hw)
		slog.Info("hw_advanced",
			"leader", rs.leaderID,
			"old_hw", oldHW, "new_hw", hw,
			"leader_offset", rs.leaderOffset,
			"isr_size", len(rs.isrSet))
	}
}
