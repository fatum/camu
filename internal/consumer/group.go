package consumer

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// GroupCoordinator manages consumer group membership and partition assignment.
type GroupCoordinator struct {
	mu     sync.Mutex
	groups map[string]*groupState
}

type groupState struct {
	topic         string
	members       map[string]*memberInfo
	numPartitions int
}

type memberInfo struct {
	consumerID    string
	partitions    []int
	lastHeartbeat time.Time
}

// ExpiredMember represents a member that has not sent a heartbeat within the timeout.
type ExpiredMember struct {
	GroupID    string
	ConsumerID string
}

// NewGroupCoordinator creates a new GroupCoordinator.
func NewGroupCoordinator() *GroupCoordinator {
	return &GroupCoordinator{
		groups: make(map[string]*groupState),
	}
}

// Join adds a consumer to a group and triggers a rebalance.
// Returns the partitions assigned to this consumer.
func (gc *GroupCoordinator) Join(groupID, topic, consumerID string, numPartitions int) ([]int, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gs, ok := gc.groups[groupID]
	if !ok {
		gs = &groupState{
			topic:         topic,
			members:       make(map[string]*memberInfo),
			numPartitions: numPartitions,
		}
		gc.groups[groupID] = gs
	}

	if gs.topic != topic {
		return nil, fmt.Errorf("group %q is bound to topic %q, not %q", groupID, gs.topic, topic)
	}

	// Update numPartitions if needed (e.g., topic was recreated with more partitions).
	if numPartitions > gs.numPartitions {
		gs.numPartitions = numPartitions
	}

	gs.members[consumerID] = &memberInfo{
		consumerID:    consumerID,
		lastHeartbeat: time.Now(),
	}

	gc.rebalance(gs)

	return gs.members[consumerID].partitions, nil
}

// Leave removes a consumer from a group and triggers a rebalance.
func (gc *GroupCoordinator) Leave(groupID, consumerID string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gs, ok := gc.groups[groupID]
	if !ok {
		return
	}

	delete(gs.members, consumerID)

	if len(gs.members) == 0 {
		delete(gc.groups, groupID)
		return
	}

	gc.rebalance(gs)
}

// Heartbeat updates a consumer's last heartbeat time.
func (gc *GroupCoordinator) Heartbeat(groupID, consumerID string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gs, ok := gc.groups[groupID]
	if !ok {
		return fmt.Errorf("group %q not found", groupID)
	}

	mi, ok := gs.members[consumerID]
	if !ok {
		return fmt.Errorf("consumer %q not a member of group %q", consumerID, groupID)
	}

	mi.lastHeartbeat = time.Now()
	return nil
}

// GetAssignment returns the current partition assignment for a consumer in a group.
func (gc *GroupCoordinator) GetAssignment(groupID, consumerID string) []int {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gs, ok := gc.groups[groupID]
	if !ok {
		return nil
	}

	mi, ok := gs.members[consumerID]
	if !ok {
		return nil
	}

	return mi.partitions
}

// CheckExpired finds members whose last heartbeat is older than the timeout.
func (gc *GroupCoordinator) CheckExpired(timeout time.Duration) []ExpiredMember {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	cutoff := time.Now().Add(-timeout)
	var expired []ExpiredMember

	for groupID, gs := range gc.groups {
		for cid, mi := range gs.members {
			if mi.lastHeartbeat.Before(cutoff) {
				expired = append(expired, ExpiredMember{
					GroupID:    groupID,
					ConsumerID: cid,
				})
			}
		}
	}

	return expired
}

// rebalance assigns partitions to members using round-robin.
// Must be called with gc.mu held.
func (gc *GroupCoordinator) rebalance(gs *groupState) {
	// Sort member IDs for deterministic assignment.
	memberIDs := make([]string, 0, len(gs.members))
	for id := range gs.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	// Clear existing assignments.
	for _, mi := range gs.members {
		mi.partitions = nil
	}

	// Round-robin assignment.
	for p := 0; p < gs.numPartitions; p++ {
		idx := p % len(memberIDs)
		mi := gs.members[memberIDs[idx]]
		mi.partitions = append(mi.partitions, p)
	}
}
