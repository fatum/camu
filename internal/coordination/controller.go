package coordination

import (
	"fmt"
	"sync"
)

// EpochEntry records when a new leader epoch started and at which log offset.
type EpochEntry struct {
	Epoch       uint64 `json:"epoch"`
	StartOffset uint64 `json:"start_offset"`
}

// PartitionMeta is the full in-memory state for a single partition as tracked
// by the controller.
type PartitionMeta struct {
	Leader       string       `json:"leader"`
	Epoch        uint64       `json:"epoch"`
	Replicas     []string     `json:"replicas"`
	ISR          []string     `json:"isr"`
	HW           uint64       `json:"hw"`
	EpochHistory []EpochEntry `json:"epoch_history"`
}

// ControllerState holds the authoritative partition map for the cluster.
// All mutations are guarded by an RWMutex and each mutation bumps the version
// counter so watchers can detect changes.
type ControllerState struct {
	mu         sync.RWMutex
	version    uint64
	partitions map[string]map[int]*PartitionMeta // topic -> pid -> meta
}

// NewControllerState creates an empty ControllerState.
func NewControllerState() *ControllerState {
	return &ControllerState{
		partitions: make(map[string]map[int]*PartitionMeta),
	}
}

// Version returns the current state version. Incremented on every mutation.
func (cs *ControllerState) Version() uint64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.version
}

// SetPartition stores (or replaces) the metadata for a partition and bumps the
// version.
func (cs *ControllerState) SetPartition(topic string, pid int, meta *PartitionMeta) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.partitions[topic] == nil {
		cs.partitions[topic] = make(map[int]*PartitionMeta)
	}
	cs.partitions[topic][pid] = meta
	cs.version++
}

// GetPartition returns a deep copy of the metadata for a partition, or nil if not found.
func (cs *ControllerState) GetPartition(topic string, pid int) *PartitionMeta {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.partitions[topic] == nil {
		return nil
	}
	meta := cs.partitions[topic][pid]
	if meta == nil {
		return nil
	}
	cp := cloneMeta(meta)
	return cp
}

// ElectLeader picks the first ISR member that is not failedLeader, promotes it
// to leader, resets ISR to [newLeader], bumps the epoch, appends an
// EpochEntry with StartOffset = current HW, and increments the version.
// Returns an error when the partition is not found or no eligible ISR member
// exists.
func (cs *ControllerState) ElectLeader(topic string, pid int, failedLeader string) (string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	meta, err := cs.getPartitionLocked(topic, pid)
	if err != nil {
		return "", err
	}

	var newLeader string
	for _, member := range meta.ISR {
		if member != failedLeader {
			newLeader = member
			break
		}
	}
	if newLeader == "" {
		return "", fmt.Errorf("elect leader: no eligible ISR member for %s/%d (failed: %s)",
			topic, pid, failedLeader)
	}

	meta.Epoch++
	meta.Leader = newLeader
	meta.ISR = []string{newLeader}
	meta.EpochHistory = append(meta.EpochHistory, EpochEntry{
		Epoch:       meta.Epoch,
		StartOffset: meta.HW,
	})
	cs.version++

	return newLeader, nil
}

// UpdateISR replaces the ISR slice for a partition and increments the version.
func (cs *ControllerState) UpdateISR(topic string, pid int, isr []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	meta, err := cs.getPartitionLocked(topic, pid)
	if err != nil {
		return
	}

	newISR := make([]string, len(isr))
	copy(newISR, isr)
	meta.ISR = newISR
	cs.version++
}

// UpdateHW advances the high-watermark for a partition. The update is
// monotonic: if hw is not greater than the current value the call is a no-op
// and the version is not incremented.
func (cs *ControllerState) UpdateHW(topic string, pid int, hw uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	meta, err := cs.getPartitionLocked(topic, pid)
	if err != nil {
		return
	}

	if hw <= meta.HW {
		return
	}
	meta.HW = hw
	cs.version++
}

// AllPartitions returns a deep copy of the entire partition map. Callers may
// freely mutate the returned map without affecting controller state.
func (cs *ControllerState) AllPartitions() map[string]map[int]*PartitionMeta {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	out := make(map[string]map[int]*PartitionMeta, len(cs.partitions))
	for topic, pids := range cs.partitions {
		out[topic] = make(map[int]*PartitionMeta, len(pids))
		for pid, meta := range pids {
			out[topic][pid] = cloneMeta(meta)
		}
	}
	return out
}

// TopicPartitions returns a deep copy of the partition map for a single topic.
// Returns an empty (non-nil) map when the topic is not found.
func (cs *ControllerState) TopicPartitions(topic string) map[int]*PartitionMeta {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	pids := cs.partitions[topic]
	out := make(map[int]*PartitionMeta, len(pids))
	for pid, meta := range pids {
		out[pid] = cloneMeta(meta)
	}
	return out
}

// getPartitionLocked returns the meta pointer for a partition. Must be called
// with cs.mu held (read or write).
func (cs *ControllerState) getPartitionLocked(topic string, pid int) (*PartitionMeta, error) {
	if cs.partitions[topic] == nil {
		return nil, fmt.Errorf("partition not found: topic=%q pid=%d", topic, pid)
	}
	meta := cs.partitions[topic][pid]
	if meta == nil {
		return nil, fmt.Errorf("partition not found: topic=%q pid=%d", topic, pid)
	}
	return meta, nil
}

// cloneMeta returns a deep copy of a PartitionMeta.
func cloneMeta(m *PartitionMeta) *PartitionMeta {
	if m == nil {
		return nil
	}
	cp := *m

	if m.Replicas != nil {
		cp.Replicas = make([]string, len(m.Replicas))
		copy(cp.Replicas, m.Replicas)
	}
	if m.ISR != nil {
		cp.ISR = make([]string, len(m.ISR))
		copy(cp.ISR, m.ISR)
	}
	if m.EpochHistory != nil {
		cp.EpochHistory = make([]EpochEntry, len(m.EpochHistory))
		copy(cp.EpochHistory, m.EpochHistory)
	}

	return &cp
}
