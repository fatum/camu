package coordination

import "sort"

// Assign distributes numPartitions across instances using round-robin.
// Returns a map from instanceID to the list of partition IDs assigned to it.
// Instances are sorted for deterministic output.
func Assign(instances []string, numPartitions int) map[string][]int {
	result := make(map[string][]int, len(instances))

	if len(instances) == 0 {
		return result
	}

	sorted := make([]string, len(instances))
	copy(sorted, instances)
	sort.Strings(sorted)

	for _, inst := range sorted {
		result[inst] = nil
	}

	for p := 0; p < numPartitions; p++ {
		inst := sorted[p%len(sorted)]
		result[inst] = append(result[inst], p)
	}

	return result
}

// AssignReplicated computes partition assignments with replication.
// Returns partitionID -> PartitionAssignment with replica placement.
func AssignReplicated(instances []string, numPartitions int, replicationFactor int, current map[int]PartitionAssignment) map[int]PartitionAssignment {
	sort.Strings(instances)
	n := len(instances)
	rf := min(replicationFactor, n)
	result := make(map[int]PartitionAssignment, numPartitions)
	// eligible tracks, per partition, the set of replicas that may hold
	// leadership: the pre-existing (native) survivors. A node backfilled into a
	// partition's replica set to maintain RF is a follower only — it has never
	// held the partition's committed prefix, and promoting it (or letting
	// rebalance promote it) would truncate committed data.
	eligible := make(map[int]map[string]bool, numPartitions)

	for pid := range numPartitions {
		replicas := make([]string, 0, rf)
		leader := ""
		leaderEpoch := uint64(1)
		if current != nil {
			if cur, ok := current[pid]; ok && len(cur.Replicas) > 0 {
				activeSet := make(map[string]struct{}, len(instances))
				for _, id := range instances {
					activeSet[id] = struct{}{}
				}

				// The native (pre-existing) replicas are the only leadership
				// candidates: a node backfilled into the replica set to
				// maintain RF is a follower only until it catches up, and
				// promoting it before then would truncate committed data. The
				// native set is taken from the persisted assignment, NOT
				// reconstructed from the replica set, so a backfill is never
				// mistaken for a native survivor on a later cycle and promoted.
				// Assignments without a persisted native set (fresh, or written
				// before the field existed) conservatively treat the whole
				// replica set as native.
				prevNative := cur.Native
				if len(prevNative) == 0 {
					prevNative = cur.Replicas
				}

				// Prune replicas that are no longer active so a vanished node
				// does not linger in the assignment forever (it would keep its
				// instance registration referenced — never garbage collected —
				// and advertise a dead replica to readers). The surviving
				// (native) replicas are the only leadership candidates.
				//
				// Pruning and backfill only happen when there are enough active
				// instances to maintain the intended replication factor
				// (n >= replicationFactor). With fewer active instances, the
				// active set is transiently degraded: preserve the original
				// replica set unchanged so returning nodes rejoin without churn
				// and GC does not collect them while the cluster is
				// under-provisioned. (Note: rf is clamped to n for fresh
				// assignment, but this preservation decision must compare
				// against the configured replicationFactor.)
				native := make(map[string]bool)
				if n >= replicationFactor {
					for _, r := range prevNative {
						if _, ok := activeSet[r]; ok {
							replicas = append(replicas, r)
							native[r] = true
						}
					}
				} else {
					replicas = append(replicas, cur.Replicas...)
					for _, r := range prevNative {
						native[r] = true
					}
				}
				// If every native replica is gone (and there were enough active
				// instances to prune), keep the original set rather than
				// introducing a brand-new node as leader: an existing partition
				// must not be handed to a node that never held its data. The
				// partition stays on its (returning) replicas, and GC does not
				// collect them while the cluster is degraded.
				if len(replicas) == 0 {
					replicas = append(replicas, cur.Replicas...)
					for _, r := range prevNative {
						native[r] = true
					}
				}

				// Backfill freed slots with active instances up to RF so the
				// routing response keeps advertising full replication. These
				// nodes are followers only; they are not leadership-eligible.
				if len(replicas) < rf && n >= rf {
					used := make(map[string]struct{}, len(replicas))
					for _, r := range replicas {
						used[r] = struct{}{}
					}
					for i := 0; len(replicas) < rf; i++ {
						cand := instances[i%n]
						if _, ok := used[cand]; ok {
							continue
						}
						used[cand] = struct{}{}
						replicas = append(replicas, cand)
					}
				}

				leader = cur.Leader
				leaderEpoch = cur.LeaderEpoch

				// Keep the current leader only if it is still active, present,
				// and a native (pre-existing) replica. Never promote a
				// backfilled node: it does not hold the committed prefix.
				switch {
				case containsReplica(instances, leader) && native[leader]:
					// leader unchanged
				default:
					if nextLeader, ok := firstNativeActiveReplica(replicas, native, activeSet); ok {
						leader = nextLeader
						leaderEpoch++
					} else if len(replicas) > 0 && !containsReplica(replicas, leader) {
						// Transient shortage with no active native replica:
						// name a surviving native replica as leader only if the
						// current leader is no longer a replica at all.
						for _, r := range replicas {
							if native[r] {
								leader = r
								leaderEpoch++
								break
							}
						}
					}
					// else: no surviving native replica — keep the current
					// leader and existing replica set hoping the node comes
					// back.
				}

				if len(replicas) > 0 {
					result[pid] = PartitionAssignment{
						Replicas:    replicas,
						Native:      nativeInReplicaOrder(replicas, native),
						Leader:      leader,
						LeaderEpoch: leaderEpoch,
					}
					eligible[pid] = native
					continue
				}
			}
		}
		for r := range rf {
			replicas = append(replicas, instances[(pid+r)%n])
		}
		leader = replicas[0]
		result[pid] = PartitionAssignment{
			Replicas:    replicas,
			Native:      append([]string(nil), replicas...),
			Leader:      leader,
			LeaderEpoch: leaderEpoch,
		}
		eligible[pid] = nil // fresh partition: no native-exclusivity constraint
	}
	rebalanceLeaders(result, activeSet(instances), eligible)
	return result
}

// nativeInReplicaOrder returns the native members of replicas in replica order.
func nativeInReplicaOrder(replicas []string, native map[string]bool) []string {
	out := make([]string, 0, len(native))
	for _, r := range replicas {
		if native[r] {
			out = append(out, r)
		}
	}
	return out
}

func activeSet(instances []string) map[string]struct{} {
	m := make(map[string]struct{}, len(instances))
	for _, id := range instances {
		m[id] = struct{}{}
	}
	return m
}

// firstNativeActiveReplica returns the first replica that is both native
// (pre-existing) and currently active.
func firstNativeActiveReplica(replicas []string, native map[string]bool, active map[string]struct{}) (string, bool) {
	for _, r := range replicas {
		if !native[r] {
			continue
		}
		if _, ok := active[r]; ok {
			return r, true
		}
	}
	return "", false
}

// AssignDiskless computes single-replica assignments for a diskless topic,
// spreading the leader of every partition across the active instances. Diskless
// topics are stateless (data lives in the shared object store), so there is no
// replica set to preserve: leaders are always recomputed from the current active
// set so a topic created while the cluster was partially up self-heals to a
// spread once the full cluster is active. The leader epoch is preserved when the
// leader is unchanged and bumped otherwise, so maintenance jobs that gate on
// (leader, epoch) are fenced correctly.
func AssignDiskless(instances []string, numPartitions int, current map[int]PartitionAssignment) map[int]PartitionAssignment {
	sort.Strings(instances)
	n := len(instances)
	result := make(map[int]PartitionAssignment, numPartitions)
	if n == 0 {
		return result
	}
	for pid := range numPartitions {
		leader := instances[pid%n]
		epoch := uint64(1)
		if cur, ok := current[pid]; ok {
			if cur.Leader == leader {
				epoch = cur.LeaderEpoch
			} else {
				epoch = cur.LeaderEpoch + 1
			}
		}
		result[pid] = PartitionAssignment{
			Replicas:    []string{leader},
			Native:      []string{leader},
			Leader:      leader,
			LeaderEpoch: epoch,
		}
	}
	return result
}

// rebalanceLeaders spreads leadership across fully active replica sets, but
// only among each partition's native (pre-existing) replicas: a backfilled
// node is a follower only and must never be promoted, because it has not held
// the partition's committed prefix. A partition with no native replicas (all
// pre-existing nodes gone) is left alone to avoid leadership churn during
// recovery. It is deterministic, so once assignments are balanced, subsequent
// coordination cycles leave their leaders unchanged.
func rebalanceLeaders(assignments map[int]PartitionAssignment, active map[string]struct{}, eligible map[int]map[string]bool) {
	leaders := make(map[string]int, len(active))
	for instance := range active {
		leaders[instance] = 0
	}

	for pid := 0; pid < len(assignments); pid++ {
		assignment, ok := assignments[pid]
		if !ok || !allReplicasActive(assignment.Replicas, active) {
			continue
		}
		native := eligible[pid]
		// With no native-exclusivity set (fresh partition), every replica is a
		// valid leader candidate.
		if native == nil {
			leader := assignment.Replicas[0]
			for _, replica := range assignment.Replicas[1:] {
				if leaders[replica] < leaders[leader] {
					leader = replica
				}
			}
			if assignment.Leader != leader {
				assignment.Leader = leader
				assignment.LeaderEpoch++
				assignments[pid] = assignment
			}
			leaders[leader]++
			continue
		}

		// Only native replicas are leadership candidates.
		var best string
		for _, replica := range assignment.Replicas {
			if !native[replica] {
				continue
			}
			if best == "" || leaders[replica] < leaders[best] {
				best = replica
			}
		}
		if best == "" {
			continue // no native replica active: leave the assignment alone
		}
		if assignment.Leader != best {
			assignment.Leader = best
			assignment.LeaderEpoch++
			assignments[pid] = assignment
		}
		leaders[best]++
	}
}

func allReplicasActive(replicas []string, active map[string]struct{}) bool {
	for _, replica := range replicas {
		if _, ok := active[replica]; !ok {
			return false
		}
	}
	return true
}

func containsReplica(replicas []string, leader string) bool {
	for _, replica := range replicas {
		if replica == leader {
			return true
		}
	}
	return false
}
