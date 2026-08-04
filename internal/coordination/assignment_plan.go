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

	for pid := range numPartitions {
		replicas := make([]string, 0, rf)
		leader := ""
		leaderEpoch := uint64(1)
		if current != nil {
			if cur, ok := current[pid]; ok && len(cur.Replicas) > 0 {
				// Preserve the existing replica set for safety. A transiently smaller
				// active set must not rewrite the partition onto unrelated nodes.
				replicas = append(replicas, cur.Replicas...)
				leader = cur.Leader
				leaderEpoch = cur.LeaderEpoch
				if !containsReplica(replicas, leader) {
					leader = replicas[0]
				}
				if !containsReplica(instances, leader) {
					if nextLeader, ok := firstActiveReplica(replicas, instances); ok {
						leader = nextLeader
						leaderEpoch++
					} else if n >= replicationFactor {
						// No active replica in the existing set and we have enough
						// active instances to satisfy RF — reassign entirely.
						replicas = replicas[:0]
						for r := range rf {
							replicas = append(replicas, instances[(pid+r)%n])
						}
						leader = replicas[0]
						leaderEpoch++
					}
					// else: transient state — fewer active instances than RF.
					// Preserve existing replicas hoping they come back.
				}
				result[pid] = PartitionAssignment{
					Replicas:    replicas,
					Leader:      leader,
					LeaderEpoch: leaderEpoch,
				}
				continue
			}
		}
		for r := range rf {
			replicas = append(replicas, instances[(pid+r)%n])
		}
		leader = replicas[0]
		result[pid] = PartitionAssignment{
			Replicas:    replicas,
			Leader:      leader,
			LeaderEpoch: leaderEpoch,
		}
	}
	rebalanceLeaders(result, instances)
	return result
}

// rebalanceLeaders spreads leadership across fully active replica sets. It is
// deterministic, so once assignments are balanced, subsequent coordination
// cycles leave their leaders unchanged. A partially available replica set is
// left alone to avoid unnecessary leadership churn during recovery.
func rebalanceLeaders(assignments map[int]PartitionAssignment, active []string) {
	activeSet := make(map[string]struct{}, len(active))
	for _, instance := range active {
		activeSet[instance] = struct{}{}
	}
	leaders := make(map[string]int, len(active))
	for _, instance := range active {
		leaders[instance] = 0
	}

	for pid := 0; pid < len(assignments); pid++ {
		assignment, ok := assignments[pid]
		if !ok || !allReplicasActive(assignment.Replicas, activeSet) {
			continue
		}

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

func rotateLeaderFirst(replicas []string, leader string) []string {
	for i, replica := range replicas {
		if replica != leader {
			continue
		}
		rotated := make([]string, 0, len(replicas))
		rotated = append(rotated, replicas[i:]...)
		rotated = append(rotated, replicas[:i]...)
		return rotated
	}
	return replicas
}

func firstActiveReplica(replicas []string, active []string) (string, bool) {
	for _, replica := range replicas {
		if containsReplica(active, replica) {
			return replica, true
		}
	}
	return "", false
}
