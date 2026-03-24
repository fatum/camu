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
		replicas := make([]string, rf)
		for r := range rf {
			replicas[r] = instances[(pid+r)%n]
		}
		leader := replicas[0]
		leaderEpoch := uint64(1)
		if current != nil {
			if cur, ok := current[pid]; ok {
				// Reassignment is leader-last when possible: if the current leader
				// still belongs to the new replica set, keep it as leader and rotate
				// replicas so it remains replicas[0].
				if containsReplica(replicas, cur.Leader) {
					replicas = rotateLeaderFirst(replicas, cur.Leader)
					leader = cur.Leader
					leaderEpoch = cur.LeaderEpoch
				} else {
					leaderEpoch = cur.LeaderEpoch + 1
				}
			}
		}
		result[pid] = PartitionAssignment{
			Replicas:    replicas,
			Leader:      leader,
			LeaderEpoch: leaderEpoch,
		}
	}
	return result
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
