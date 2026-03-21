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

	// Sort instances for deterministic assignment.
	sorted := make([]string, len(instances))
	copy(sorted, instances)
	sort.Strings(sorted)

	// Initialize all instances in the map (even if numPartitions == 0).
	for _, inst := range sorted {
		result[inst] = nil
	}

	for p := 0; p < numPartitions; p++ {
		inst := sorted[p%len(sorted)]
		result[inst] = append(result[inst], p)
	}

	return result
}
