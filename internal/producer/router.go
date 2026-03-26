package producer

import (
	"hash/fnv"
	"sync/atomic"
)

// Router routes messages to partitions based on key hashing or round-robin.
type Router struct {
	numPartitions     int
	roundRobinCounter atomic.Uint64
}

// NewRouter creates a new Router for the given number of partitions.
func NewRouter(numPartitions int) *Router {
	return &Router{numPartitions: numPartitions}
}

// Route returns the partition index for the given key.
// If key is nil, it round-robins across partitions.
// If key is non-nil, it hashes the key using FNV-32a to pick a consistent partition.
func (r *Router) Route(key []byte) int {
	if key == nil {
		counter := r.roundRobinCounter.Add(1) - 1
		return int(counter % uint64(r.numPartitions))
	}

	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(r.numPartitions))
}
