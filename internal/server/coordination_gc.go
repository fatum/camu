package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
)

// coordinationGC removes stale coordination files from S3.
// Only called by the leader on a slow cadence (every 10th renewal tick).
func (s *Server) coordinationGC(ctx context.Context, topics []meta.TopicConfig) {
	s.coordinationLeader().runGC(ctx, topics)
}

// gcStaleInstances deletes instance registration files whose heartbeat
// has expired far beyond the registry TTL. The threshold is a multiple of the
// instance TTL (which itself is 3x the lease TTL): a node is only garbage
// collected when its heartbeat is older than 2x the liveness window, so a
// healthy node with a slow-but-working heartbeat write is never deleted while
// still registered and active. Instances referenced by a partition assignment
// (leader or replica) are always kept: deleting them would orphan the
// assignment and force a full reassignment on the next publish cycle.
func (s *Server) gcStaleInstances(ctx context.Context) {
	keys, err := s.s3Client.List(ctx, "_coordination/instances/")
	if err != nil {
		slog.Warn("coordinationGC: list instances", "error", err)
		return
	}
	now := time.Now()
	gcThreshold := 2 * s.instanceTTL
	if gcThreshold <= 0 {
		gcThreshold = 2 * s.leaseTTL * 3
	}
	if gcThreshold <= 0 {
		gcThreshold = 5 * time.Minute // safe floor for bare test servers
	}

	// Build the set of instance IDs still referenced by any assignment so a
	// referenced (but currently stale-heartbeating) instance is never deleted.
	referenced := make(map[string]struct{})
	if referencedKeys, err := s.s3Client.List(ctx, "_coordination/assignments/"); err == nil {
		for _, ak := range referencedKeys {
			data, err := s.s3Client.Get(ctx, ak)
			if err != nil {
				continue
			}
			var ta coordination.TopicAssignments
			if err := json.Unmarshal(data, &ta); err != nil {
				continue
			}
			for _, pa := range ta.Partitions {
				referenced[pa.Leader] = struct{}{}
				for _, r := range pa.Replicas {
					referenced[r] = struct{}{}
				}
			}
		}
	} else {
		slog.Warn("coordinationGC: list assignments for instance GC", "error", err)
	}

	for _, key := range keys {
		data, err := s.s3Client.Get(ctx, key)
		if err != nil {
			continue
		}
		var info coordination.InstanceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		if _, used := referenced[info.InstanceID]; used {
			continue
		}
		if now.Sub(info.HeartbeatAt) > gcThreshold {
			if err := s.s3Client.Delete(ctx, key); err != nil {
				slog.Warn("coordinationGC: delete stale instance", "key", key, "error", err)
			} else {
				slog.Info("coordinationGC: removed stale instance", "instance", info.InstanceID)
			}
		}
	}
}

// gcStaleISR deletes ISR state files for topics or partitions that no longer exist.
func (s *Server) gcStaleISR(ctx context.Context, topics []meta.TopicConfig) {
	topicSet := make(map[string]int)
	for _, t := range topics {
		topicSet[t.Name] = t.Partitions
	}

	keys, err := s.s3Client.List(ctx, "_coordination/isr/")
	if err != nil {
		slog.Warn("coordinationGC: list ISR", "error", err)
		return
	}
	for _, key := range keys {
		rest := key[len("_coordination/isr/"):]
		slashIdx := strings.Index(rest, "/")
		if slashIdx < 0 {
			continue
		}
		topic := rest[:slashIdx]
		var pid int
		if n, _ := fmt.Sscanf(rest[slashIdx+1:], "%d.json", &pid); n != 1 {
			continue
		}
		partCount, topicExists := topicSet[topic]
		if !topicExists || pid >= partCount {
			if err := s.s3Client.Delete(ctx, key); err != nil {
				slog.Warn("coordinationGC: delete stale ISR", "key", key, "error", err)
			} else {
				slog.Info("coordinationGC: removed stale ISR", "topic", topic, "partition", pid)
			}
		}
	}
}
