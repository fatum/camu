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
// has expired beyond the registry TTL.
func (s *Server) gcStaleInstances(ctx context.Context) {
	keys, err := s.s3Client.List(ctx, "_coordination/instances/")
	if err != nil {
		slog.Warn("coordinationGC: list instances", "error", err)
		return
	}
	now := time.Now()
	for _, key := range keys {
		data, err := s.s3Client.Get(ctx, key)
		if err != nil {
			continue
		}
		var info coordination.InstanceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		if now.Sub(info.HeartbeatAt) > s.leaseTTL*3 {
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
