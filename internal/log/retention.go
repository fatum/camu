package log

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// PartitionInfo identifies a partition owned by this broker.
type PartitionInfo struct {
	Topic       string
	PartitionID int
}

// RetentionCleaner runs a background loop that deletes expired segments from
// owned partitions. It lists objects in S3 and checks per-segment metadata
// to determine which segments have expired.
type RetentionCleaner struct {
	s3Client  *storage.S3Client
	interval  time.Duration
	retention time.Duration
	stopCh    chan struct{}
}

// NewRetentionCleaner creates a RetentionCleaner that checks every interval
// and removes segments older than retention.
func NewRetentionCleaner(s3Client *storage.S3Client, interval, retention time.Duration) *RetentionCleaner {
	return &RetentionCleaner{
		s3Client:  s3Client,
		interval:  interval,
		retention: retention,
		stopCh:    make(chan struct{}),
	}
}

// Start launches the background cleanup goroutine. getOwnedPartitions is called
// on each tick to obtain the current set of partitions owned by this broker.
func (rc *RetentionCleaner) Start(ctx context.Context, getOwnedPartitions func() []PartitionInfo) {
	go func() {
		ticker := time.NewTicker(rc.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rc.runOnce(ctx, getOwnedPartitions())
			case <-rc.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop signals the background goroutine to exit.
func (rc *RetentionCleaner) Stop() {
	close(rc.stopCh)
}

func (rc *RetentionCleaner) runOnce(ctx context.Context, partitions []PartitionInfo) {
	for _, p := range partitions {
		if err := rc.cleanPartition(ctx, p); err != nil {
			slog.Error("retention cleanup failed",
				"topic", p.Topic,
				"partition", p.PartitionID,
				"err", err,
			)
		}
	}
}

func (rc *RetentionCleaner) cleanPartition(ctx context.Context, p PartitionInfo) error {
	prefix := ListSegmentPrefix(p.Topic, p.PartitionID)

	keys, err := rc.s3Client.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", prefix, err)
	}

	cutoff := time.Now().Add(-rc.retention)
	var deleted int

	for _, key := range keys {
		if !strings.HasSuffix(key, ".meta.json") {
			continue
		}

		data, err := rc.s3Client.Get(ctx, key)
		if err != nil {
			slog.Error("failed to get segment metadata",
				"key", key,
				"err", err,
			)
			continue
		}

		var meta SegmentMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			slog.Error("failed to unmarshal segment metadata",
				"key", key,
				"err", err,
			)
			continue
		}

		if meta.CreatedAt.After(cutoff) {
			continue
		}

		// Delete the segment file, offset index, and metadata sidecar.
		segKey := meta.SegmentKey
		offsetIdxKey := meta.OffsetIndexKey
		if offsetIdxKey == "" {
			offsetIdxKey = SegmentOffsetIndexKey(segKey)
		}

		if err := rc.s3Client.Delete(ctx, segKey); err != nil {
			slog.Error("failed to delete expired segment",
				"key", segKey,
				"err", err,
			)
		}
		if err := rc.s3Client.Delete(ctx, offsetIdxKey); err != nil {
			slog.Error("failed to delete expired segment offset index",
				"key", offsetIdxKey,
				"err", err,
			)
		}
		if err := rc.s3Client.Delete(ctx, key); err != nil {
			slog.Error("failed to delete expired segment metadata",
				"key", key,
				"err", err,
			)
		}
		deleted++
	}

	if deleted > 0 {
		slog.Info("retention cleanup removed segments",
			"topic", p.Topic,
			"partition", p.PartitionID,
			"count", deleted,
		)
	}
	return nil
}
