package log

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/storage"
)

// PartitionInfo identifies a partition owned by this broker.
type PartitionInfo struct {
	Topic       string
	PartitionID int
}

// RetentionCleaner runs a background loop that deletes expired segments from
// owned partitions. It updates the index in S3 before deleting objects so that
// readers never see a reference to a missing segment.
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
	indexKey := fmt.Sprintf("%s/%d/index.json", p.Topic, p.PartitionID)

	// Load the current index and its ETag for conditional update.
	data, etag, err := rc.s3Client.GetWithETag(ctx, indexKey)
	if err != nil {
		return fmt.Errorf("get index %s: %w", indexKey, err)
	}

	idx := NewIndex()
	if err := json.Unmarshal(data, idx); err != nil {
		return fmt.Errorf("unmarshal index %s: %w", indexKey, err)
	}

	expired := idx.RemoveExpired(rc.retention)
	if len(expired) == 0 {
		return nil
	}

	// Write updated index back to S3 before deleting objects.
	updated, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index %s: %w", indexKey, err)
	}
	if _, err := rc.s3Client.ConditionalPut(ctx, indexKey, updated, etag); err != nil {
		return fmt.Errorf("conditional put index %s: %w", indexKey, err)
	}

	// Delete the segment objects now that the index no longer references them.
	for _, ref := range expired {
		if err := rc.s3Client.Delete(ctx, ref.Key); err != nil {
			slog.Error("failed to delete expired segment",
				"key", ref.Key,
				"err", err,
			)
		}
	}

	slog.Info("retention cleanup removed segments",
		"topic", p.Topic,
		"partition", p.PartitionID,
		"count", len(expired),
	)
	return nil
}
