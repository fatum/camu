package log

import (
	"context"
	"fmt"
	"strings"

	"github.com/maksim/camu/internal/storage"
)

// GarbageCollector removes orphaned segment objects from S3 that are no longer
// referenced by the partition index.
type GarbageCollector struct {
	s3Client *storage.S3Client
}

// NewGarbageCollector creates a GarbageCollector backed by the given S3 client.
func NewGarbageCollector(s3 *storage.S3Client) *GarbageCollector {
	return &GarbageCollector{s3Client: s3}
}

// FindOrphans lists all .segment objects under {topic}/{partitionID}/ in S3,
// loads the partition index, and returns the keys that are not referenced by
// any index entry.
func (gc *GarbageCollector) FindOrphans(ctx context.Context, topic string, partitionID int) ([]string, error) {
	prefix := fmt.Sprintf("%s/%d/", topic, partitionID)
	indexKey := fmt.Sprintf("%s%s", prefix, "index.json")

	// Load index
	data, err := gc.s3Client.Get(ctx, indexKey)
	if err != nil {
		return nil, fmt.Errorf("gc FindOrphans: load index %q: %w", indexKey, err)
	}
	idx := NewIndex()
	if err := idx.UnmarshalJSON(data); err != nil {
		return nil, fmt.Errorf("gc FindOrphans: unmarshal index: %w", err)
	}

	// Build set of indexed keys
	indexed := make(map[string]struct{}, len(idx.segments)*3)
	for _, r := range idx.segments {
		indexed[r.Key] = struct{}{}
		indexed[r.OffsetIndexObjectKey()] = struct{}{}
		indexed[r.MetaObjectKey()] = struct{}{}
	}

	// List all objects under the prefix
	allKeys, err := gc.s3Client.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("gc FindOrphans: list %q: %w", prefix, err)
	}

	var orphans []string
	for _, key := range allKeys {
		if !strings.HasSuffix(key, ".segment") &&
			!strings.HasSuffix(key, ".offset.idx") &&
			!strings.HasSuffix(key, ".meta.json") {
			continue
		}
		if _, ok := indexed[key]; !ok {
			orphans = append(orphans, key)
		}
	}
	return orphans, nil
}

// CleanOrphans finds and deletes all orphaned segment objects for the partition.
func (gc *GarbageCollector) CleanOrphans(ctx context.Context, topic string, partitionID int) error {
	orphans, err := gc.FindOrphans(ctx, topic, partitionID)
	if err != nil {
		return fmt.Errorf("gc CleanOrphans: %w", err)
	}
	for _, key := range orphans {
		if err := gc.s3Client.Delete(ctx, key); err != nil {
			return fmt.Errorf("gc CleanOrphans: delete %q: %w", key, err)
		}
	}
	return nil
}
