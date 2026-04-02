package log

import (
	"context"
	"fmt"
	"strings"

	"github.com/maksim/camu/internal/storage"
)

// GarbageCollector removes orphaned sidecar objects from S3 whose matching
// segment file is missing (typically from a crash during flush).
type GarbageCollector struct {
	s3Client *storage.S3Client
}

// NewGarbageCollector creates a GarbageCollector backed by the given S3 client.
func NewGarbageCollector(s3 *storage.S3Client) *GarbageCollector {
	return &GarbageCollector{s3Client: s3}
}

// FindOrphans lists all objects under {topic}/{partitionID}/ in S3 and returns
// sidecar keys (.offset.idx, .meta.json) that have no matching .segment file.
func (gc *GarbageCollector) FindOrphans(ctx context.Context, topic string, partitionID int) ([]string, error) {
	prefix := fmt.Sprintf("%s/%d/", topic, partitionID)
	keys, err := gc.s3Client.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("gc list: %w", err)
	}

	// Build sets of segment and meta keys, plus a lookup set of all keys.
	segments := make(map[string]struct{})
	metas := make(map[string]struct{})
	all := make(map[string]struct{})
	for _, key := range keys {
		all[key] = struct{}{}
		if strings.HasSuffix(key, ".segment") {
			segments[key] = struct{}{}
		} else if strings.HasSuffix(key, ".meta.json") {
			metas[key] = struct{}{}
		}
	}

	// Orphan = sidecar without matching segment.
	var orphans []string
	for _, key := range keys {
		if strings.HasSuffix(key, ".offset.idx") {
			segKey := strings.TrimSuffix(key, ".offset.idx") + ".segment"
			if _, ok := segments[segKey]; !ok {
				orphans = append(orphans, key)
			}
		} else if strings.HasSuffix(key, ".meta.json") {
			segKey := strings.TrimSuffix(key, ".meta.json") + ".segment"
			if _, ok := segments[segKey]; !ok {
				orphans = append(orphans, key)
			}
		}
	}

	// Reverse orphan: .segment without .meta.json (incomplete upload).
	for segKey := range segments {
		metaKey := strings.TrimSuffix(segKey, ".segment") + ".meta.json"
		if _, ok := metas[metaKey]; !ok {
			orphans = append(orphans, segKey)
			// Also flag the .offset.idx if it exists.
			idxKey := strings.TrimSuffix(segKey, ".segment") + ".offset.idx"
			if _, ok := all[idxKey]; ok {
				orphans = append(orphans, idxKey)
			}
		}
	}

	// Legacy index.json files.
	for _, key := range keys {
		if strings.HasSuffix(key, "/index.json") {
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
