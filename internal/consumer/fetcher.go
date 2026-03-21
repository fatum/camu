package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

// Fetcher implements the read path: disk cache -> S3.
// All instances use the same read path regardless of partition ownership.
type Fetcher struct {
	s3Client  *storage.S3Client
	diskCache *log.DiskCache
}

// NewFetcher creates a new Fetcher.
func NewFetcher(s3Client *storage.S3Client, diskCache *log.DiskCache) *Fetcher {
	return &Fetcher{
		s3Client:  s3Client,
		diskCache: diskCache,
	}
}

// Fetch retrieves messages starting at startOffset, up to limit.
// Read path: disk cache -> S3 fetch (cached on fetch).
// Returns the messages and the next offset to fetch from.
func (f *Fetcher) Fetch(ctx context.Context, index *log.Index, topic string, partitionID int, startOffset uint64, limit int) ([]log.Message, uint64, error) {
	if index == nil {
		return nil, startOffset, nil
	}

	segRef, found := index.Lookup(startOffset)
	if !found {
		return nil, startOffset, nil
	}

	// 1. Try disk cache.
	data, err := f.diskCache.Get(segRef.Key)
	if err != nil {
		if !errors.Is(err, log.ErrCacheMiss) {
			return nil, 0, fmt.Errorf("disk cache get: %w", err)
		}

		// 2. Cache miss — fetch from S3.
		data, err = f.s3Client.Get(ctx, segRef.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, startOffset, nil
			}
			return nil, 0, fmt.Errorf("s3 get: %w", err)
		}

		// 3. Cache the fetched segment.
		if putErr := f.diskCache.Put(segRef.Key, data); putErr != nil {
			_ = putErr
		}
	}

	// 4. Parse segment.
	msgs, err := log.ReadSegmentFromOffset(bytes.NewReader(data), int64(len(data)), startOffset, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("read segment: %w", err)
	}

	nextOffset := startOffset
	if len(msgs) > 0 {
		nextOffset = msgs[len(msgs)-1].Offset + 1
	}

	return msgs, nextOffset, nil
}
