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
// Reads across multiple segments until limit is reached or no more data.
// Returns the messages and the next offset to fetch from.
func (f *Fetcher) Fetch(ctx context.Context, index *log.Index, topic string, partitionID int, startOffset uint64, limit int) ([]log.Message, uint64, error) {
	if index == nil {
		return nil, startOffset, nil
	}

	var allMsgs []log.Message
	currentOffset := startOffset
	remaining := limit

	for remaining > 0 {
		segRef, found := index.Lookup(currentOffset)
		if !found {
			break
		}

		data, err := f.fetchSegmentData(ctx, segRef.Key)
		if err != nil {
			if len(allMsgs) > 0 {
				break // return what we have
			}
			return nil, 0, err
		}

		msgs, err := log.ReadSegmentFromOffset(bytes.NewReader(data), int64(len(data)), currentOffset, remaining)
		if err != nil {
			if len(allMsgs) > 0 {
				break
			}
			return nil, 0, fmt.Errorf("read segment: %w", err)
		}

		if len(msgs) == 0 {
			break
		}

		allMsgs = append(allMsgs, msgs...)
		remaining -= len(msgs)
		currentOffset = msgs[len(msgs)-1].Offset + 1
	}

	nextOffset := startOffset
	if len(allMsgs) > 0 {
		nextOffset = allMsgs[len(allMsgs)-1].Offset + 1
	}

	return allMsgs, nextOffset, nil
}

// fetchSegmentData reads a segment from disk cache, falling back to S3.
func (f *Fetcher) fetchSegmentData(ctx context.Context, key string) ([]byte, error) {
	data, err := f.diskCache.Get(key)
	if err != nil {
		if !errors.Is(err, log.ErrCacheMiss) {
			return nil, fmt.Errorf("disk cache get: %w", err)
		}
		// Cache miss — fetch from S3.
		data, err = f.s3Client.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			return nil, fmt.Errorf("s3 get: %w", err)
		}
		// Cache the fetched segment.
		if putErr := f.diskCache.Put(key, data); putErr != nil {
			_ = putErr
		}
	}
	return data, nil
}
