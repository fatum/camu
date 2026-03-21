package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

// Fetcher implements the tiered read path: in-memory buffer -> disk cache -> S3.
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
// It checks the in-memory buffer first, then the index (disk cache / S3).
// Returns the messages and the next offset to fetch from.
func (f *Fetcher) Fetch(ctx context.Context, index *log.Index, buffer []log.Message, topic string, partitionID int, startOffset uint64, limit int) ([]log.Message, uint64, error) {
	// 1. Check if any messages in the unflushed buffer satisfy the request.
	if len(buffer) > 0 {
		firstBufOffset := buffer[0].Offset
		if startOffset >= firstBufOffset {
			var msgs []log.Message
			for _, m := range buffer {
				if m.Offset >= startOffset {
					msgs = append(msgs, m)
					if limit > 0 && len(msgs) >= limit {
						break
					}
				}
			}
			if len(msgs) > 0 {
				nextOffset := msgs[len(msgs)-1].Offset + 1
				return msgs, nextOffset, nil
			}
			// Buffer exists but no matching messages — consumer is caught up.
			return nil, startOffset, nil
		}
	}

	// 2. Look up the segment in the index.
	if index == nil {
		return nil, startOffset, nil
	}

	segRef, found := index.Lookup(startOffset)
	if !found {
		// Offset is beyond all indexed segments.
		// Check if buffer has anything at all (even below startOffset means
		// there's unflushed data and the consumer is caught up).
		return nil, startOffset, nil
	}

	// 3. Try disk cache first.
	data, err := f.diskCache.Get(segRef.Key)
	if err != nil {
		if !errors.Is(err, log.ErrCacheMiss) {
			return nil, 0, fmt.Errorf("disk cache get: %w", err)
		}

		// 4. Cache miss — fetch from S3.
		data, err = f.s3Client.Get(ctx, segRef.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, startOffset, nil
			}
			return nil, 0, fmt.Errorf("s3 get: %w", err)
		}

		// 5. Cache the fetched segment.
		if putErr := f.diskCache.Put(segRef.Key, data); putErr != nil {
			// Non-fatal: log but continue.
			_ = putErr
		}
	}

	// 6. Parse segment.
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
