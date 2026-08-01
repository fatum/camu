package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

const (
	fetchParallelism   = 4
	maxRangeReadBytes  = 4 << 20
	rangeReadAttempts  = 3
	rangeRetryInterval = 50 * time.Millisecond
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
	if index == nil || limit <= 0 {
		return nil, startOffset, nil
	}

	var allMsgs []log.Message
	currentOffset := startOffset
	remaining := limit

	for remaining > 0 {
		segmentPlan := index.SegmentsFrom(currentOffset, fetchParallelism)
		if len(segmentPlan) == 0 {
			slog.Debug("consume_fetch_plan_empty",
				"topic", topic,
				"partition", partitionID,
				"offset", currentOffset,
				"remaining", remaining,
			)
			break
		}
		slog.Debug("consume_fetch_plan",
			"topic", topic,
			"partition", partitionID,
			"offset", currentOffset,
			"remaining", remaining,
			"planned_segments", len(segmentPlan),
			"first_segment_base", segmentPlan[0].BaseOffset,
			"first_segment_end", segmentPlan[0].EndOffset,
			"last_segment_base", segmentPlan[len(segmentPlan)-1].BaseOffset,
			"last_segment_end", segmentPlan[len(segmentPlan)-1].EndOffset,
		)

		fetched := f.fetchSegmentBatch(ctx, segmentPlan)
		progressed := false
		for i, segRef := range segmentPlan {
			if fetched[i].err != nil {
				slog.Debug("consume_segment_fetch_failed",
					"topic", topic,
					"partition", partitionID,
					"offset", currentOffset,
					"segment_key", segRef.Key,
					"segment_base", segRef.BaseOffset,
					"segment_end", segRef.EndOffset,
					"error", fetched[i].err,
					"returned_messages", len(allMsgs),
				)
				if len(allMsgs) > 0 {
					remaining = 0
					break
				}
				return nil, 0, fetched[i].err
			}

			msgs, err := decodeSegmentData(fetched[i].data, currentOffset, remaining)
			if err != nil {
				slog.Debug("consume_segment_decode_failed",
					"topic", topic,
					"partition", partitionID,
					"offset", currentOffset,
					"segment_key", segRef.Key,
					"segment_base", segRef.BaseOffset,
					"segment_end", segRef.EndOffset,
					"error", err,
					"returned_messages", len(allMsgs),
				)
				if len(allMsgs) > 0 {
					remaining = 0
					break
				}
				return nil, 0, fmt.Errorf("read segment: %w", err)
			}

			if len(msgs) == 0 {
				slog.Debug("consume_segment_decode_empty",
					"topic", topic,
					"partition", partitionID,
					"offset", currentOffset,
					"remaining", remaining,
					"segment_key", segRef.Key,
					"segment_base", segRef.BaseOffset,
					"segment_end", segRef.EndOffset,
				)
				break
			}

			prevOffset := currentOffset
			progressed = true
			allMsgs = append(allMsgs, msgs...)
			remaining -= len(msgs)
			currentOffset = msgs[len(msgs)-1].Offset + 1
			slog.Debug("consume_segment_decoded",
				"topic", topic,
				"partition", partitionID,
				"requested_offset", prevOffset,
				"remaining_after_decode", remaining,
				"segment_key", segRef.Key,
				"segment_base", segRef.BaseOffset,
				"segment_end", segRef.EndOffset,
				"segment_bytes", len(fetched[i].data),
				"decoded_messages", len(msgs),
				"decoded_first_offset", firstDecodedOffset(msgs),
				"decoded_last_offset", lastDecodedOffset(msgs),
				"next_offset", currentOffset,
			)
			if remaining == 0 || currentOffset > segRef.EndOffset {
				continue
			}
			break
		}
		if !progressed {
			break
		}
	}

	nextOffset := startOffset
	if len(allMsgs) > 0 {
		nextOffset = allMsgs[len(allMsgs)-1].Offset + 1
	}

	return allMsgs, nextOffset, nil
}

// Walk retrieves messages starting at startOffset, up to limit, and calls
// visit for each decoded message in offset order. Returning false from visit
// stops the scan early.
//
// Sealed segments are read in bounded, contiguous RecordBatch ranges using
// their offset sidecar. In particular, do not use fetchSegmentData for the
// segment itself: it materializes an entire segment in memory and defeats HTTP
// response back-pressure.
func (f *Fetcher) Walk(ctx context.Context, index *log.Index, topic string, partitionID int, startOffset uint64, limit int, visit func(log.Message) bool) (uint64, error) {
	if index == nil || limit <= 0 {
		return startOffset, nil
	}

	currentOffset := startOffset
	remaining := limit

	for remaining > 0 {
		segmentPlan := index.SegmentsFrom(currentOffset, 1)
		if len(segmentPlan) == 0 {
			break
		}
		segRef := segmentPlan[0]
		sidecar, err := f.fetchSegmentData(ctx, segRef.OffsetIndexObjectKey())
		if err != nil {
			return currentOffset, fmt.Errorf("read segment sidecar: %w", err)
		}
		entries, _, err := log.ReadSidecar(sidecar)
		if err != nil {
			return currentOffset, fmt.Errorf("read segment sidecar: %w", err)
		}
		for entryIndex := 0; entryIndex < len(entries); {
			entry := entries[entryIndex]
			if entry.LastOffset < int64(currentOffset) {
				entryIndex++
				continue
			}
			if entry.BatchSize <= 0 || entry.Position < 0 {
				return currentOffset, fmt.Errorf("invalid sidecar entry for segment %s", segRef.Key)
			}

			rangeStart := entry.Position
			rangeEnd := entry.Position + int64(entry.BatchSize)
			rangeEndIndex := entryIndex + 1
			for rangeEndIndex < len(entries) {
				next := entries[rangeEndIndex]
				if next.BatchSize <= 0 || next.Position != rangeEnd {
					break
				}
				nextEnd := next.Position + int64(next.BatchSize)
				if nextEnd-rangeStart > maxRangeReadBytes {
					break
				}
				rangeEnd = nextEnd
				rangeEndIndex++
			}

			data, err := f.getRangeWithRetry(ctx, segRef.Key, rangeStart, rangeEnd-rangeStart)
			if err != nil {
				return currentOffset, fmt.Errorf("read segment range: %w", err)
			}
			for _, batch := range entries[entryIndex:rangeEndIndex] {
				start := batch.Position - rangeStart
				end := start + int64(batch.BatchSize)
				if start < 0 || end > int64(len(data)) {
					return currentOffset, fmt.Errorf("short segment range for %s", segRef.Key)
				}
				msgs, err := log.DecodeRecordBatch(data[start:end])
				if err != nil {
					return currentOffset, fmt.Errorf("read segment batch: %w", err)
				}
				for _, msg := range msgs {
					if msg.Offset < currentOffset {
						continue
					}
					currentOffset = msg.Offset + 1
					remaining--
					if visit != nil && !visit(msg) {
						return currentOffset, nil
					}
					if remaining == 0 {
						return currentOffset, nil
					}
				}
			}
			entryIndex = rangeEndIndex
		}
		if currentOffset <= segRef.EndOffset {
			break
		}
	}

	return currentOffset, nil
}

func (f *Fetcher) getRangeWithRetry(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	var err error
	for attempt := 0; attempt < rangeReadAttempts; attempt++ {
		data, getErr := f.s3Client.GetRange(ctx, key, offset, length)
		if getErr == nil {
			return data, nil
		}
		err = getErr
		if errors.Is(err, storage.ErrNotFound) || attempt == rangeReadAttempts-1 {
			break
		}
		timer := time.NewTimer(rangeRetryInterval << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, err
}

type segmentFetchResult struct {
	data []byte
	err  error
}

func (f *Fetcher) fetchSegmentBatch(ctx context.Context, refs []log.SegmentRef) []segmentFetchResult {
	results := make([]segmentFetchResult, len(refs))
	var wg sync.WaitGroup
	wg.Add(len(refs))
	for i, ref := range refs {
		go func(i int, ref log.SegmentRef) {
			defer wg.Done()
			data, err := f.fetchSegmentData(ctx, ref.Key)
			if err != nil {
				results[i].err = err
				return
			}
			results[i].data = data
		}(i, ref)
	}
	wg.Wait()
	return results
}

// fetchSegmentData reads a segment from disk cache, falling back to S3.
func (f *Fetcher) fetchSegmentData(ctx context.Context, key string) ([]byte, error) {
	data, err := f.diskCache.Get(key)
	if err != nil {
		if !errors.Is(err, log.ErrCacheMiss) {
			return nil, fmt.Errorf("disk cache get: %w", err)
		}
		slog.Debug("consume_segment_cache_miss", "segment_key", key)
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
		slog.Debug("consume_segment_fetched_from_s3", "segment_key", key, "bytes", len(data))
		return data, nil
	}
	slog.Debug("consume_segment_cache_hit", "segment_key", key, "bytes", len(data))
	return data, nil
}

func decodeSegmentData(data []byte, startOffset uint64, limit int) ([]log.Message, error) {
	return log.ReadSegmentBatchesAsMessages(data, startOffset, limit)
}

func firstDecodedOffset(msgs []log.Message) any {
	if len(msgs) == 0 {
		return nil
	}
	return msgs[0].Offset
}

func lastDecodedOffset(msgs []log.Message) any {
	if len(msgs) == 0 {
		return nil
	}
	return msgs[len(msgs)-1].Offset
}
