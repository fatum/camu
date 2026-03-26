package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

const fetchParallelism = 4

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

			msgs, err := log.ReadSegmentFromOffsetWithIndex(
				bytes.NewReader(fetched[i].data),
				int64(len(fetched[i].data)),
				fetched[i].offsetIdx,
				segRef.BaseOffset,
				currentOffset,
				remaining,
			)
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

type segmentFetchResult struct {
	data      []byte
	offsetIdx []byte
	err       error
}

func (f *Fetcher) fetchSegmentBatch(ctx context.Context, refs []log.SegmentRef) []segmentFetchResult {
	results := make([]segmentFetchResult, len(refs))
	var wg sync.WaitGroup
	wg.Add(len(refs))
	for i, ref := range refs {
		go func(i int, ref log.SegmentRef) {
			defer wg.Done()
			var (
				data      []byte
				offsetIdx []byte
				dataErr   error
				idxErr    error
			)

			var innerWG sync.WaitGroup
			innerWG.Add(2)
			go func() {
				defer innerWG.Done()
				data, dataErr = f.fetchSegmentData(ctx, ref.Key)
			}()
			go func() {
				defer innerWG.Done()
				offsetIdx, idxErr = f.fetchOptionalSegmentData(ctx, ref.OffsetIndexObjectKey())
			}()
			innerWG.Wait()

			if dataErr != nil {
				results[i].err = dataErr
				return
			}
			if idxErr != nil {
				results[i].err = idxErr
				return
			}
			results[i].data = data
			results[i].offsetIdx = offsetIdx
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

func (f *Fetcher) fetchOptionalSegmentData(ctx context.Context, key string) ([]byte, error) {
	data, err := f.fetchSegmentData(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
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
