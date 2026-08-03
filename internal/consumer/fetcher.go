package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/metrics"
	"github.com/maksim/camu/internal/storage"
)

const (
	fetchParallelism   = 4
	maxRangeReadBytes  = 4 << 20
	rangeReadAttempts  = 5
	rangeRetryInterval = 50 * time.Millisecond
)

// Fetcher implements the read path: disk cache -> S3.
// All instances use the same read path regardless of partition ownership.
type Fetcher struct {
	s3Client  *storage.S3Client
	diskCache *log.DiskCache
	metrics   *metrics.Registry
}

func (f *Fetcher) SetMetrics(registry *metrics.Registry) {
	f.metrics = registry
}

func (f *Fetcher) recordRetry(operation string) {
	if f.metrics != nil {
		f.metrics.Inc("camu_consume_s3_read_retries_total", "S3 read retries performed by the consume path", map[string]string{"operation": operation})
	}
}

func (f *Fetcher) recordExhaustedRead(operation string) {
	if f.metrics != nil {
		f.metrics.Inc("camu_consume_s3_read_failures_total", "S3 reads that exhausted consume-path retries", map[string]string{"operation": operation})
	}
}

// NewFetcher creates a new Fetcher.
func NewFetcher(s3Client *storage.S3Client, diskCache *log.DiskCache) *Fetcher {
	return &Fetcher{
		s3Client:  s3Client,
		diskCache: diskCache,
	}
}

// Fetch retrieves messages starting at startOffset, up to limit. It is a
// collecting wrapper around Walk, so it keeps the same bounded range-read
// behavior as streaming consumers.
func (f *Fetcher) Fetch(ctx context.Context, index *log.Index, topic string, partitionID int, startOffset uint64, limit int) ([]log.Message, uint64, error) {
	messages := make([]log.Message, 0, limit)
	nextOffset, err := f.Walk(ctx, index, topic, partitionID, startOffset, limit, func(msg log.Message) bool {
		messages = append(messages, msg)
		return true
	})
	if err != nil {
		if len(messages) > 0 {
			return messages, nextOffset, nil
		}
		return nil, startOffset, err
	}
	return messages, nextOffset, nil
}

// Walk retrieves messages starting at startOffset, up to limit, and calls
// visit for each decoded message in offset order. Returning false from visit
// stops the scan early.
//
// Sealed segments are read in bounded, contiguous RecordBatch ranges using
// their offset sidecar.
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
		sidecar, err := f.fetchSidecar(ctx, segRef.OffsetIndexObjectKey())
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
		f.recordRetry("get_range")
		timer := time.NewTimer(rangeRetryInterval << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	f.recordExhaustedRead("get_range")
	return nil, err
}

func (f *Fetcher) getWithRetry(ctx context.Context, key string) ([]byte, error) {
	var err error
	for attempt := 0; attempt < rangeReadAttempts; attempt++ {
		data, getErr := f.s3Client.Get(ctx, key)
		if getErr == nil {
			return data, nil
		}
		err = getErr
		if errors.Is(err, storage.ErrNotFound) || attempt == rangeReadAttempts-1 {
			break
		}
		f.recordRetry("get")
		timer := time.NewTimer(rangeRetryInterval << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	f.recordExhaustedRead("get")
	return nil, err
}

// fetchSidecar reads a small immutable sidecar from disk cache, falling back
// to S3 and caching it locally. Segment payloads are never cached here.
func (f *Fetcher) fetchSidecar(ctx context.Context, key string) ([]byte, error) {
	data, err := f.diskCache.Get(key)
	if err != nil {
		if !errors.Is(err, log.ErrCacheMiss) {
			return nil, fmt.Errorf("disk cache get: %w", err)
		}
		slog.Debug("consume_sidecar_cache_miss", "sidecar_key", key)
		data, err = f.getWithRetry(ctx, key)
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
		slog.Debug("consume_sidecar_fetched_from_s3", "sidecar_key", key, "bytes", len(data))
		return data, nil
	}
	slog.Debug("consume_sidecar_cache_hit", "sidecar_key", key, "bytes", len(data))
	return data, nil
}
