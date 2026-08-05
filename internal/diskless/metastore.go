package diskless

import (
	"context"
	"time"
)

// MetaStore coordinates offset allocation and segment discovery for diskless topics.
type MetaStore interface {
	// AllocateOffsets atomically assigns offset ranges for one or more partition batches.
	AllocateOffsets(ctx context.Context, allocs []OffsetAllocation) ([]OffsetResult, error)

	// RegisterSegment records a flushed data file in the segment catalog.
	RegisterSegment(ctx context.Context, seg SegmentRecord) error

	// QuerySegments returns segment references covering [fromOffset, ...) for a
	// given topic-partition, up to maxBytes of data.
	QuerySegments(ctx context.Context, topic string, partition int,
		fromOffset int64, maxBytes int) ([]SegmentRef, error)

	// GetPartitionHead returns the next offset that will be allocated for a partition.
	GetPartitionHead(ctx context.Context, topic string, partition int) (int64, error)

	// GetCommittedHead returns the highest offset durably materialized for a
	// partition — the readable high watermark. It advances only through the
	// longest run of contiguous registered segments, so it never includes
	// offsets that were allocated but not yet persisted (in-flight flushes),
	// abandoned (gap after a hard failure), or registered out of order (a later
	// range materialized before an earlier one).
	GetCommittedHead(ctx context.Context, topic string, partition int) (int64, error)

	// GetPartitionStart returns the earliest readable offset for a partition
	// after retention cleanup has removed old segment references.
	GetPartitionStart(ctx context.Context, topic string, partition int) (int64, error)

	// PlanExpiredFileDeletes returns diskless backing file keys whose references
	// for the given topic-partition are expired and whose remaining references, if
	// any, are also expired. This allows callers to delete S3 data first and only
	// then delete metastore refs.
	PlanExpiredFileDeletes(ctx context.Context, topic string, partition int, cutoff time.Time) ([]string, error)

	// DeleteFileRefs removes all segment references pointing at fileKey.
	DeleteFileRefs(ctx context.Context, fileKey string) error

	// DeleteTopic removes all MetaStore state for a topic.
	DeleteTopic(ctx context.Context, topic string) error

	// Close releases any resources held by the MetaStore.
	Close() error
}

// contiguousCommittedEnd walks refs (sorted ascending by BaseOffset and
// non-overlapping within a partition) starting at the current committed head,
// returning the end of the longest run of refs that are contiguous with it. A
// ref advances the watermark only when its base equals the current position, so
// a registration that arrives before an earlier range (an in-flight or
// abandoned prefix) never exposes a gap to readers.
func contiguousCommittedEnd(committed int64, refs []SegmentRef) int64 {
	for _, r := range refs {
		if r.BaseOffset > committed {
			break
		}
		if r.BaseOffset == committed {
			committed = r.EndOffset
		}
	}
	return committed
}
