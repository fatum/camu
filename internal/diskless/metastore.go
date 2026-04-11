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
