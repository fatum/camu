package diskless

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// MetaStore coordinates offset allocation and segment discovery for diskless topics.
type MetaStore interface {
	// CommitUploadedBatches atomically, per partition, validates producer
	// sequences, assigns offsets, publishes uploaded refs, and advances the
	// readable head. All batches in one invocation must belong to the same
	// topic-partition, and the commit is all-or-nothing for the invocation:
	// either every batch becomes visible or none does. Idempotency follows the
	// Kafka contract: an exact retry of an idempotent batch (same producer,
	// first sequence, count) is deduplicated against the producer's commit
	// history and returns its original offset without another ref. Dedup
	// matches any recorded sequence within the bounded history (the last
	// `uploadedProducerHistory` batches per producer), so a retry of a
	// non-latest but still-recorded batch is also deduplicated. An exact replay
	// that has rotated out of that window is rejected as out-of-order rather
	// than re-allocated, so a stale retry can never silently duplicate records
	// at a fresh offset. Non-idempotent batches are not deduplicated.
	CommitUploadedBatches(ctx context.Context, batches []UploadedBatch) ([]OffsetResult, error)
	// QuerySegments returns segment references covering [fromOffset, ...) for a
	// given topic-partition, up to maxBytes of data.
	QuerySegments(ctx context.Context, topic string, partition int,
		fromOffset int64, maxBytes int) ([]SegmentRef, error)

	// ReplaceSegmentRefs atomically removes the references identified by remove
	// and inserts add into the partition's segment catalog. Readers must never
	// observe a gap or a duplicate for the affected range: add must exactly
	// cover the union of the removed ranges (compaction of a contiguous run).
	// The committed watermark is never modified by this call.
	ReplaceSegmentRefs(ctx context.Context, topic string, partition int, remove []RefKey, add []SegmentRef) error

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

	// ListFileRefs returns every segment reference across all partitions that
	// points at fileKey. A flush can pack batches for multiple partitions into
	// one data file, so retention must account for every reference before
	// deleting the object.
	ListFileRefs(ctx context.Context, fileKey string) ([]FileRef, error)

	// PlanUnreferencedFileDeletes returns the subset of fileKeys that are no
	// longer referenced by any partition, so their data objects can be deleted.
	PlanUnreferencedFileDeletes(ctx context.Context, fileKeys []string) ([]string, error)

	// ArchiveCommitted rolls the oldest compaction-final refs of a partition out
	// of the backend's hot head window into immutable archived storage, bounding
	// the head object regardless of history. targetBytes is the size at which a
	// ref is considered compaction-final (<= 0 means archive anything);
	// retentionCutoff excludes refs older than it so retention, not archiving,
	// drops them. Returns the number of refs archived. Backends whose metadata
	// is not a single bounded object (memory, DynamoDB) no-op.
	ArchiveCommitted(ctx context.Context, topic string, partition int, targetBytes int64, retentionCutoff time.Time) (int, error)

	// DeleteTopic removes all MetaStore state for a topic.
	DeleteTopic(ctx context.Context, topic string) error

	// Close releases any resources held by the MetaStore.
	Close() error
}

// ReplaceItemLimited is implemented by metastores whose ReplaceSegmentRefs
// atomically rewrites refs inside a single bounded transaction. DynamoDB caps
// TransactWriteItems at 100 operations; the S3 head-CAS and in-memory
// metastores have no such bound, so a compaction run can replace a full
// target-sized batch in one call.
type ReplaceItemLimited interface {
	ReplaceItemLimit() int
}

// parsePartitionKey splits a partition key of the form "topic#partition". The
// partition is the segment after the last "#", so topic names containing "#"
// are preserved.
func parsePartitionKey(key string) (string, int, error) {
	idx := strings.LastIndex(key, "#")
	if idx < 0 {
		return "", 0, fmt.Errorf("malformed partition key %q", key)
	}
	partition, err := strconv.Atoi(key[idx+1:])
	if err != nil {
		return "", 0, fmt.Errorf("malformed partition key %q: %w", key, err)
	}
	return key[:idx], partition, nil
}

// samePartitionBatches enforces the atomicity boundary of a commit invocation:
// every batch must belong to the same topic-partition so the write is
// all-or-nothing per partition and never spans a cross-partition prefix.
func samePartitionBatches(batches []UploadedBatch) error {
	for i := 1; i < len(batches); i++ {
		if batches[i].Topic != batches[0].Topic || batches[i].Partition != batches[0].Partition {
			return fmt.Errorf("commit uploaded batches spans multiple partitions")
		}
	}
	return nil
}
