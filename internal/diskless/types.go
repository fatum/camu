package diskless

import "time"

// OffsetResult is the result of an offset allocation.
type OffsetResult struct {
	BaseOffset int64
	Duplicate  bool
}

// UploadedBatch describes a batch which has already been durably uploaded but
// has not yet been assigned a logical offset. CommitUploadedBatches is the
// visibility boundary: objects without a committed batch are orphaned data and
// must never be returned to readers.
type UploadedBatch struct {
	// BatchID is a durable identity for this physical batch.  It is required for
	// every producer, including non-idempotent ones: retries of an uncertain
	// metadata commit must never append the same uploaded bytes twice.
	BatchID    string
	FileKey    string
	Topic      string
	Partition  int
	Count      int
	ProducerID int64
	Sequence   int64
	ByteOffset int64
	ByteLength int64
	CreatedAt  time.Time
}

// SegmentRef is a pointer to a byte range in an S3 data file for a consumer read.
type SegmentRef struct {
	FileKey    string
	ByteOffset int64
	ByteLength int64
	BaseOffset int64
	EndOffset  int64
	// CreatedAt is when the ref was materialized; used by compaction and
	// retention to decide eligibility. Ignored by the read path.
	CreatedAt time.Time
}

// RefKey identifies a segment reference by its offset range within a partition.
type RefKey struct {
	BaseOffset int64
	EndOffset  int64
}

// FileRef identifies one segment reference pointing at a shared data file. A
// single flush can pack batches for several partitions into one file, so a file
// may be referenced by multiple (topic, partition) pairs.
type FileRef struct {
	Topic     string
	Partition int
	Ref       SegmentRef
}
