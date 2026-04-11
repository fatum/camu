package diskless

import "time"

// OffsetAllocation is a request to allocate offsets for a batch of records.
type OffsetAllocation struct {
	Topic      string
	Partition  int
	Count      int
	ProducerID int64 // 0 = non-idempotent
	Sequence   int64 // -1 = non-idempotent
}

// OffsetResult is the result of an offset allocation.
type OffsetResult struct {
	BaseOffset int64
	Duplicate  bool
}

// SegmentRecord describes a data file and its per-partition batch locations.
type SegmentRecord struct {
	FileKey   string
	Batches   []BatchRef
	CreatedAt time.Time
	SizeBytes int64
}

// BatchRef locates a single partition's RecordBatch within a data file.
type BatchRef struct {
	Topic      string
	Partition  int
	BaseOffset int64
	EndOffset  int64 // exclusive: last offset + 1
	ByteOffset int64 // position within the data file
	ByteLength int64
}

// SegmentRef is a pointer to a byte range in an S3 data file for a consumer read.
type SegmentRef struct {
	FileKey    string
	ByteOffset int64
	ByteLength int64
	BaseOffset int64
	EndOffset  int64
}
