package log

// Message represents a single record stored in a segment.
type Message struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// Batch groups messages from a single produce call with optional
// idempotency metadata. ProducerID=0 means no idempotency tracking.
type Batch struct {
	ProducerID uint64
	Sequence   uint64
	Messages   []Message
}

// BatchMeta summarizes a stored batch without materializing its messages.
type BatchMeta struct {
	ProducerID   uint64
	Sequence     uint64
	MessageCount int
	FirstOffset  uint64
	LastOffset   uint64
}

// BatchFrame is a raw batch envelope plus its parsed metadata.
// Data stores the raw batch bytes.
type BatchFrame struct {
	Data []byte
	Meta BatchMeta
}

// IndexEntry is one entry in the per-batch offset index.
// Each entry describes a single batch in the sealed segment and is used for
// binary-search lookups by offset.
type IndexEntry struct {
	BaseOffset     int64
	LastOffset     int64
	Position       int64
	BatchSize      int32
	FirstTimestamp int64
	MaxTimestamp   int64
}

// TimestampIndexEntry is one entry in the monotonic timestamp index.
// It maps a batch's earliest timestamp to the batch's base offset and is used
// for binary-search lookups by timestamp.
type TimestampIndexEntry struct {
	Timestamp  int64
	BaseOffset int64
}
