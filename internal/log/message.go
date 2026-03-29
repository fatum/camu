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

// BatchMeta summarizes a WAL batch without materializing its messages.
type BatchMeta struct {
	ProducerID   uint64
	Sequence     uint64
	MessageCount int
	FirstOffset  uint64
	LastOffset   uint64
}

// BatchFrame is a raw batch envelope plus its parsed metadata.
// Data is encoded in the WAL batch wire format.
type BatchFrame struct {
	Data []byte
	Meta BatchMeta
}
