package log

// Message represents a single record stored in a segment.
type Message struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}
