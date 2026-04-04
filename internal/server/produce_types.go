package server

type produceMessageRequest struct {
	Key     string            `json:"key"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

// produceBatchRequest is the idempotent produce format:
// {"producer_id": N, "sequence": M, "messages": [...]}
type produceBatchRequest struct {
	ProducerID uint64                  `json:"producer_id"`
	Sequence   uint64                  `json:"sequence"`
	Messages   []produceMessageRequest `json:"messages"`
}

type produceResponse struct {
	Offsets []offsetInfo `json:"offsets"`
}

type offsetInfo struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}
