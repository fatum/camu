package server

type consumeResponse struct {
	Messages   []consumedMessage `json:"messages"`
	NextOffset uint64            `json:"next_offset"`
}

type consumedMessage struct {
	Offset    uint64            `json:"offset"`
	Timestamp int64             `json:"timestamp"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

const maxConsumeLimit = 20000
