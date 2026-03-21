package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/maksim/camu/internal/log"
)

// sseMessage is the JSON payload for a single SSE data line.
type sseMessage struct {
	Offset    uint64            `json:"offset"`
	Timestamp int64             `json:"timestamp"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// WriteSSEEvent formats a message as an SSE event and writes it to w.
// Format: "id: {offset}\ndata: {json}\n\n"
func WriteSSEEvent(w io.Writer, msg log.Message) error {
	payload := sseMessage{
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
		Key:       string(msg.Key),
		Value:     string(msg.Value),
		Headers:   msg.Headers,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal SSE payload: %w", err)
	}
	_, err = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", msg.Offset, data)
	return err
}

// StreamSSE continuously fetches messages and writes them as SSE events.
// It loops until ctx is cancelled. If no new messages are available, it sleeps
// briefly (100ms) before retrying.
func StreamSSE(ctx context.Context, w http.ResponseWriter, fetcher *Fetcher,
	index *log.Index,
	topic string, partitionID int, startOffset uint64) error {

	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("response writer does not support flushing")
	}

	currentOffset := startOffset

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msgs, nextOffset, err := fetcher.Fetch(ctx, index, topic, partitionID, currentOffset, 100)
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}

		if len(msgs) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		for _, msg := range msgs {
			if err := WriteSSEEvent(w, msg); err != nil {
				return fmt.Errorf("write SSE event: %w", err)
			}
		}
		flusher.Flush()
		currentOffset = nextOffset
	}
}
