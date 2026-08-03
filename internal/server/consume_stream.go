package server

import (
	"context"
	"fmt"
	"sort"

	logstore "github.com/maksim/camu/internal/log"
)

const httpConsumeFetchBytes = 4 << 20

// readMessagesPage shares the committed raw-batch reader used by Kafka. HTTP
// only decodes the bounded result into its JSON representation.
func (s *Server) readMessagesPage(ctx context.Context, topicName string, partitionID int, startOffset uint64, limit int) ([]consumedMessage, uint64, error) {
	raw, _, err := s.partitionManager.ReadRawBatches(ctx, topicName, partitionID, int64(startOffset), httpConsumeFetchBytes)
	if err != nil {
		return nil, startOffset, fmt.Errorf("read committed batches: %w", err)
	}
	msgs, err := logstore.ReadSegmentBatchesAsMessages(raw, startOffset, 0)
	if err != nil {
		return nil, startOffset, fmt.Errorf("decode committed batches: %w", err)
	}
	// A newly active segment can overlap its sealed predecessor after a leader
	// change. Raw batches preserve that overlap for Kafka; JSON presents one
	// message per offset, preferring the later (active) batch.
	byOffset := make(map[uint64]logstore.Message, len(msgs))
	for _, msg := range msgs {
		byOffset[msg.Offset] = msg
	}
	msgOffsets := make([]uint64, 0, len(byOffset))
	for offset := range byOffset {
		msgOffsets = append(msgOffsets, offset)
	}
	sort.Slice(msgOffsets, func(i, j int) bool { return msgOffsets[i] < msgOffsets[j] })
	if len(msgOffsets) > limit {
		msgOffsets = msgOffsets[:limit]
	}

	result := make([]consumedMessage, 0, len(msgOffsets))
	for _, offset := range msgOffsets {
		msg := byOffset[offset]
		result = append(result, consumedMessage{
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
			Key:       string(msg.Key),
			Value:     tryString(msg.Value),
			Headers:   msg.Headers,
		})
	}
	nextOffset := startOffset
	if len(msgOffsets) > 0 {
		nextOffset = msgOffsets[len(msgOffsets)-1] + 1
	}
	return result, nextOffset, nil
}
