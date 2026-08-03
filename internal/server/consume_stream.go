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
	// Decode the bounded raw page before applying the message limit. A later
	// active batch can replace offsets from an earlier sealed batch, so stopping
	// after the first N batches could return stale values.
	msgs, err := decodeCommittedPage(raw, startOffset, limit)
	if err != nil {
		return nil, startOffset, fmt.Errorf("decode committed batches: %w", err)
	}
	result := make([]consumedMessage, 0, len(msgs))
	for _, msg := range msgs {
		result = append(result, consumedMessage{
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
			Key:       string(msg.Key),
			Value:     tryString(msg.Value),
			Headers:   msg.Headers,
		})
	}
	nextOffset := startOffset
	if len(msgs) > 0 {
		nextOffset = msgs[len(msgs)-1].Offset + 1
	}
	return result, nextOffset, nil
}

// decodeCommittedPage decodes one RecordBatch at a time and retains only the
// lowest requested offsets. Later active batches may overwrite values from a
// sealed predecessor without requiring all decoded messages to remain live.
func decodeCommittedPage(raw []byte, startOffset uint64, limit int) ([]logstore.Message, error) {
	if limit <= 0 {
		return nil, nil
	}
	byOffset := make(map[uint64]logstore.Message, limit)
	var largestOffset uint64
	hasLargestOffset := false
	for position := 0; position < len(raw); {
		header, err := logstore.ReadRecordBatchHeader(raw[position:])
		if err != nil {
			return nil, fmt.Errorf("read record batch header: %w", err)
		}
		batchSize := int(header.RecordBatchSize())
		if batchSize <= 0 || batchSize > len(raw)-position {
			return nil, fmt.Errorf("invalid record batch size %d", batchSize)
		}
		batch, err := logstore.DecodeRecordBatch(raw[position : position+batchSize])
		if err != nil {
			return nil, fmt.Errorf("decode record batch: %w", err)
		}
		for _, msg := range batch {
			if msg.Offset < startOffset {
				continue
			}
			if _, exists := byOffset[msg.Offset]; exists {
				byOffset[msg.Offset] = msg
				continue
			}
			if len(byOffset) < limit {
				byOffset[msg.Offset] = msg
				if !hasLargestOffset || msg.Offset > largestOffset {
					largestOffset = msg.Offset
					hasLargestOffset = true
				}
				continue
			}
			if msg.Offset < largestOffset {
				delete(byOffset, largestOffset)
				byOffset[msg.Offset] = msg
				hasLargestOffset = false
				for offset := range byOffset {
					if !hasLargestOffset || offset > largestOffset {
						largestOffset = offset
						hasLargestOffset = true
					}
				}
			}
		}
		position += batchSize
	}

	offsets := make([]uint64, 0, len(byOffset))
	for offset := range byOffset {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	messages := make([]logstore.Message, 0, len(offsets))
	for _, offset := range offsets {
		messages = append(messages, byOffset[offset])
	}
	return messages, nil
}
