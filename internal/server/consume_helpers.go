package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"unicode/utf8"

	logstore "github.com/maksim/camu/internal/log"
)

func readableHighWatermark(ps *partitionState) (uint64, bool) {
	if ps == nil {
		return 0, false
	}
	if ps.replicaState != nil {
		return ps.replicaState.HighWatermark(), true
	}
	if ps.followerHW > 0 {
		return ps.followerHW, true
	}
	return 0, false
}

func (s *Server) readMessages(ctx context.Context, topicName string, partitionID int, startOffset uint64, limit int, index *logstore.Index, ps *partitionState) ([]logstore.Message, uint64, error) {
	msgs, nextOffset, err := s.fetcher.Fetch(ctx, index, topicName, partitionID, startOffset, limit)
	if err != nil {
		return nil, 0, err
	}
	slog.Debug("consume_segment_fetch_complete",
		"topic", topicName,
		"partition", partitionID,
		"offset", startOffset,
		"limit", limit,
		"segment_messages", len(msgs),
		"segment_first_offset", firstMessageOffset(msgs),
		"segment_last_offset", lastMessageOffset(msgs),
		"segment_next_offset", nextOffset,
	)

	return msgs, nextOffset, nil
}

func mergeMessagesByOffset(startOffset uint64, limit int, segmentMsgs, walMsgs []logstore.Message) ([]logstore.Message, uint64) {
	if limit <= 0 {
		return nil, startOffset
	}
	if len(segmentMsgs) == 0 {
		if len(walMsgs) > limit {
			walMsgs = walMsgs[:limit]
		}
		nextOffset := startOffset
		if len(walMsgs) > 0 {
			nextOffset = walMsgs[len(walMsgs)-1].Offset + 1
		}
		return walMsgs, nextOffset
	}
	if len(walMsgs) == 0 {
		if len(segmentMsgs) > limit {
			segmentMsgs = segmentMsgs[:limit]
		}
		nextOffset := startOffset
		if len(segmentMsgs) > 0 {
			nextOffset = segmentMsgs[len(segmentMsgs)-1].Offset + 1
		}
		return segmentMsgs, nextOffset
	}

	merged := make([]logstore.Message, 0, limit)
	i, j := 0, 0
	for len(merged) < limit && (i < len(segmentMsgs) || j < len(walMsgs)) {
		switch {
		case i >= len(segmentMsgs):
			merged = append(merged, walMsgs[j])
			j++
		case j >= len(walMsgs):
			merged = append(merged, segmentMsgs[i])
			i++
		case walMsgs[j].Offset == segmentMsgs[i].Offset:
			merged = append(merged, walMsgs[j])
			i++
			j++
		case walMsgs[j].Offset < segmentMsgs[i].Offset:
			merged = append(merged, walMsgs[j])
			j++
		default:
			merged = append(merged, segmentMsgs[i])
			i++
		}
	}

	nextOffset := startOffset
	if len(merged) > 0 {
		nextOffset = merged[len(merged)-1].Offset + 1
	}
	return merged, nextOffset
}

func firstMessageOffset(msgs []logstore.Message) any {
	if len(msgs) == 0 {
		return nil
	}
	return msgs[0].Offset
}

func lastMessageOffset(msgs []logstore.Message) any {
	if len(msgs) == 0 {
		return nil
	}
	return msgs[len(msgs)-1].Offset
}

func writeConsumeJSON(w http.ResponseWriter, status int, msgs []logstore.Message, nextOffset uint64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_, _ = w.Write([]byte(`{"messages":[`))
	enc := json.NewEncoder(w)
	for i, m := range msgs {
		if i > 0 {
			_, _ = w.Write([]byte(","))
		}
		if err := enc.Encode(consumedMessage{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       string(m.Key),
			Value:     tryString(m.Value),
			Headers:   m.Headers,
		}); err != nil {
			return
		}
	}
	_, _ = w.Write([]byte(`],"next_offset":`))
	_, _ = w.Write([]byte(strconv.FormatUint(nextOffset, 10)))
	_, _ = w.Write([]byte("}"))
}

// tryString returns the string representation of b if it is valid UTF-8,
// otherwise returns a base64-encoded version.
func tryString(b []byte) string {
	if !utf8.Valid(b) {
		return base64.StdEncoding.EncodeToString(b)
	}
	return string(b)
}

// decodeRawBatchMessages decodes a raw Kafka v2 RecordBatch into []logstore.Message.
// This is the consume-side counterpart to the produce path that writes via
// log.EncodeRecordBatch / PartitionManager.AppendRawBatch.
//
// Integration point: once the consumer fetcher returns raw RecordBatch bytes
// from the active segment (Tasks 11-13), callers should use this function to
// expand them into individual messages for JSON serialization.
func decodeRawBatchMessages(rawBatch []byte) ([]logstore.Message, error) {
	return logstore.DecodeRecordBatch(rawBatch)
}
