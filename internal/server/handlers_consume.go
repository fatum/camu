package server

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/maksim/camu/internal/consumer"
	logstore "github.com/maksim/camu/internal/log"
)

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

func (s *Server) handleConsumeLowLevel(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")

	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid partition ID")
		return
	}

	// Parse query params.
	var startOffset uint64
	if v := r.URL.Query().Get("offset"); v != "" {
		startOffset, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid offset")
			return
		}
	}

	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 1 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if limit > 1000 {
			limit = 1000
		}
	}

	// Cap reads at the readable high watermark for replicated partitions.
	ps := s.partitionManager.GetPartitionState(topicName, partitionID)
	owned := s.isOwnedPartition(topicName, partitionID)
	var readableHW uint64
	var hasReadableHW bool
	if hw, ok := readableHighWatermark(ps); ok {
		readableHW = hw
		hasReadableHW = true
		w.Header().Set("X-High-Watermark", strconv.FormatUint(hw, 10))
		if startOffset >= hw {
			slog.Debug("consume_short_circuit_at_hw",
				"topic", topicName,
				"partition", partitionID,
				"offset", startOffset,
				"limit", limit,
				"owned", owned,
				"high_watermark", hw,
			)
			writeJSON(w, 200, consumeResponse{Messages: nil, NextOffset: startOffset})
			return
		}
		maxReadable := hw - startOffset
		if uint64(limit) > maxReadable {
			limit = int(maxReadable)
		}
	}

	// Refresh index from S3 for non-owned partitions so we see the latest
	// segments flushed by the current owner.
	if !owned {
		slog.Debug("consume_refresh_index",
			"topic", topicName,
			"partition", partitionID,
			"offset", startOffset,
			"limit", limit,
		)
		s.partitionManager.RefreshIndex(r.Context(), topicName, partitionID)
	}

	index := s.partitionManager.GetIndex(topicName, partitionID)
	if index == nil {
		writeError(w, http.StatusNotFound, "partition not found")
		return
	}

	slog.Debug("consume_begin",
		"topic", topicName,
		"partition", partitionID,
		"offset", startOffset,
		"limit", limit,
		"owned", owned,
		"has_readable_hw", hasReadableHW,
		"high_watermark", readableHW,
	)

	msgs, nextOffset, err := s.readMessages(r.Context(), topicName, partitionID, startOffset, limit, index, ps)
	if err != nil {
		slog.Error("consume_failed", "topic", topicName, "partition", partitionID, "offset", startOffset, "error", err)
		writeError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
		return
	}

	slog.Debug("consume_complete",
		"topic", topicName,
		"partition", partitionID,
		"offset", startOffset,
		"limit", limit,
		"returned_messages", len(msgs),
		"next_offset", nextOffset,
		"owned", owned,
		"high_watermark", readableHW,
	)

	// Build response.
	resp := consumeResponse{
		Messages:   make([]consumedMessage, 0, len(msgs)),
		NextOffset: nextOffset,
	}
	for _, m := range msgs {
		cm := consumedMessage{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       string(m.Key),
			Value:     tryString(m.Value),
			Headers:   m.Headers,
		}
		resp.Messages = append(resp.Messages, cm)
	}

	writeJSON(w, http.StatusOK, resp)
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

	// Partitions can have a readable suffix that is still only present in the
	// local WAL. Merge segment-backed data with local WAL by offset rather than
	// assuming they are disjoint: after failover, the refreshed index can
	// overlap a stale or partially flushed WAL window.
	hw, ok := readableHighWatermark(ps)
	if ps == nil || !ok {
		return msgs, nextOffset, nil
	}

	walMsgs, err := ps.wal.ReadFrom(startOffset, limit)
	if err != nil {
		return nil, 0, err
	}
	rawWALCount := len(walMsgs)

	filteredWAL := walMsgs[:0]
	for _, msg := range walMsgs {
		if msg.Offset >= startOffset && msg.Offset < hw {
			filteredWAL = append(filteredWAL, msg)
		}
	}
	walMsgs = filteredWAL
	slog.Debug("consume_wal_overlay",
		"topic", topicName,
		"partition", partitionID,
		"offset", startOffset,
		"limit", limit,
		"raw_wal_messages", rawWALCount,
		"filtered_wal_messages", len(walMsgs),
		"wal_first_offset", firstMessageOffset(walMsgs),
		"wal_last_offset", lastMessageOffset(walMsgs),
		"high_watermark", hw,
	)

	merged, nextOffset := mergeMessagesByOffset(startOffset, limit, msgs, walMsgs)
	slog.Debug("consume_merge_complete",
		"topic", topicName,
		"partition", partitionID,
		"offset", startOffset,
		"limit", limit,
		"segment_messages", len(msgs),
		"wal_messages", len(walMsgs),
		"merged_messages", len(merged),
		"merged_first_offset", firstMessageOffset(merged),
		"merged_last_offset", lastMessageOffset(merged),
		"next_offset", nextOffset,
	)
	return merged, nextOffset, nil
}

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

func mergeMessagesByOffset(startOffset uint64, limit int, segmentMsgs, walMsgs []logstore.Message) ([]logstore.Message, uint64) {
	if limit <= 0 {
		return nil, startOffset
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

func (s *Server) handleStreamLowLevel(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")

	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid partition ID")
		return
	}

	// Check for SSE flusher support.
	if _, ok := w.(http.Flusher); !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	// Determine start offset: Last-Event-ID header takes precedence for reconnection.
	var startOffset uint64
	if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
		parsed, err := strconv.ParseUint(lastID, 10, 64)
		if err == nil {
			startOffset = parsed + 1 // resume after last seen event
		}
	} else if v := r.URL.Query().Get("offset"); v != "" {
		startOffset, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid offset")
			return
		}
	}

	// Get the partition index.
	index := s.partitionManager.GetIndex(topicName, partitionID)
	if index == nil {
		writeError(w, http.StatusNotFound, "partition not found")
		return
	}

	// Set HW header before streaming starts (headers must be sent before body).
	ps := s.partitionManager.GetPartitionState(topicName, partitionID)
	if ps != nil && ps.replicaState != nil {
		w.Header().Set("X-High-Watermark", strconv.FormatUint(ps.replicaState.HighWatermark(), 10))
	}

	// Set SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	consumer.StreamSSE(r.Context(), w, s.fetcher, index, topicName, partitionID, startOffset)
}

// tryString returns the string representation of b if it is valid UTF-8,
// otherwise returns a base64-encoded version.
func tryString(b []byte) string {
	s := string(b)
	// Fast path: most values are valid UTF-8 strings.
	for _, r := range s {
		if r == '\uFFFD' {
			return base64.StdEncoding.EncodeToString(b)
		}
	}
	return s
}
