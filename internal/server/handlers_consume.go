package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"unicode/utf8"

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

const maxConsumeLimit = 20000

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
		if limit > maxConsumeLimit {
			limit = maxConsumeLimit
		}
	}

	// Cap reads at the readable high watermark for replicated partitions.
	ps := s.partitionManager.GetPartitionState(topicName, partitionID)
	owned := s.isOwnedPartition(topicName, partitionID)
	var readableHW uint64
	var hasReadableHW bool
	if ps != nil {
		ps.mu.RLock()
		hw, ok := readableHighWatermark(ps)
		ps.mu.RUnlock()
		if ok {
			readableHW = hw
			hasReadableHW = true
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

	// The index HW (from S3) may be ahead of the in-memory follower HW
	// after a leader failover. Use the higher of the two so reads aren't
	// capped at a stale value.
	if hasReadableHW {
		if indexHW := index.HighWatermark(); indexHW > readableHW {
			readableHW = indexHW
		}
		w.Header().Set("X-High-Watermark", strconv.FormatUint(readableHW, 10))
		if startOffset >= readableHW {
			slog.Debug("consume_short_circuit_at_hw",
				"topic", topicName,
				"partition", partitionID,
				"offset", startOffset,
				"limit", limit,
				"owned", owned,
				"high_watermark", readableHW,
			)
			writeJSON(w, 200, consumeResponse{Messages: nil, NextOffset: startOffset})
			return
		}
		if maxReadable := readableHW - startOffset; uint64(limit) > maxReadable {
			limit = int(maxReadable)
		}
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

	streamCount, nextOffset, err := s.streamMessagesJSON(r.Context(), w, topicName, partitionID, startOffset, limit, index, ps)
	if err != nil {
		slog.Error("consume_failed", "topic", topicName, "partition", partitionID, "offset", startOffset, "error", err)
		return
	}

	slog.Debug("consume_complete",
		"topic", topicName,
		"partition", partitionID,
		"offset", startOffset,
		"limit", limit,
		"returned_messages", streamCount,
		"next_offset", nextOffset,
		"owned", owned,
		"high_watermark", readableHW,
	)
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
	if ps == nil {
		return msgs, nextOffset, nil
	}

	ps.mu.RLock()
	hw, ok := readableHighWatermark(ps)
	if !ok {
		ps.mu.RUnlock()
		return msgs, nextOffset, nil
	}
	walMsgs, err := ps.wal.ReadFromLocked(startOffset, limit)
	ps.mu.RUnlock()
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

type consumeIterItem struct {
	msg  logstore.Message
	err  error
	done bool
}

type consumeIterator struct {
	ch   <-chan consumeIterItem
	next *logstore.Message
	err  error
	done bool
}

func startConsumeIterator(ctx context.Context, walk func(func(logstore.Message) bool) error) *consumeIterator {
	ch := make(chan consumeIterItem, 1)
	go func() {
		defer close(ch)
		err := walk(func(msg logstore.Message) bool {
			select {
			case <-ctx.Done():
				return false
			case ch <- consumeIterItem{msg: msg}:
				return true
			}
		})
		if err != nil {
			select {
			case <-ctx.Done():
			case ch <- consumeIterItem{err: err, done: true}:
			}
			return
		}
		select {
		case <-ctx.Done():
		case ch <- consumeIterItem{done: true}:
		}
	}()
	return &consumeIterator{ch: ch}
}

func (it *consumeIterator) peek() (*logstore.Message, error) {
	if it.done {
		return nil, it.err
	}
	if it.next != nil {
		return it.next, nil
	}
	item, ok := <-it.ch
	if !ok {
		it.done = true
		return nil, it.err
	}
	if item.err != nil {
		it.err = item.err
		it.done = true
		return nil, item.err
	}
	if item.done {
		it.done = true
		return nil, nil
	}
	msg := item.msg
	it.next = &msg
	return it.next, nil
}

func (it *consumeIterator) pop() {
	it.next = nil
}

func (s *Server) streamMessagesJSON(ctx context.Context, w http.ResponseWriter, topicName string, partitionID int, startOffset uint64, limit int, index *logstore.Index, ps *partitionState) (int, uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var readableHW uint64
	if ps != nil {
		ps.mu.RLock()
		hw, ok := readableHighWatermark(ps)
		ps.mu.RUnlock()
		if ok {
			readableHW = hw
		}
	}

	segmentIter := startConsumeIterator(ctx, func(visit func(logstore.Message) bool) error {
		_, err := s.fetcher.Walk(ctx, index, topicName, partitionID, startOffset, limit, visit)
		return err
	})

	var walIter *consumeIterator
	if ps != nil {
		walIter = startConsumeIterator(ctx, func(visit func(logstore.Message) bool) error {
			return ps.wal.WalkFrom(startOffset, limit, func(msg logstore.Message) bool {
				if readableHW > 0 && msg.Offset >= readableHW {
					return true
				}
				return visit(msg)
			})
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"messages":[`))
	enc := json.NewEncoder(w)

	written := 0
	nextOffset := startOffset
	writeMsg := func(m logstore.Message) error {
		if written > 0 {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		if err := enc.Encode(consumedMessage{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       string(m.Key),
			Value:     tryString(m.Value),
			Headers:   m.Headers,
		}); err != nil {
			return err
		}
		written++
		nextOffset = m.Offset + 1
		return nil
	}

	for written < limit {
		segMsg, err := segmentIter.peek()
		if err != nil {
			return written, nextOffset, err
		}
		var walMsg *logstore.Message
		if walIter != nil {
			walMsg, err = walIter.peek()
			if err != nil {
				return written, nextOffset, err
			}
		}

		switch {
		case segMsg == nil && walMsg == nil:
			_, _ = w.Write([]byte(`],"next_offset":`))
			_, _ = w.Write([]byte(strconv.FormatUint(nextOffset, 10)))
			_, _ = w.Write([]byte("}"))
			return written, nextOffset, nil
		case segMsg == nil:
			if err := writeMsg(*walMsg); err != nil {
				return written, nextOffset, err
			}
			walIter.pop()
		case walMsg == nil:
			if err := writeMsg(*segMsg); err != nil {
				return written, nextOffset, err
			}
			segmentIter.pop()
		case walMsg.Offset == segMsg.Offset:
			if err := writeMsg(*walMsg); err != nil {
				return written, nextOffset, err
			}
			walIter.pop()
			segmentIter.pop()
		case walMsg.Offset < segMsg.Offset:
			if err := writeMsg(*walMsg); err != nil {
				return written, nextOffset, err
			}
			walIter.pop()
		default:
			if err := writeMsg(*segMsg); err != nil {
				return written, nextOffset, err
			}
			segmentIter.pop()
		}
	}

	_, _ = w.Write([]byte(`],"next_offset":`))
	_, _ = w.Write([]byte(strconv.FormatUint(nextOffset, 10)))
	_, _ = w.Write([]byte("}"))
	return written, nextOffset, nil
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
	if ps != nil {
		ps.mu.RLock()
		if ps.replicaState != nil {
			w.Header().Set("X-High-Watermark", strconv.FormatUint(ps.replicaState.HighWatermark(), 10))
		}
		ps.mu.RUnlock()
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
	if !utf8.Valid(b) {
		return base64.StdEncoding.EncodeToString(b)
	}
	return string(b)
}
