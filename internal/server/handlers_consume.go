package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

func (s *Server) handleConsumeLowLevel(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")
	w.Header().Set("X-Camu-Instance-ID", s.instanceID)

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

	tc, err := s.topicStore.Get(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if partitionID < 0 || partitionID >= tc.Partitions {
		writeError(w, http.StatusBadRequest, "partition ID out of range")
		return
	}

	if s.isTopicDiskless(r.Context(), topicName) {
		out, nextOffset, highWatermark, err := s.consumeDisklessMessages(r.Context(), topicName, partitionID, startOffset, limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-High-Watermark", strconv.FormatUint(highWatermark, 10))
		if startOffset >= highWatermark {
			writeJSON(w, 200, consumeResponse{Messages: nil, NextOffset: startOffset})
			return
		}
		writeJSON(w, http.StatusOK, consumeResponse{Messages: out, NextOffset: nextOffset})
		return
	}

	// Cap reads at the readable high watermark for replicated partitions.
	ps := s.partitionManager.GetPartitionState(topicName, partitionID)
	owned := s.isOwnedPartition(topicName, partitionID)
	if r.URL.Query().Get("consistency") == "leader" && !owned {
		writeError(w, http.StatusMisdirectedRequest, "not partition leader")
		return
	}
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
	if limit > maxAtomicConsumeLimit {
		limit = maxAtomicConsumeLimit
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

	messages, nextOffset, err := s.readMessagesPage(r.Context(), topicName, partitionID, startOffset, limit)
	if err != nil {
		slog.Error("consume_failed", "topic", topicName, "partition", partitionID, "offset", startOffset, "error", err)
		writeError(w, http.StatusServiceUnavailable, "consume source unavailable")
		return
	}
	writeJSON(w, http.StatusOK, consumeResponse{Messages: messages, NextOffset: nextOffset})

	slog.Debug("consume_complete",
		"topic", topicName,
		"partition", partitionID,
		"offset", startOffset,
		"limit", limit,
		"returned_messages", len(messages),
		"next_offset", nextOffset,
		"owned", owned,
		"high_watermark", readableHW,
	)
}

func (s *Server) handleStreamLowLevel(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")
	w.Header().Set("X-Camu-Instance-ID", s.instanceID)

	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid partition ID")
		return
	}

	tc, err := s.topicStore.Get(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if partitionID < 0 || partitionID >= tc.Partitions {
		writeError(w, http.StatusBadRequest, "partition ID out of range")
		return
	}

	if s.isTopicDiskless(r.Context(), topicName) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming not supported")
			return
		}
		var startOffset uint64
		if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
			if parsed, err := strconv.ParseUint(lastID, 10, 64); err == nil {
				startOffset = parsed + 1
			}
		} else if v := r.URL.Query().Get("offset"); v != "" {
			if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
				startOffset = parsed
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher.Flush()

		currentOffset := int64(startOffset)
		for {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			data, _, err := s.disklessEngine.Fetch(r.Context(), topicName, partitionID, currentOffset, 100*1024)
			if err != nil {
				return
			}
			msgs, _ := log.ReadSegmentBatchesAsMessages(data, uint64(currentOffset), 100)
			if len(msgs) == 0 {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
			for _, msg := range msgs {
				if err := consumer.WriteSSEEvent(w, msg); err != nil {
					return
				}
			}
			flusher.Flush()
			currentOffset = int64(msgs[len(msgs)-1].Offset) + 1
		}
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
	w.(http.Flusher).Flush()

	consumer.StreamSSE(r.Context(), w, s.fetcher, index, topicName, partitionID, startOffset)
}

const disklessConsumeFetchBytes = 64 * 1024

func (s *Server) consumeDisklessMessages(ctx context.Context, topic string, partitionID int, startOffset uint64, limit int) ([]consumedMessage, uint64, uint64, error) {
	currentOffset := startOffset
	nextOffset := startOffset
	var highWatermark uint64
	var out []consumedMessage

	for len(out) < limit {
		data, hw, err := s.disklessEngine.Fetch(ctx, topic, partitionID, int64(currentOffset), disklessConsumeFetchBytes)
		if err != nil {
			return nil, startOffset, highWatermark, fmt.Errorf("diskless fetch: %w", err)
		}
		if hw > 0 {
			highWatermark = uint64(hw)
		}
		if len(data) == 0 {
			break
		}

		msgs, err := log.ReadSegmentBatchesAsMessages(data, currentOffset, limit-len(out))
		if err != nil {
			return nil, startOffset, highWatermark, fmt.Errorf("decode diskless batches: %w", err)
		}
		if len(msgs) == 0 {
			break
		}

		for _, msg := range msgs {
			out = append(out, consumedMessage{
				Offset:    msg.Offset,
				Timestamp: msg.Timestamp,
				Key:       string(msg.Key),
				Value:     string(msg.Value),
				Headers:   msg.Headers,
			})
		}

		nextOffset = msgs[len(msgs)-1].Offset + 1
		if nextOffset <= currentOffset {
			break
		}
		currentOffset = nextOffset
		if currentOffset >= highWatermark {
			break
		}
	}

	if len(out) == 0 {
		if highWatermark > startOffset {
			return nil, highWatermark, highWatermark, nil
		}
		return nil, startOffset, highWatermark, nil
	}

	return out, nextOffset, highWatermark, nil
}
