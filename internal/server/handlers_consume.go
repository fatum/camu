package server

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/maksim/camu/internal/consumer"
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

func (s *Server) handleStreamLowLevel(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")
	w.Header().Set("X-Camu-Instance-ID", s.instanceID)

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
