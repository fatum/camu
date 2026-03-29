package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/replication"
)

func (s *Server) handleReplicaFetch(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	pid, err := strconv.Atoi(r.PathValue("pid"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid partition id")
		return
	}
	fromOffset, err := strconv.ParseUint(r.URL.Query().Get("from_offset"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid from_offset")
		return
	}
	replicaID := r.Header.Get("X-Replica-ID")
	replicaOffset, err := strconv.ParseUint(r.Header.Get("X-Replica-Offset"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid X-Replica-Offset")
		return
	}
	replicaEpoch, err := strconv.ParseUint(r.Header.Get("X-Replica-Epoch"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid X-Replica-Epoch")
		return
	}

	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps == nil || ps.replicaState == nil {
		slog.Debug("replica_fetch: partition not found or not replicated",
			"topic", topic, "pid", pid, "replica", replicaID)
		writeError(w, http.StatusNotFound, "partition not found or not replicated")
		return
	}

	// Check epoch divergence and implicit ack under ps.mu.Lock
	// because UpdateFollower mutates replica state.
	ps.mu.Lock()
	truncateTo, diverged := ps.replicaState.CheckDivergence(replicaEpoch, replicaOffset)
	if diverged {
		epoch := ps.epoch
		ps.mu.Unlock()
		slog.Info("replica_fetch: epoch divergence, requesting truncation",
			"topic", topic, "pid", pid, "replica", replicaID,
			"replica_epoch", replicaEpoch, "replica_offset", replicaOffset,
			"truncate_to", truncateTo)
		w.Header().Set("X-Truncate-To", strconv.FormatUint(truncateTo, 10))
		w.Header().Set("X-Leader-Epoch", strconv.FormatUint(epoch, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	// Implicit ack
	ps.replicaState.UpdateFollower(replicaID, replicaOffset)

	// Try WAL first (hot path for real-time replication). Preserve raw WAL
	// batch envelopes so the follower can append them directly without
	// decoding and re-encoding.
	frames, err := ps.wal.ReadBatchFramesFromLocked(fromOffset)
	hw := ps.replicaState.HighWatermark()
	ps.mu.Unlock()
	if err != nil {
		slog.Error("replica_fetch: WAL read failed",
			"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
		writeError(w, 500, "WAL read failed")
		return
	}

	if len(frames) > 0 {
		first := frames[0].Meta.FirstOffset
		last := frames[len(frames)-1].Meta.LastOffset
		slog.Info("replica_fetch: served from WAL",
			"topic", topic, "pid", pid, "replica", replicaID,
			"from_offset", fromOffset,
			"batch_count", len(frames),
			"first", first, "last", last,
			"hw", hw)
	}

	// Fall back to flushed segments if WAL doesn't have the data
	if len(frames) == 0 {
		index := s.partitionManager.GetIndex(topic, pid)
		if index != nil {
			var msgs []log.Message
			var nextOff uint64
			msgs, nextOff, err = s.fetcher.Fetch(r.Context(), index, topic, pid, fromOffset, 1000)
			if err != nil {
				slog.Error("replica_fetch: segment fetch failed",
					"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
				writeError(w, 500, "fetch failed")
				return
			}
			if len(msgs) > 0 {
				slog.Info("replica_fetch: served from segments",
					"topic", topic, "pid", pid, "replica", replicaID,
					"from_offset", fromOffset, "msg_count", len(msgs),
					"next_offset", nextOff, "index_next", index.NextOffset())
				frames = make([]log.BatchFrame, 0, 1)
				var buf bytes.Buffer
				if err := replication.WriteMessageFrames(&buf, msgs); err != nil {
					slog.Error("replica_fetch: WriteMessageFrames failed",
						"topic", topic, "pid", pid, "replica", replicaID,
						"msg_count", len(msgs), "error", err)
					writeError(w, 500, "replication encode failed")
					return
				}
				segmentFrames, err := replication.ReadBatchFrames(bytes.NewReader(buf.Bytes()))
				if err != nil {
					slog.Error("replica_fetch: ReadBatchFrames failed",
						"topic", topic, "pid", pid, "replica", replicaID,
						"msg_count", len(msgs), "error", err)
					writeError(w, 500, "replication decode failed")
					return
				}
				frames = append(frames, segmentFrames...)
			}
		}
	}

	// Long-poll if still no data (waiting for new writes)
	// WaitForData uses its own internal signalling — don't hold ps.mu.
	if len(frames) == 0 {
		if ps.replicaState.WaitForData(500 * time.Millisecond) {
			ps.mu.RLock()
			frames, err = ps.wal.ReadBatchFramesFromLocked(fromOffset)
			ps.mu.RUnlock()
			if err != nil {
				slog.Error("replica_fetch: WAL read after wait failed",
					"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
			} else if len(frames) > 0 {
				slog.Info("replica_fetch: served from WAL after long-poll",
					"topic", topic, "pid", pid, "replica", replicaID,
					"batch_count", len(frames))
			}
		}
	}

	// Snapshot state under lock for response headers.
	ps.mu.RLock()
	respHW := ps.replicaState.HighWatermark()
	respEpoch := ps.epoch
	respFlushed := ps.flushedOffset
	ps.mu.RUnlock()

	slog.Debug("replica_fetch: serving",
		"topic", topic, "pid", pid, "replica", replicaID,
		"from_offset", fromOffset, "batch_count", len(frames),
		"hw", respHW, "epoch", respEpoch)

	// Response headers
	w.Header().Set("X-High-Watermark", strconv.FormatUint(respHW, 10))
	w.Header().Set("X-Leader-Epoch", strconv.FormatUint(respEpoch, 10))
	w.Header().Set("X-Flushed-Offset", strconv.FormatUint(respFlushed, 10))

	// Write binary frames
	for _, frame := range frames {
		if _, err := w.Write(frame.Data); err != nil {
			slog.Error("replica_fetch: write raw batch frame failed",
				"topic", topic, "pid", pid, "replica", replicaID,
				"error", err)
			return
		}
	}
}
