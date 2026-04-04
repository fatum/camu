package server

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/log"
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

	ps.mu.Unlock()
	rawBytes, _, err := s.partitionManager.ReadReplicaRawBatches(r.Context(), topic, pid, int64(fromOffset), 1<<20)
	if err != nil {
		slog.Error("replica_fetch: ReadRawBatches failed",
			"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
		writeError(w, 500, "fetch failed")
		return
	}
	frames := make([]log.BatchFrame, 0)
	if len(rawBytes) > 0 {
		batches, bErr := log.ReadSegmentBatches(rawBytes, fromOffset, 0)
		if bErr != nil {
			slog.Error("replica_fetch: parse raw batches failed",
				"topic", topic, "pid", pid, "from_offset", fromOffset, "error", bErr)
			writeError(w, 500, "fetch failed")
			return
		}
		frames = make([]log.BatchFrame, 0, len(batches))
		for _, batch := range batches {
			hdr, hErr := log.ReadRecordBatchHeader(batch)
			if hErr != nil {
				continue
			}
			frames = append(frames, log.BatchFrame{
				Meta: log.BatchMeta{
					FirstOffset: uint64(hdr.FirstOffset),
					LastOffset:  uint64(hdr.LastOffset()),
				},
				Data: batch,
			})
		}
		slog.Info("replica_fetch: served raw batches from segments",
			"topic", topic, "pid", pid, "replica", replicaID,
			"from_offset", fromOffset, "batch_count", len(frames))
	}

	// Long-poll if still no data (waiting for new writes)
	// WaitForData uses its own internal signalling — don't hold ps.mu.
	if len(frames) == 0 {
		if ps.replicaState.WaitForData(500 * time.Millisecond) {
			rawBytes, _, err = s.partitionManager.ReadReplicaRawBatches(r.Context(), topic, pid, int64(fromOffset), 1<<20)
			if err != nil {
				slog.Error("replica_fetch: ReadRawBatches after wait failed",
					"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
			} else if len(rawBytes) > 0 {
				batches, bErr := log.ReadSegmentBatches(rawBytes, fromOffset, 0)
				if bErr == nil {
					frames = frames[:0]
					for _, batch := range batches {
						hdr, hErr := log.ReadRecordBatchHeader(batch)
						if hErr != nil {
							continue
						}
						frames = append(frames, log.BatchFrame{
							Meta: log.BatchMeta{
								FirstOffset: uint64(hdr.FirstOffset),
								LastOffset:  uint64(hdr.LastOffset()),
							},
							Data: batch,
						})
					}
				}
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
