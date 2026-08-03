package server

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

const maxReplicaFetchBytes = 1 << 20

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
	if ps == nil {
		slog.Debug("replica_fetch: partition not found or not replicated",
			"topic", topic, "pid", pid, "replica", replicaID)
		writeError(w, http.StatusNotFound, "partition not found or not replicated")
		return
	}

	// Check epoch divergence and implicit ack under ps.mu.Lock
	// because UpdateFollower mutates replica state.
	ps.mu.Lock()
	if !ps.isLeader || ps.replicaState == nil {
		ps.mu.Unlock()
		slog.Debug("replica_fetch: partition is no longer local leader",
			"topic", topic, "pid", pid, "replica", replicaID)
		writeError(w, http.StatusNotFound, "partition is no longer local leader")
		return
	}
	replicaState := ps.replicaState
	truncateTo, diverged := replicaState.CheckDivergence(replicaEpoch, replicaOffset)
	if diverged {
		epoch := ps.epoch
		// A follower which is asked to truncate must continue at the epoch
		// beginning at that offset. Returning the current partition epoch can
		// leave it reporting an older epoch after it fetches the new tail,
		// causing the same divergence check to repeat indefinitely.
		if ps.epochHistory != nil {
			if truncateEpoch, ok := ps.epochHistory.EpochAt(truncateTo); ok {
				epoch = truncateEpoch
			}
		}
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
	replicaState.UpdateFollower(replicaID, replicaOffset)

	activeBase := replicaActiveBase(ps)
	dataAvailable := fromOffset >= activeBase && fromOffset < ps.nextOffset
	behindSealedPrefix := fromOffset < activeBase
	ps.mu.Unlock()

	// The replica protocol transports concatenated self-framing RecordBatches.
	// Keep the bytes returned by the partition manager intact: reparsing them
	// into BatchFrames only to write the same bytes again doubled allocation
	// pressure for every follower fetch.
	readBatches := func() ([]byte, error) {
		bytes, _, err := s.partitionManager.ReadReplicaRawBatches(r.Context(), topic, pid, int64(fromOffset), maxReplicaFetchBytes)
		return bytes, err
	}
	var rawBytes []byte
	if dataAvailable {
		rawBytes, err = readBatches()
		if err != nil {
			slog.Error("replica_fetch: ReadRawBatches failed",
				"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
			writeError(w, 500, "fetch failed")
			return
		}
	}

	// Long-poll if still no data (waiting for new writes)
	// WaitForData uses its own internal signalling — don't hold ps.mu.
	if len(rawBytes) == 0 && !behindSealedPrefix {
		if replicaState.WaitForData(500 * time.Millisecond) {
			rawBytes, err = readBatches()
			if err != nil {
				slog.Error("replica_fetch: ReadRawBatches after wait failed",
					"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
			}
		}
	}

	// Snapshot state under lock for response headers. A concurrent reassignment
	// can demote this partition while the long-poll above is waiting. Do not
	// serve data from the former leader, and never dereference replicaState
	// after it has been cleared by that transition.
	ps.mu.RLock()
	if !ps.isLeader || ps.replicaState != replicaState {
		ps.mu.RUnlock()
		slog.Debug("replica_fetch: partition demoted during fetch",
			"topic", topic, "pid", pid, "replica", replicaID)
		writeError(w, http.StatusNotFound, "partition is no longer local leader")
		return
	}
	respHW := replicaState.HighWatermark()
	respEpoch := ps.epoch
	respFlushed := ps.flushedOffset
	respActiveBase := replicaActiveBase(ps)
	ps.mu.RUnlock()

	slog.Debug("replica_fetch: serving",
		"topic", topic, "pid", pid, "replica", replicaID,
		"from_offset", fromOffset, "bytes", len(rawBytes),
		"hw", respHW, "epoch", respEpoch)

	// Response headers
	w.Header().Set("X-High-Watermark", strconv.FormatUint(respHW, 10))
	w.Header().Set("X-Leader-Epoch", strconv.FormatUint(respEpoch, 10))
	w.Header().Set("X-Flushed-Offset", strconv.FormatUint(respFlushed, 10))
	w.Header().Set("X-Active-Base", strconv.FormatUint(respActiveBase, 10))

	if len(rawBytes) > 0 {
		if _, err := w.Write(rawBytes); err != nil {
			slog.Error("replica_fetch: write raw batches failed",
				"topic", topic, "pid", pid, "replica", replicaID, "error", err)
		}
	}
}

// replicaActiveBase is the offset where the leader's local tail begins. The
// preceding prefix is sealed and must be read by followers through S3.
func replicaActiveBase(ps *partitionState) uint64 {
	if ps.activeSegment != nil {
		return uint64(ps.activeSegment.BaseOffset())
	}
	return ps.nextOffset
}
