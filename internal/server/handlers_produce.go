package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/producer"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

type produceMessageRequest struct {
	Key     string            `json:"key"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

type produceResponse struct {
	Offsets []offsetInfo `json:"offsets"`
}

type offsetInfo struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

func (s *Server) handleProduceHighLevel(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown.Load() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "server is shutting down")
		return
	}

	topicName := r.PathValue("topic")

	// Validate topic exists.
	_, err := s.topicStore.Get(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Parse body: either a JSON array or single object.
	var msgs []produceMessageRequest
	decoder := json.NewDecoder(r.Body)
	// Try to detect if it's an array or object by peeking.
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Try array first, then single object.
	if err := json.Unmarshal(raw, &msgs); err != nil {
		var single produceMessageRequest
		if err2 := json.Unmarshal(raw, &single); err2 != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: expected array or object")
			return
		}
		msgs = []produceMessageRequest{single}
	}

	if len(msgs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one message is required")
		return
	}

	router := s.partitionManager.GetRouter(topicName)
	if router == nil {
		writeError(w, http.StatusInternalServerError, "topic not initialized")
		return
	}

	// Group messages by partition for batch WAL writes (single fsync per partition).
	type indexedMsg struct {
		idx       int // original position in the request
		partition int
		msg       log.Message
	}
	byPartition := make(map[int][]indexedMsg)
	for i, m := range msgs {
		var key []byte
		if m.Key != "" {
			key = []byte(m.Key)
		}
		partitionID := router.Route(key)
		byPartition[partitionID] = append(byPartition[partitionID], indexedMsg{
			idx:       i,
			partition: partitionID,
			msg: log.Message{
				Key:     key,
				Value:   []byte(m.Value),
				Headers: m.Headers,
			},
		})
	}

	// Check ownership of all target partitions before writing.
	for partitionID := range byPartition {
		if !s.isOwnedPartition(topicName, partitionID) {
			routing := s.getRoutingMap(topicName)
			writeJSON(w, 421, routing)
			return
		}
	}

	// Append each partition's batch with a single WAL fsync.
	offsets := make([]offsetInfo, len(msgs))
	for partitionID, group := range byPartition {
		batch := make([]log.Message, len(group))
		for i, im := range group {
			batch[i] = im.msg
		}

		ps := s.partitionManager.GetPartitionState(topicName, partitionID)
		topicCfg, _ := s.topicStore.Get(r.Context(), topicName)

		// For replicated topics, reject writes if replicaState not yet initialized.
		// Don't check min_insync_replicas here — the purgatory will wait until
		// enough ISR members ack. This avoids a chicken-and-egg problem where
		// followers can't catch up (join ISR) if no data flows.
		if topicCfg.ReplicationFactor > 1 {
			if ps == nil || ps.replicaState == nil {
				slog.Debug("produce_rejected: replicaState not ready",
					"topic", topicName, "partition", partitionID)
				w.Header().Set("Retry-After", "1")
				writeError(w, 503, "partition not ready for replicated writes")
				return
			}
			if !s.verifyProduceLeadership(topicName, partitionID, ps.epoch) {
				routing := s.getRoutingMap(topicName)
				writeJSON(w, 421, routing)
				return
			}
		}

		assignedOffsets, err := s.partitionManager.AppendBatch(r.Context(), topicName, partitionID, batch)
		if err != nil {
			if errors.Is(err, producer.ErrBackpressure) {
				w.Header().Set("Retry-After", "1")
				writeError(w, http.StatusServiceUnavailable, "backpressure: buffer full")
				return
			}
			slog.Error("produce_failed", "topic", topicName, "partition", partitionID, "error", err)
			writeError(w, http.StatusInternalServerError, "append failed: "+err.Error())
			return
		}

		if ps != nil && ps.replicaState != nil {
			lastOffset := assignedOffsets[len(assignedOffsets)-1]

			slog.Debug("produce_awaiting_replication",
				"topic", topicName, "partition", partitionID,
				"offset", lastOffset, "hw", ps.replicaState.HighWatermark(),
				"isr_size", ps.replicaState.ISRSize())

			if err := ps.replicaState.Purgatory().Wait(lastOffset, 30*time.Second); err != nil {
				if errors.Is(err, replication.ErrReplicationTimeout) {
					slog.Warn("produce_replication_timeout",
						"topic", topicName, "partition", partitionID,
						"offset", lastOffset, "hw", ps.replicaState.HighWatermark(),
						"isr_size", ps.replicaState.ISRSize())
					writeError(w, 408, "replication timeout")
					return
				}
				writeError(w, http.StatusInternalServerError, "replication error: "+err.Error())
				return
			}

			slog.Info("produce_replicated",
				"topic", topicName, "partition", partitionID,
				"offset", lastOffset, "hw", ps.replicaState.HighWatermark(),
				"isr_size", ps.replicaState.ISRSize(),
				"epoch", ps.epoch)
		}

		if ps != nil {
			w.Header().Set("X-Leader-Epoch", strconv.FormatUint(ps.epoch, 10))
		}

		for i, im := range group {
			offsets[im.idx] = offsetInfo{
				Partition: partitionID,
				Offset:    assignedOffsets[i],
			}
		}
	}

	writeJSON(w, http.StatusOK, produceResponse{Offsets: offsets})
}

func (s *Server) handleProduceLowLevel(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown.Load() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "server is shutting down")
		return
	}

	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")

	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid partition ID")
		return
	}

	// Validate topic exists.
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

	// Check partition ownership.
	if !s.isOwnedPartition(topicName, partitionID) {
		routing := s.getRoutingMap(topicName)
		writeJSON(w, 421, routing)
		return
	}

	// Parse body.
	var msgs []produceMessageRequest
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := json.Unmarshal(raw, &msgs); err != nil {
		var single produceMessageRequest
		if err2 := json.Unmarshal(raw, &single); err2 != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: expected array or object")
			return
		}
		msgs = []produceMessageRequest{single}
	}

	if len(msgs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one message is required")
		return
	}

	batch := make([]log.Message, len(msgs))
	for i, m := range msgs {
		var key []byte
		if m.Key != "" {
			key = []byte(m.Key)
		}
		batch[i] = log.Message{
			Key:     key,
			Value:   []byte(m.Value),
			Headers: m.Headers,
		}
	}

	ps := s.partitionManager.GetPartitionState(topicName, partitionID)

	// For replicated topics, reject writes if replicaState not yet initialized.
	if tc.ReplicationFactor > 1 {
		if ps == nil || ps.replicaState == nil {
			w.Header().Set("Retry-After", "1")
			writeError(w, 503, "partition not ready for replicated writes")
			return
		}
		if !s.verifyProduceLeadership(topicName, partitionID, ps.epoch) {
			routing := s.getRoutingMap(topicName)
			writeJSON(w, 421, routing)
			return
		}
	}

	assignedOffsets, err := s.partitionManager.AppendBatch(r.Context(), topicName, partitionID, batch)
	if err != nil {
		if errors.Is(err, producer.ErrBackpressure) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "backpressure: buffer full")
			return
		}
		slog.Error("produce_failed", "topic", topicName, "partition", partitionID, "error", err)
		writeError(w, http.StatusInternalServerError, "append failed: "+err.Error())
		return
	}

	if ps != nil && ps.replicaState != nil {
		lastOffset := assignedOffsets[len(assignedOffsets)-1]
		ps.replicaState.SetLeaderOffset(lastOffset + 1)
		ps.replicaState.NotifyNewData()

		if err := ps.replicaState.Purgatory().Wait(lastOffset, 30*time.Second); err != nil {
			if errors.Is(err, replication.ErrReplicationTimeout) {
				writeError(w, 408, "replication timeout")
				return
			}
			writeError(w, http.StatusInternalServerError, "replication error: "+err.Error())
			return
		}
	}

	if ps != nil {
		w.Header().Set("X-Leader-Epoch", strconv.FormatUint(ps.epoch, 10))
	}

	offsets := make([]offsetInfo, len(assignedOffsets))
	for i, o := range assignedOffsets {
		offsets[i] = offsetInfo{
			Partition: partitionID,
			Offset:    o,
		}
	}

	writeJSON(w, http.StatusOK, produceResponse{Offsets: offsets})
}
