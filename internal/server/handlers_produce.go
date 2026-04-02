package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"unsafe"

	"github.com/maksim/camu/internal/idempotency"
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

// produceBatchRequest is the idempotent produce format:
// {"producer_id": N, "sequence": M, "messages": [...]}
type produceBatchRequest struct {
	ProducerID uint64                  `json:"producer_id"`
	Sequence   uint64                  `json:"sequence"`
	Messages   []produceMessageRequest `json:"messages"`
}

type produceResponse struct {
	Offsets []offsetInfo `json:"offsets"`
}

type offsetInfo struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

func newBodyDecoder(r io.Reader) (*json.Decoder, byte, error) {
	br := bufio.NewReader(r)
	first, err := peekFirstNonSpaceByte(br)
	if err != nil {
		return nil, 0, err
	}
	return json.NewDecoder(br), first, nil
}

func peekFirstNonSpaceByte(r *bufio.Reader) (byte, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			if err := r.UnreadByte(); err != nil {
				return 0, err
			}
			return b, nil
		}
	}
}

// immutableStringBytes exposes a string as a read-only byte slice without
// copying. The returned slice must never be mutated.
func immutableStringBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func (s *Server) handleProduceHighLevel(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown.Load() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "server is shutting down")
		return
	}

	// Buffer body so it can be replayed if we need to proxy to the leader.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	topicName := r.PathValue("topic")

	// Validate topic exists and cache config for the per-partition loop.
	topicCfg, err := s.topicStore.Get(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Parse body: JSON array only. Idempotent batch format is not allowed on
	// the high-level endpoint — use the partition-specific endpoint for
	// idempotent produce.
	var msgs []produceMessageRequest
	dec, firstByte, err := newBodyDecoder(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	switch firstByte {
	case '[':
		if err := dec.Decode(&msgs); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: expected array")
			return
		}
	case '{':
		var batchReq produceBatchRequest
		if err := dec.Decode(&batchReq); err == nil && batchReq.Messages != nil {
			writeError(w, http.StatusBadRequest, "idempotent produce requires the partition-specific endpoint: POST /v1/topics/{topic}/partitions/{id}/messages")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request body: expected array")
		return
	default:
		writeError(w, http.StatusBadRequest, "invalid request body: expected array")
		return
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
		key := immutableStringBytes(m.Key)
		partitionID := router.Route(key)
		byPartition[partitionID] = append(byPartition[partitionID], indexedMsg{
			idx:       i,
			partition: partitionID,
			msg: log.Message{
				Key:     key,
				Value:   immutableStringBytes(m.Value),
				Headers: m.Headers,
			},
		})
	}

	// For non-replicated topics, check ownership early to avoid unnecessary work.
	// Replicated topics defer the check to verifyProduceLeadership which checks
	// both ownership and epoch in a single assignmentsMu.RLock.
	if topicCfg.ReplicationFactor <= 1 {
		for partitionID := range byPartition {
			if !s.isOwnedPartition(topicName, partitionID) {
				if leaderAddr := s.leaderInternalAddr(topicName, partitionID); leaderAddr != "" && r.Header.Get(headerForwardedBy) == "" {
					r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
					s.proxyToLeader(w, r, leaderAddr)
					return
				}
				routing := s.getRoutingMap(topicName)
				writeJSON(w, 421, routing)
				return
			}
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
		if ps == nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("partition %d not initialized for topic %q", partitionID, topicName))
			return
		}

		// For replicated topics, reject writes if replicaState not yet initialized.
		// Don't check min_insync_replicas here — the purgatory will wait until
		// enough ISR members ack. This avoids a chicken-and-egg problem where
		// followers can't catch up (join ISR) if no data flows.
		// verifyProduceLeadership checks both ownership and epoch in a single
		// assignmentsMu.RLock, avoiding a separate isOwnedPartition call.
		if topicCfg.ReplicationFactor > 1 {
			if ps.replicaState == nil {
				slog.Debug("produce_rejected: replicaState not ready",
					"topic", topicName, "partition", partitionID)
				w.Header().Set("Retry-After", "1")
				writeError(w, 503, "partition not ready for replicated writes")
				return
			}
			if !s.verifyProduceLeadership(topicName, partitionID, ps.epoch) {
				if leaderAddr := s.leaderInternalAddr(topicName, partitionID); leaderAddr != "" && r.Header.Get(headerForwardedBy) == "" {
					r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
					s.proxyToLeader(w, r, leaderAddr)
					return
				}
				routing := s.getRoutingMap(topicName)
				writeJSON(w, 421, routing)
				return
			}
		}

		assignedOffsets, err := s.partitionManager.appendBatchToPS(ps, topicName, partitionID, batch)
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

			if err := ps.replicaState.Purgatory().Wait(r.Context(), lastOffset, s.replicationTimeout); err != nil {
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

	// For non-replicated topics, check ownership early to skip body parsing
	// for non-owned partitions. Replicated topics defer the check to
	// verifyProduceLeadership which checks both ownership and epoch in one
	// lock acquisition.
	if tc.ReplicationFactor <= 1 {
		if !s.isOwnedPartition(topicName, partitionID) {
			if leaderAddr := s.leaderInternalAddr(topicName, partitionID); leaderAddr != "" && r.Header.Get(headerForwardedBy) == "" {
				s.proxyToLeader(w, r, leaderAddr)
				return
			}
			routing := s.getRoutingMap(topicName)
			writeJSON(w, 421, routing)
			return
		}
	}

	// Parse body: idempotent batch or JSON array.
	var (
		msgs       []produceMessageRequest
		producerID uint64
		sequence   uint64
	)
	dec, firstByte, err := newBodyDecoder(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	switch firstByte {
	case '{':
		var batchReq produceBatchRequest
		if err := dec.Decode(&batchReq); err != nil || batchReq.Messages == nil {
			writeError(w, http.StatusBadRequest, "invalid request body: expected batch or array")
			return
		}
		msgs = batchReq.Messages
		producerID = batchReq.ProducerID
		sequence = batchReq.Sequence
	case '[':
		if err := dec.Decode(&msgs); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: expected batch or array")
			return
		}
	default:
		writeError(w, http.StatusBadRequest, "invalid request body: expected batch or array")
		return
	}

	if len(msgs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one message is required")
		return
	}

	batch := make([]log.Message, len(msgs))
	for i, m := range msgs {
		key := immutableStringBytes(m.Key)
		batch[i] = log.Message{
			Key:     key,
			Value:   immutableStringBytes(m.Value),
			Headers: m.Headers,
		}
	}

	ps := s.partitionManager.GetPartitionState(topicName, partitionID)
	if ps == nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("partition %d not initialized for topic %q", partitionID, topicName))
		return
	}

	// For replicated topics, reject writes if replicaState not yet initialized.
	// verifyProduceLeadership checks both ownership and epoch in a single
	// assignmentsMu.RLock, avoiding a separate isOwnedPartition call.
	if tc.ReplicationFactor > 1 {
		if !s.verifyProduceLeadership(topicName, partitionID, ps.epoch) {
			if leaderAddr := s.leaderInternalAddr(topicName, partitionID); leaderAddr != "" && r.Header.Get(headerForwardedBy) == "" {
				s.proxyToLeader(w, r, leaderAddr)
				return
			}
			routing := s.getRoutingMap(topicName)
			writeJSON(w, 421, routing)
			return
		}
		if ps.replicaState == nil {
			w.Header().Set("Retry-After", "1")
			writeError(w, 503, "partition not ready for replicated writes")
			return
		}
	}

	var assignedOffsets []uint64
	if producerID != 0 {
		assignedOffsets, err = s.partitionManager.appendBatchWithMetaToPS(ps, topicName, partitionID, log.Batch{
			ProducerID: producerID,
			Sequence:   sequence,
			Messages:   batch,
		}, &IdempotencyOpts{
			Sequence: sequence,
		})
		if errors.Is(err, idempotency.ErrDuplicateSequence) {
			// Join the replication purgatory for the original batch —
			// only return success once the data is committed.
			if ps != nil {
				ps.mu.RLock()
				lastOff, ok := ps.getLastOffset(producerID)
				hw, hwOK := readableHighWatermark(ps)
				replicaState := ps.replicaState
				ps.mu.RUnlock()
				if ok && hwOK && hw > lastOff {
					writeJSON(w, http.StatusOK, map[string]bool{"duplicate": true})
					return
				}
				if ok && replicaState != nil {
					if waitErr := replicaState.Purgatory().Wait(r.Context(), lastOff, s.replicationTimeout); waitErr != nil {
						if errors.Is(waitErr, replication.ErrReplicationTimeout) {
							writeError(w, 408, "replication timeout")
							return
						}
						writeError(w, http.StatusInternalServerError, "replication error: "+waitErr.Error())
						return
					}
				}
			}
			writeJSON(w, http.StatusOK, map[string]bool{"duplicate": true})
			return
		}
	} else {
		assignedOffsets, err = s.partitionManager.appendBatchToPS(ps, topicName, partitionID, batch)
	}
	if err != nil {
		if errors.Is(err, producer.ErrBackpressure) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "backpressure: buffer full")
			return
		}
		if errors.Is(err, idempotency.ErrSequenceGap) || errors.Is(err, idempotency.ErrUnknownProducer) {
			writeError(w, 422, err.Error())
			return
		}
		slog.Error("produce_failed", "topic", topicName, "partition", partitionID, "error", err)
		writeError(w, http.StatusInternalServerError, "append failed: "+err.Error())
		return
	}

	if ps != nil && ps.replicaState != nil {
		lastOffset := assignedOffsets[len(assignedOffsets)-1]
		if err := ps.replicaState.Purgatory().Wait(r.Context(), lastOffset, s.replicationTimeout); err != nil {
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
