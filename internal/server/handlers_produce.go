package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/idempotency"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/producer"
	"github.com/maksim/camu/internal/storage"
)

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
	if topicCfg.Schema != nil {
		for i, m := range msgs {
			if err := validateTypedValue(topicCfg.Schema, m.Value); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("message %d: %v", i, err))
				return
			}
		}
	}

	router := s.partitionManager.GetRouter(topicName)
	if router == nil {
		// Diskless topics are not registered in the partition manager, so
		// create an ephemeral router for key-based partition routing.
		if topicCfg.StorageMode == "diskless" {
			router = producer.NewRouter(topicCfg.Partitions)
		} else {
			writeError(w, http.StatusInternalServerError, "topic not initialized")
			return
		}
	}

	// Group messages by partition for batch appends.
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
	// both ownership and epoch in a single assignmentsMu.RLock. Diskless topics
	// are stateless and accept produce on any node, so they skip ownership.
	if topicCfg.ReplicationFactor <= 1 && topicCfg.StorageMode != "diskless" {
		for partitionID := range byPartition {
			if !s.isOwnedPartition(topicName, partitionID) {
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				s.proxyOrRejectNotLeader(w, r, topicName, partitionID)
				return
			}
		}
	}

	// Append each partition's batch in one native write.
	offsets := make([]offsetInfo, len(msgs))
	for partitionID, group := range byPartition {
		batch := make([]log.Message, len(group))
		for i, im := range group {
			batch[i] = im.msg
		}

		var ps *partitionState
		if topicCfg.StorageMode != "diskless" {
			ps = s.partitionManager.GetPartitionState(topicName, partitionID)
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
					r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
					s.proxyOrRejectNotLeader(w, r, topicName, partitionID)
					return
				}
			} else if !s.verifyPartitionFence(r.Context(), topicName, partitionID, ps.epoch) {
				// rf=1 has no ISR quorum to fence on: re-verify ownership against
				// the authoritative assignment store (amortized by fenceInterval)
				// so a fenced leader stops acknowledging.
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				s.proxyOrRejectNotLeader(w, r, topicName, partitionID)
				return
			}
		}

		assignedOffsets, err := s.appendHTTPMessagesAsRecordBatch(r.Context(), ps, topicName, partitionID, batch)
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

			if err := waitForReplicatedOffset(r.Context(), ps, lastOffset, s.replicationTimeout); err != nil {
				slog.Warn("produce_replication_timeout",
					"topic", topicName, "partition", partitionID,
					"offset", lastOffset, "hw", ps.replicaState.HighWatermark(),
					"isr_size", ps.replicaState.ISRSize())
				writeReplicationError(w, err)
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

	// Check leadership before consuming the request body. A follower forwards
	// the original request to the leader; parsing first would exhaust the body
	// and make the leader receive an empty request. Diskless topics are
	// stateless and accept produce on any node, so they skip ownership.
	if tc.ReplicationFactor <= 1 && tc.StorageMode != "diskless" {
		if !s.isOwnedPartition(topicName, partitionID) {
			s.proxyOrRejectNotLeader(w, r, topicName, partitionID)
			return
		}
	}

	var ps *partitionState
	if tc.StorageMode != "diskless" {
		ps = s.partitionManager.GetPartitionState(topicName, partitionID)
		if ps == nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("partition %d not initialized for topic %q", partitionID, topicName))
			return
		}
		if tc.ReplicationFactor > 1 {
			if !s.verifyProduceLeadership(topicName, partitionID, ps.epoch) {
				s.proxyOrRejectNotLeader(w, r, topicName, partitionID)
				return
			}
			if ps.replicaState == nil {
				w.Header().Set("Retry-After", "1")
				writeError(w, http.StatusServiceUnavailable, "partition not ready for replicated writes")
				return
			}
		} else if !s.verifyPartitionFence(r.Context(), topicName, partitionID, ps.epoch) {
			// rf=1 has no ISR quorum to fence on: re-verify ownership against the
			// authoritative assignment store (amortized by fenceInterval) so a
			// fenced leader stops acknowledging.
			s.proxyOrRejectNotLeader(w, r, topicName, partitionID)
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
	if tc.Schema != nil {
		for i, m := range msgs {
			if err := validateTypedValue(tc.Schema, m.Value); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("message %d: %v", i, err))
				return
			}
		}
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

	var assignedOffsets []uint64
	var duplicate bool
	if producerID != 0 {
		if tc.StorageMode == meta.StorageModeDiskless {
			assignedOffsets, duplicate, err = s.appendDisklessMessagesWithMeta(r.Context(), topicName, partitionID, log.Batch{
				ProducerID: producerID,
				Sequence:   sequence,
				Messages:   batch,
			})
		} else {
			assignedOffsets, err = s.partitionManager.appendBatchWithMetaToPS(ps, topicName, partitionID, log.Batch{
				ProducerID: producerID,
				Sequence:   sequence,
				Messages:   batch,
			}, &IdempotencyOpts{
				Sequence: sequence,
			})
			if errors.Is(err, idempotency.ErrDuplicateSequence) {
				s.handleDuplicateSequence(w, r, ps, partitionID, producerID)
				return
			}
		}
	} else {
		assignedOffsets, err = s.appendHTTPMessagesAsRecordBatch(r.Context(), ps, topicName, partitionID, batch)
	}
	if err != nil {
		if errors.Is(err, producer.ErrBackpressure) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "backpressure: buffer full")
			return
		}
		if errors.Is(err, idempotency.ErrSequenceGap) || errors.Is(err, idempotency.ErrUnknownProducer) ||
			errors.Is(err, diskless.ErrSequenceGap) || errors.Is(err, diskless.ErrOutOfOrderSequence) {
			writeError(w, 422, err.Error())
			return
		}
		slog.Error("produce_failed", "topic", topicName, "partition", partitionID, "error", err)
		writeError(w, http.StatusInternalServerError, "append failed: "+err.Error())
		return
	}
	if duplicate {
		// An idempotent retry of a batch that was already allocated: the data
		// was re-materialized at the original offsets, so confirm them.
		offsets := make([]offsetInfo, len(assignedOffsets))
		for i, o := range assignedOffsets {
			offsets[i] = offsetInfo{Partition: partitionID, Offset: o}
		}
		writeJSON(w, http.StatusOK, struct {
			Duplicate bool         `json:"duplicate"`
			Offsets   []offsetInfo `json:"offsets"`
		}{Duplicate: true, Offsets: offsets})
		return
	}

	if ps != nil && ps.replicaState != nil {
		lastOffset := assignedOffsets[len(assignedOffsets)-1]
		if writeReplicationError(w, waitForReplicatedOffset(r.Context(), ps, lastOffset, s.replicationTimeout)) {
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

// appendDisklessMessagesWithMeta appends an idempotent batch to a diskless
// topic, encoding the producer metadata into the RecordBatch so the metastore
// can deduplicate retried batches. Duplicate reports whether the batch was an
// exact retry of a previously allocated range.
func (s *Server) appendDisklessMessagesWithMeta(ctx context.Context, topic string, partitionID int, batch log.Batch) ([]uint64, bool, error) {
	now := time.Now().UnixMilli()
	for i := range batch.Messages {
		if batch.Messages[i].Timestamp == 0 {
			batch.Messages[i].Timestamp = now
		}
	}
	rawBatch := log.EncodeRecordBatchWithMeta(0, batch)
	result, err := s.disklessEngine.Produce(ctx, topic, partitionID, rawBatch)
	if err != nil {
		return nil, false, err
	}
	offsets := make([]uint64, len(batch.Messages))
	for i := range offsets {
		offsets[i] = uint64(result.BaseOffset) + uint64(i)
	}
	return offsets, result.Duplicate, nil
}

// handleDuplicateSequence handles the ErrDuplicateSequence case for idempotent
// produce. It waits for the original batch to be replicated before confirming.
func (s *Server) handleDuplicateSequence(w http.ResponseWriter, r *http.Request, ps *partitionState, partition int, producerID uint64) {
	if ps == nil {
		writeJSON(w, http.StatusOK, struct {
			Duplicate bool         `json:"duplicate"`
			Offsets   []offsetInfo `json:"offsets"`
		}{Duplicate: true})
		return
	}

	ps.mu.RLock()
	lastOff, ok := ps.getLastOffset(producerID)
	hw, hwOK := readableHighWatermark(ps)
	replicaState := ps.replicaState
	ps.mu.RUnlock()

	if ok && hwOK && hw > lastOff {
		writeJSON(w, http.StatusOK, struct {
			Duplicate bool         `json:"duplicate"`
			Offsets   []offsetInfo `json:"offsets"`
		}{Duplicate: true, Offsets: []offsetInfo{{Partition: partition, Offset: lastOff}}})
		return
	}
	if ok && replicaState != nil {
		if err := waitForReplicatedOffset(r.Context(), ps, lastOff, s.replicationTimeout); err != nil {
			writeReplicationError(w, err)
			return
		}
	}

	writeJSON(w, http.StatusOK, struct {
		Duplicate bool         `json:"duplicate"`
		Offsets   []offsetInfo `json:"offsets"`
	}{Duplicate: true, Offsets: []offsetInfo{{Partition: partition, Offset: lastOff}}})
}
