package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/maksim/camu/internal/log"
)

// appendHTTPMessagesAsRecordBatch is the HTTP produce fast path:
// it stamps missing timestamps, encodes the HTTP messages into one Kafka
// RecordBatch, and appends that native batch directly to the active segment.
//
// The only intentional format conversion is HTTP JSON -> Kafka RecordBatch.
// If the active segment is unavailable, it falls back to the generic native
// append path that performs the same conversion through log.Batch.
func (s *Server) appendHTTPMessagesAsRecordBatch(ctx context.Context, ps *partitionState, topic string, partitionID int, msgs []log.Message) ([]uint64, error) {
	stamped := make([]log.Message, len(msgs))
	copy(stamped, msgs)
	now := time.Now().UnixMilli()
	for i := range stamped {
		if stamped[i].Timestamp == 0 {
			stamped[i].Timestamp = now
		}
	}
	rawBatch := log.EncodeRecordBatch(0, stamped)

	if s.isTopicDiskless(ctx, topic) {
		result, err := s.disklessEngine.Produce(ctx, topic, partitionID, rawBatch)
		if err != nil {
			return nil, err
		}
		offsets := make([]uint64, len(stamped))
		for i := range offsets {
			offsets[i] = uint64(result.BaseOffset) + uint64(i)
		}
		return offsets, nil
	}

	baseOffset, err := s.partitionManager.AppendRawBatch(ctx, topic, partitionID, rawBatch)
	if err == nil {
		offsets := make([]uint64, len(stamped))
		for i := range offsets {
			offsets[i] = uint64(baseOffset) + uint64(i)
		}
		return offsets, nil
	}
	if !isRawBatchUnavailable(err) {
		return nil, err
	}
	// Active segment not ready — fall back to the standard append path.
	slog.Debug("produce_active_segment_fallback",
		"topic", topic, "partition", partitionID, "err", err)
	return s.partitionManager.appendBatchToPS(ps, topic, partitionID, stamped)
}

// isRawBatchUnavailable reports whether err indicates that AppendRawBatch
// cannot be used (active segment not initialized, or this node is not marked
// as leader in the partition state). In both cases the caller should fall back
// to the standard append path.
func isRawBatchUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errKafkaNotLeader) {
		return true
	}
	return strings.Contains(err.Error(), "active segment not initialized")
}

// waitForReplicatedOffset blocks until the given offset has been replicated
// to enough ISR members or the timeout expires.
func waitForReplicatedOffset(ctx context.Context, ps *partitionState, offset uint64, timeout time.Duration) error {
	if ps == nil || ps.replicaState == nil {
		return nil
	}

	ps.mu.RLock()
	hw, hwOK := readableHighWatermark(ps)
	replicaState := ps.replicaState
	ps.mu.RUnlock()

	if hwOK && hw > offset {
		return nil
	}
	return replicaState.Purgatory().Wait(ctx, offset, timeout)
}
