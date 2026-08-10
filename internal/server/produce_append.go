package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/replication"
)

// appendHTTPMessagesAsRecordBatch is the HTTP produce fast path:
// it stamps missing timestamps, encodes the HTTP messages into one Kafka
// RecordBatch, and appends that native batch directly to the active segment.
//
// The only intentional format conversion is HTTP JSON -> Kafka RecordBatch.
// If the active segment is unavailable, it falls back to the generic native
// append path that performs the same conversion through log.Batch.
func (s *Server) appendHTTPMessagesAsRecordBatch(ctx context.Context, ps *partitionState, topic string, partitionID int, msgs []log.Message) ([]uint64, error) {
	// HTTP handlers construct this batch for this append, so it is safe to
	// stamp it in place. Avoiding a second []log.Message copy is material for
	// large HTTP requests.
	now := time.Now().UnixMilli()
	for i := range msgs {
		if msgs[i].Timestamp == 0 {
			msgs[i].Timestamp = now
		}
	}
	rawBatch := log.EncodeRecordBatch(0, msgs)

	if s.isTopicDiskless(ctx, topic) {
		result, err := s.disklessEngine.Produce(ctx, topic, partitionID, rawBatch)
		if err != nil {
			return nil, err
		}
		offsets := make([]uint64, len(msgs))
		for i := range offsets {
			offsets[i] = uint64(result.BaseOffset) + uint64(i)
		}
		return offsets, nil
	}

	baseOffset, err := s.partitionManager.AppendRawBatch(ctx, topic, partitionID, rawBatch)
	if err == nil {
		offsets := make([]uint64, len(msgs))
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
	return s.partitionManager.appendBatchToPS(ps, topic, partitionID, msgs)
}

// isRawBatchUnavailable reports whether err indicates that AppendRawBatch
// cannot be used (active segment not initialized, or this node is not marked
// as leader in the partition state). In both cases the caller should fall back
// to the standard append path.
func isRawBatchUnavailable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errKafkaNotLeader) || errors.Is(err, errKafkaSegmentNotReady)
}

// waitForReplicatedOffset blocks until the given offset has been replicated
// to enough ISR members or the timeout expires. Once the offset is committed,
// the leader persists its high watermark to the ISR store BEFORE returning, so
// a produce is only acknowledged after the committed watermark it advanced is
// durable: a later takeover can never truncate acked records based on a stale
// recorded watermark. rf=1 topics have no ISR tracking and skip both.
func waitForReplicatedOffset(ctx context.Context, s *Server, ps *partitionState, topic string, pid int, offset uint64, timeout time.Duration) error {
	if ps == nil || ps.replicaState == nil {
		return nil
	}

	ps.mu.RLock()
	hw, hwOK := readableHighWatermark(ps)
	replicaState := ps.replicaState
	ps.mu.RUnlock()

	if !(hwOK && hw > offset) {
		if err := replicaState.Purgatory().Wait(ctx, offset, timeout); err != nil {
			return err
		}
	}
	return s.persistCommittedHW(ctx, ps, topic, pid)
}

// persistCommittedHW durably records the partition's current high watermark in
// the ISR store (raising it if it advanced past the last persisted value), so
// the committed watermark a produce acknowledges is never stale in the object
// store. It is called on the ack path before the produce returns.
func (s *Server) persistCommittedHW(ctx context.Context, ps *partitionState, topic string, pid int) error {
	if s.isrStore == nil {
		return nil
	}
	ps.mu.RLock()
	rs := ps.replicaState
	if rs == nil {
		ps.mu.RUnlock()
		return nil
	}
	hw := rs.HighWatermark()
	epoch := ps.epoch
	ps.mu.RUnlock()

	key := fmt.Sprintf("%s/%d", topic, pid)
	s.isrWriteMu.Lock()
	last := s.lastISRWrite[key]
	s.isrWriteMu.Unlock()
	if hw <= last {
		return nil // already durable
	}
	if err := s.isrStore.Update(ctx, topic, pid, epoch, func(cur replication.ISRState) (replication.ISRState, error) {
		if hw > cur.HighWatermark {
			cur.HighWatermark = hw
		}
		return cur, nil
	}); err != nil {
		return fmt.Errorf("persist committed hw %s/%d: %w", topic, pid, err)
	}
	s.isrWriteMu.Lock()
	if hw > s.lastISRWrite[key] {
		s.lastISRWrite[key] = hw
	}
	s.isrWriteMu.Unlock()
	return nil
}
