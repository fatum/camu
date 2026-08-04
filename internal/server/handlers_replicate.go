package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/replication"
)

func (s *Server) handleReplicaFetchTCP(ctx context.Context, req *replication.ReplicaFetchRequest) (*replication.ReplicaFetchResponse, error) {
	topic := req.Topic
	pid := int(req.PartitionID)
	fromOffset := req.FromOffset
	replicaID := req.ReplicaID
	replicaOffset := req.ReplicaOffset
	replicaEpoch := req.ReplicaEpoch

	ps := s.partitionManager.GetPartitionState(topic, pid)
	if ps == nil {
		slog.Debug("replica_fetch: partition not found or not replicated",
			"topic", topic, "pid", pid, "replica", replicaID)
		return &replication.ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     replication.ReplicaErrNotFound,
		}, nil
	}

	ps.mu.Lock()
	if !ps.isLeader || ps.replicaState == nil {
		ps.mu.Unlock()
		slog.Debug("replica_fetch: partition is no longer local leader",
			"topic", topic, "pid", pid, "replica", replicaID)
		return &replication.ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     replication.ReplicaErrNotFound,
		}, nil
	}
	replicaState := ps.replicaState
	truncateTo, diverged := replicaState.CheckDivergence(replicaEpoch, replicaOffset)
	if diverged {
		epoch := ps.epoch
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
		return &replication.ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     replication.ReplicaErrTruncate,
			TruncateTo:    truncateTo,
			LeaderEpoch:   epoch,
			HighWatermark: replicaState.HighWatermark(),
		}, nil
	}

	replicaState.UpdateFollower(replicaID, replicaOffset)

	activeBase := replicaActiveBase(ps)
	dataAvailable := fromOffset >= activeBase && fromOffset < ps.nextOffset
	behindSealedPrefix := fromOffset < activeBase
	ps.mu.Unlock()

	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}

	readBatches := func() ([]byte, error) {
		bytes, _, err := s.partitionManager.ReadReplicaRawBatches(ctx, topic, pid, int64(fromOffset), maxBytes)
		return bytes, err
	}
	var rawBytes []byte
	var readErr error
	if dataAvailable {
		rawBytes, readErr = readBatches()
		if readErr != nil {
			slog.Error("replica_fetch: ReadRawBatches failed",
				"topic", topic, "pid", pid, "from_offset", fromOffset, "error", readErr)
			return &replication.ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     replication.ReplicaErrInternal,
			}, nil
		}
	}

	if len(rawBytes) == 0 && !behindSealedPrefix {
		if replicaState.WaitForData(500 * time.Millisecond) {
			rawBytes, readErr = readBatches()
			if readErr != nil {
				slog.Error("replica_fetch: ReadRawBatches after wait failed",
					"topic", topic, "pid", pid, "from_offset", fromOffset, "error", readErr)
			}
		}
	}

	ps.mu.RLock()
	if !ps.isLeader || ps.replicaState != replicaState {
		ps.mu.RUnlock()
		slog.Debug("replica_fetch: partition demoted during fetch",
			"topic", topic, "pid", pid, "replica", replicaID)
		return &replication.ReplicaFetchResponse{
			CorrelationID: req.CorrelationID,
			ErrorCode:     replication.ReplicaErrNotFound,
		}, nil
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

	return &replication.ReplicaFetchResponse{
		CorrelationID: req.CorrelationID,
		ErrorCode:     replication.ReplicaErrOK,
		LeaderEpoch:   respEpoch,
		HighWatermark: respHW,
		FlushedOffset: respFlushed,
		ActiveBase:    respActiveBase,
		BatchData:     rawBytes,
	}, nil
}

// replicaActiveBase is the offset where the leader's local tail begins. The
// preceding prefix is sealed and must be read by followers through S3.
func replicaActiveBase(ps *partitionState) uint64 {
	if ps.activeSegment != nil {
		return uint64(ps.activeSegment.BaseOffset())
	}
	return ps.nextOffset
}
