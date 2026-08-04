package server

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/replication"
)

func (s *Server) handleReplicaFetchTCP(ctx context.Context, req *replication.ReplicaFetchRequest) (*replication.ReplicaFetchResult, error) {
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
		return &replication.ReplicaFetchResult{
			Resp: &replication.ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     replication.ReplicaErrNotFound,
			},
		}, nil
	}

	ps.mu.Lock()
	if !ps.isLeader || ps.replicaState == nil {
		ps.mu.Unlock()
		slog.Debug("replica_fetch: partition is no longer local leader",
			"topic", topic, "pid", pid, "replica", replicaID)
		return &replication.ReplicaFetchResult{
			Resp: &replication.ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     replication.ReplicaErrNotFound,
			},
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
		return &replication.ReplicaFetchResult{
			Resp: &replication.ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     replication.ReplicaErrTruncate,
				TruncateTo:    truncateTo,
				LeaderEpoch:   epoch,
				HighWatermark: replicaState.HighWatermark(),
			},
		}, nil
	}

	replicaState.UpdateFollower(replicaID, replicaOffset)

	activeBase := replicaActiveBase(ps)
	behindSealedPrefix := fromOffset < activeBase
	ps.mu.Unlock()

	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}

	fetchRange := func() (ReplicaBatchRange, error) {
		return s.partitionManager.ReadReplicaBatchRange(topic, pid, int64(fromOffset), maxBytes)
	}

	batchRange, err := fetchRange()
	if err != nil {
		slog.Error("replica_fetch: ReadReplicaBatchRange failed",
			"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
		return &replication.ReplicaFetchResult{
			Resp: &replication.ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     replication.ReplicaErrInternal,
			},
		}, nil
	}

	if batchRange.Length == 0 && !behindSealedPrefix {
		if replicaState.WaitForData(500 * time.Millisecond) {
			batchRange, err = fetchRange()
			if err != nil {
				slog.Error("replica_fetch: ReadReplicaBatchRange after wait failed",
					"topic", topic, "pid", pid, "from_offset", fromOffset, "error", err)
			}
		}
	}

	ps.mu.RLock()
	if !ps.isLeader || ps.replicaState != replicaState {
		ps.mu.RUnlock()
		slog.Debug("replica_fetch: partition demoted during fetch",
			"topic", topic, "pid", pid, "replica", replicaID)
		return &replication.ReplicaFetchResult{
			Resp: &replication.ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     replication.ReplicaErrNotFound,
			},
		}, nil
	}
	respHW := replicaState.HighWatermark()
	respEpoch := ps.epoch
	respFlushed := ps.flushedOffset
	respActiveBase := replicaActiveBase(ps)
	ps.mu.RUnlock()

	slog.Debug("replica_fetch: serving",
		"topic", topic, "pid", pid, "replica", replicaID,
		"from_offset", fromOffset, "bytes", batchRange.Length,
		"hw", respHW, "epoch", respEpoch)

	resp := &replication.ReplicaFetchResponse{
		CorrelationID: req.CorrelationID,
		ErrorCode:     replication.ReplicaErrOK,
		LeaderEpoch:   respEpoch,
		HighWatermark: respHW,
		FlushedOffset: respFlushed,
		ActiveBase:    respActiveBase,
	}

	if batchRange.Length > 0 && batchRange.File != nil {
		return &replication.ReplicaFetchResult{
			Resp:        resp,
			BatchReader: io.NewSectionReader(batchRange.File, batchRange.FileOffset, batchRange.Length),
			BatchLen:    int32(batchRange.Length),
		}, nil
	}

	return &replication.ReplicaFetchResult{Resp: resp}, nil
}

// replicaActiveBase is the offset where the leader's local tail begins. The
// preceding prefix is sealed and must be read by followers through S3.
func replicaActiveBase(ps *partitionState) uint64 {
	if ps.activeSegment != nil {
		return uint64(ps.activeSegment.BaseOffset())
	}
	return ps.nextOffset
}
