package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"

	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

type partitionFollowerService struct {
	server *Server
}

func (s *Server) partitionFollower() partitionFollowerService {
	return partitionFollowerService{server: s}
}

func (p partitionFollowerService) leaderInternalAddr(topic string, pid int) string {
	ctx := context.Background()
	assigned, err := p.server.assignmentStore.Read(ctx, topic)
	if err != nil {
		return ""
	}
	pa, ok := assigned.Partitions[pid]
	if !ok {
		return ""
	}
	leaderID := pa.Leader
	if leaderID == "" || leaderID == p.server.instanceID {
		return ""
	}
	info, err := p.server.registry.GetInstanceInfo(ctx, leaderID)
	if err != nil {
		return ""
	}
	addr := info.InternalAddress
	if addr == "" {
		addr = info.Address
	}
	return addr
}

func (p partitionFollowerService) proxyToLeader(w http.ResponseWriter, r *http.Request, leaderAddr string) {
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = leaderAddr
			req.Host = leaderAddr
			req.Header.Set(headerForwardedBy, p.server.instanceID)
		},
		ModifyResponse: func(resp *http.Response) error {
			if id := resp.Header.Get("X-Camu-Instance-ID"); id != "" {
				resp.Header.Set("X-Camu-Instance-ID", id)
			}
			return nil
		},
		Transport: p.server.internalClient.Transport,
	}
	w.Header().Del("X-Camu-Instance-ID")
	w.Header().Del("Content-Type")
	proxy.ServeHTTP(w, r)
}

func (p partitionFollowerService) handleLeaderDown(topic string, pid int) {
	slog.Warn("leader down detected, reporting to controller", "topic", topic, "pid", pid)
	if err := p.server.reportFailureToController(topic, pid); err != nil {
		slog.Error("report failure to controller failed, falling back to self-election",
			"topic", topic, "pid", pid, "err", err)
		if err := p.attemptPartitionLeadership(topic, pid); err != nil {
			slog.Error("fallback self-election also failed",
				"topic", topic, "pid", pid, "err", err)
		}
	}
}

func (p partitionFollowerService) attemptPartitionLeadership(topic string, pid int) error {
	ctx := context.Background()

	isrState, isrErr := p.server.isrStore.Read(ctx, topic, pid)
	if isrErr != nil {
		slog.Warn("attemptLeadership: no ISR state", "topic", topic, "pid", pid)
	}

	if isrErr == nil {
		inISR := false
		for _, id := range isrState.ISR {
			if id == p.server.instanceID {
				inISR = true
				break
			}
		}
		if !inISR {
			topicCfg, _ := p.server.topicStore.Get(ctx, topic)
			if !topicCfg.UncleanLeaderElection {
				return fmt.Errorf("not in ISR and unclean election disabled")
			}
			slog.Warn("attemptLeadership: unclean election", "topic", topic, "pid", pid)
		}
	}

	assignments, err := p.server.assignmentStore.Read(ctx, topic)
	if err != nil {
		return fmt.Errorf("read assignments: %w", err)
	}
	pa := assignments.Partitions[pid]
	if pa.Leader == p.server.instanceID {
		return nil
	}

	newEpoch := pa.LeaderEpoch + 1
	slog.Info("attempt_leadership_begin",
		"topic", topic,
		"partition", pid,
		"current_leader", pa.Leader,
		"current_epoch", pa.LeaderEpoch,
		"candidate", p.server.instanceID,
		"isr", func() []string {
			if isrErr != nil {
				return nil
			}
			return isrState.ISR
		}(),
		"isr_hw", func() uint64 {
			if isrErr != nil {
				return 0
			}
			return isrState.HighWatermark
		}(),
	)
	pa.Leader = p.server.instanceID
	pa.LeaderEpoch = newEpoch
	assignments.Partitions[pid] = pa
	assignments.Version++

	if err := p.server.assignmentStore.Write(ctx, topic, assignments, assignments.ETag); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("lost leadership race (CAS conflict)")
		}
		return fmt.Errorf("write assignments: %w", err)
	}

	slog.Info("won partition leadership", "topic", topic, "pid", pid, "epoch", newEpoch)

	ps := p.server.partitionManager.GetPartitionState(topic, pid)
	if ps == nil {
		return fmt.Errorf("partition state not found")
	}

	ps.mu.Lock()
	existingCancel := ps.fetchCancel
	existingDone := ps.fetchDone
	ps.fetchCancel = nil
	ps.fetchDone = nil
	ps.fetchAssignmentEpoch = 0
	ps.mu.Unlock()
	if existingCancel != nil {
		existingCancel()
		if existingDone != nil {
			<-existingDone
		}
	}

	p.server.partitionManager.RefreshIndex(ctx, topic, pid)
	if err := p.server.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("attemptPartitionLeadership: ensure active segment before recovery", "topic", topic, "pid", pid, "error", err)
	}

	ps.mu.RLock()
	prevEpoch := ps.epoch
	prevNextOffset := ps.nextOffset
	indexHW := ps.index.HighWatermark()
	flushedOffset := ps.flushedOffset
	ps.mu.RUnlock()
	logEnd := p.server.partitionManager.recoverLocalLogEnd(topic, pid)

	slog.Info("failover_recovery_state",
		"topic", topic,
		"partition", pid,
		"new_epoch", newEpoch,
		"previous_epoch", prevEpoch,
		"previous_next_offset", prevNextOffset,
		"log_end", logEnd,
		"index_hw", indexHW,
		"flushed_offset", flushedOffset,
		"isr_hw", func() uint64 {
			if isrErr != nil {
				return 0
			}
			return isrState.HighWatermark
		}(),
	)

	recoveredHW := logEnd
	ps.mu.RLock()
	indexNext := ps.index.NextOffset()
	currentIndexHW := ps.index.HighWatermark()
	ps.mu.RUnlock()
	if indexNext > recoveredHW {
		recoveredHW = indexNext
	}
	if isrErr == nil && isrState.HighWatermark > recoveredHW {
		recoveredHW = isrState.HighWatermark
	}
	slog.Info("failover: recovered HW",
		"topic", topic, "pid", pid,
		"hw", recoveredHW, "log_end", logEnd,
		"index_next", indexNext, "index_hw", currentIndexHW)

	ehPath := p.server.partitionManager.EpochHistoryPath(topic, pid)
	s3eh, _ := p.server.isrStore.ReadEpochHistory(ctx, topic, pid)
	ps.mu.RLock()
	eh := ps.epochHistory
	ps.mu.RUnlock()
	if s3eh != nil && len(s3eh.Entries) > 0 {
		eh = s3eh
	} else if eh == nil {
		eh, _ = replication.LoadEpochHistory(ehPath)
		if eh == nil {
			eh = &replication.EpochHistory{}
		}
	}
	eh.Append(replication.EpochEntry{Epoch: newEpoch, StartOffset: logEnd})
	ps.mu.Lock()
	ps.epochHistory = eh
	ps.isLeader = true
	ps.leaderID = ""
	ps.epoch = newEpoch
	ps.index.SetHighWatermark(recoveredHW)
	ps.mu.Unlock()
	if err := p.server.partitionManager.ensureActiveSegment(topic, pid); err != nil {
		slog.Warn("attemptPartitionLeadership: ensure active segment", "topic", topic, "pid", pid, "error", err)
	}
	if err := eh.SaveToFile(ehPath); err != nil {
		slog.Warn("attemptPartitionLeadership: save epoch history locally", "topic", topic, "pid", pid, "error", err)
	}
	if err := p.server.isrStore.WriteEpochHistory(ctx, topic, pid, eh); err != nil {
		slog.Warn("attemptPartitionLeadership: save epoch history to S3", "topic", topic, "pid", pid, "error", err)
	}

	topicCfg, err := p.server.topicStore.Get(ctx, topic)
	if err != nil {
		slog.Error("attemptPartitionLeadership: get topic config", "topic", topic, "pid", pid, "error", err)
		return err
	}
	if topicCfg.ReplicationFactor > 1 {
		ps.mu.Lock()
		ps.replicaState = replication.NewReplicaState(p.server.instanceID, recoveredHW, topicCfg.MinInsyncReplicas, p.server.cfg.Coordination.ISRExpansionThresholdValue())
		ps.replicaState.SetEpochHistory(ps.epochHistory)
		for _, r := range pa.Replicas {
			if r != p.server.instanceID {
				ps.replicaState.AddFollower(r)
			}
		}
		ps.mu.Unlock()
	}

	if err := p.server.isrStore.Update(ctx, topic, pid, newEpoch, func(_ replication.ISRState) (replication.ISRState, error) {
		return replication.ISRState{
			ISR:           []string{p.server.instanceID},
			Leader:        p.server.instanceID,
			HighWatermark: recoveredHW,
		}, nil
	}); err != nil {
		p.server.onISRWriteError(topic, pid, err)
	}

	checkpointKey := fmt.Sprintf("%s/%d/producers.checkpoint", topic, pid)
	if data, err := p.server.s3Client.Get(ctx, checkpointKey); err == nil && len(data) > 0 {
		ps.mu.Lock()
		ps.loadProducerCheckpoint(data)
		ps.mu.Unlock()
		slog.Info("idempotency_checkpoint_loaded", "topic", topic, "partition", pid, "size", len(data))
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		slog.Warn("idempotency_checkpoint_load_failed", "topic", topic, "partition", pid, "error", err)
	}

	if source, n := p.server.partitionManager.RebuildProducerStateFromLocalTail(topic, pid); n > 0 {
		slog.Info("idempotency_local_tail_recovery", "topic", topic, "partition", pid, "source", source, "batches", n)
	}

	ps.mu.RLock()
	indexNextOffset := ps.index.NextOffset()
	ps.mu.RUnlock()
	if recoveredHW > indexNextOffset {
		if err := p.server.partitionManager.flushRecoveredTail(topic, pid); err != nil {
			slog.Warn("attemptPartitionLeadership: flush recovered tail",
				"topic", topic,
				"partition", pid,
				"epoch", newEpoch,
				"recovered_hw", recoveredHW,
				"index_next_offset", indexNextOffset,
				"error", err,
			)
		}
	}

	p.server.assignmentsMu.Lock()
	if p.server.myPartitions[topic] == nil {
		p.server.myPartitions[topic] = make(map[int]localPartitionAssignment)
	}
	p.server.myPartitions[topic][pid] = localPartitionAssignment{
		Owned:       true,
		LeaderEpoch: newEpoch,
	}
	p.server.assignmentsMu.Unlock()

	return nil
}

func (p partitionFollowerService) reconfigureFollower(ctx context.Context, req pushAssignmentRequest) {
	ps := p.server.partitionManager.GetPartitionState(req.Topic, req.Partition)
	if ps == nil {
		slog.Warn("reconfigureFollower: partition not found", "topic", req.Topic, "partition", req.Partition)
		return
	}

	ps.mu.Lock()
	if followerFetchMatchesAssignment(ps, req.Leader, req.Epoch) {
		ps.mu.Unlock()
		return
	}
	existingCancel := ps.fetchCancel
	existingDone := ps.fetchDone
	ps.fetchCancel = nil
	ps.fetchDone = nil
	ps.fetchGeneration++
	generation := ps.fetchGeneration
	ps.isLeader = false
	ps.replicaState = nil
	ps.mu.Unlock()

	if existingCancel != nil {
		existingCancel()
		if existingDone != nil {
			<-existingDone
		}
	}

	leaderInfo, err := p.server.registry.GetInstanceInfo(ctx, req.Leader)
	if err != nil {
		slog.Warn("reconfigureFollower: resolve leader", "leader", req.Leader, "error", err)
		return
	}
	leaderAddr := routableReplicationAddress(req.Leader, leaderInfo.ReplicationAddress)

	ps.mu.Lock()
	if ps.fetchGeneration != generation || ps.isLeader {
		ps.mu.Unlock()
		return
	}
	ps.leaderID = req.Leader
	ps.fetchAssignmentEpoch = req.Epoch
	localOffset := ps.nextOffset
	localEpoch := ps.epoch
	fetchDone := make(chan struct{})
	fetchCtx, cancel := context.WithCancel(context.Background())
	ps.fetchCancel = cancel
	ps.fetchDone = fetchDone
	ps.mu.Unlock()

	slog.Info("reconfigureFollower: starting fetch",
		"topic", req.Topic, "partition", req.Partition,
		"leader", req.Leader, "leader_addr", leaderAddr,
		"local_offset", localOffset, "epoch", localEpoch)

	go func() {
		defer func() {
			close(fetchDone)
			ps.mu.Lock()
			if ps.fetchGeneration == generation {
				ps.fetchCancel = nil
				ps.fetchDone = nil
				ps.fetchAssignmentEpoch = 0
			}
			ps.mu.Unlock()
		}()
		p.server.followerFetcher.Run(fetchCtx, req.Topic, req.Partition, leaderAddr, localOffset, localEpoch, p.server.instanceID, p.server.partitionManager)
	}()
}
