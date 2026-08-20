package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/storage"
)

type reportFailureRequest struct {
	Topic        string `json:"topic"`
	Partition    int    `json:"partition"`
	FailedLeader string `json:"failed_leader"`
}

type reportISRRequest struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	ISR       []string `json:"isr"`
	Leader    string   `json:"leader"`
}

type reportHWRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	HW        uint64 `json:"hw"`
}

func (s *Server) handleReportFailure(w http.ResponseWriter, r *http.Request) {
	cs := s.controllerState.Load()
	if cs == nil {
		writeError(w, http.StatusServiceUnavailable, "not the controller")
		return
	}

	var req reportFailureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	ctx := r.Context()

	// Read the partition's authoritative ISR from the ISR store. The
	// controller's in-memory ISR is only a reconciliation-time snapshot that is
	// never refreshed during operation (report-isr/report-hw have no production
	// callers), so it cannot be trusted to pick a leader that still holds the
	// committed prefix. Electing a replica that has fallen out of the ISR would
	// let it set an epoch boundary at its (shorter) log end and truncate
	// committed data held by remaining ISR members.
	isrState, isrErr := s.isrStore.Read(ctx, req.Topic, req.Partition)
	if isrErr != nil && !errors.Is(isrErr, storage.ErrNotFound) {
		writeError(w, http.StatusServiceUnavailable, "read ISR")
		return
	}

	active, err := s.registry.ActiveInstances(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "list active instances")
		return
	}
	activeSet := make(map[string]struct{}, len(active))
	for _, id := range active {
		activeSet[id] = struct{}{}
	}

	candidates := activeEligible(isrState.ISR, req.FailedLeader, activeSet)

	// Unclean leader election fallback: with no eligible ISR member, allow any
	// active replica that is not the failed leader.
	if len(candidates) == 0 {
		topicCfg, terr := s.topicStore.Get(ctx, req.Topic)
		if terr == nil && topicCfg.UncleanLeaderElection {
			assigned, aerr := s.assignmentStore.Read(ctx, req.Topic)
			if aerr == nil {
				if pa, ok := assigned.Partitions[req.Partition]; ok {
					candidates = activeEligible(pa.Replicas, req.FailedLeader, activeSet)
				}
			}
		}
	}
	if len(candidates) == 0 {
		writeError(w, http.StatusConflict, "no eligible ISR leader")
		return
	}
	sort.Strings(candidates)
	newLeader := candidates[0]

	// Bump the epoch and record the transition in controller state.
	meta := cs.GetPartition(req.Topic, req.Partition)
	if meta == nil {
		assigned, aerr := s.assignmentStore.Read(ctx, req.Topic)
		if aerr != nil {
			writeError(w, http.StatusServiceUnavailable, "read assignments")
			return
		}
		pa, ok := assigned.Partitions[req.Partition]
		if !ok {
			writeError(w, http.StatusConflict, "partition not assigned")
			return
		}
		meta = &coordination.PartitionMeta{
			Leader:   pa.Leader,
			Epoch:    pa.LeaderEpoch,
			Replicas: append([]string(nil), pa.Replicas...),
		}
		if isrErr == nil {
			meta.ISR = append([]string(nil), isrState.ISR...)
		} else {
			meta.ISR = append([]string(nil), pa.Replicas...)
		}
	}
	meta.Epoch++
	meta.Leader = newLeader
	meta.ISR = []string{newLeader}
	meta.EpochHistory = append(meta.EpochHistory, coordination.EpochEntry{
		Epoch:       meta.Epoch,
		StartOffset: meta.HW,
	})
	cs.SetPartition(req.Topic, req.Partition, meta)

	go s.pushAssignmentToNodes(req.Topic, req.Partition)
	slog.Info("controller: elected new leader",
		"topic", req.Topic, "partition", req.Partition,
		"failed_leader", req.FailedLeader,
		"new_leader", newLeader, "epoch", meta.Epoch)

	writeJSON(w, http.StatusOK, map[string]any{
		"new_leader": newLeader,
		"epoch":      meta.Epoch,
	})
}

// activeEligible filters candidates to active instances, excluding the failed
// leader. Selection order is preserved so callers can sort for determinism.
func activeEligible(candidates []string, failedLeader string, activeSet map[string]struct{}) []string {
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if c == failedLeader {
			continue
		}
		if _, ok := activeSet[c]; ok {
			out = append(out, c)
		}
	}
	return out
}

func (s *Server) handleReportISR(w http.ResponseWriter, r *http.Request) {
	cs := s.controllerState.Load()
	if cs == nil {
		writeError(w, http.StatusServiceUnavailable, "not the controller")
		return
	}

	var req reportISRRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	cs.UpdateISR(req.Topic, req.Partition, req.ISR)

	slog.Info("controller: ISR updated",
		"topic", req.Topic, "partition", req.Partition,
		"isr", req.ISR, "leader", req.Leader)

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleReportHW(w http.ResponseWriter, r *http.Request) {
	cs := s.controllerState.Load()
	if cs == nil {
		writeError(w, http.StatusServiceUnavailable, "not the controller")
		return
	}

	var req reportHWRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	cs.UpdateHW(req.Topic, req.Partition, req.HW)

	slog.Info("controller: HW updated",
		"topic", req.Topic, "partition", req.Partition,
		"hw", req.HW)

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetAssignments(w http.ResponseWriter, r *http.Request) {
	cs := s.controllerState.Load()
	if cs == nil {
		writeError(w, http.StatusServiceUnavailable, "not the controller")
		return
	}

	all := cs.AllPartitions()
	writeJSON(w, http.StatusOK, all)
}
