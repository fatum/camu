package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
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

	newLeader, err := cs.ElectLeader(req.Topic, req.Partition, req.FailedLeader)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	meta := cs.GetPartition(req.Topic, req.Partition)
	epoch := meta.Epoch

	go s.pushAssignmentToNodes(req.Topic, req.Partition)
	slog.Info("controller: elected new leader",
		"topic", req.Topic, "partition", req.Partition,
		"failed_leader", req.FailedLeader,
		"new_leader", newLeader, "epoch", epoch)

	writeJSON(w, http.StatusOK, map[string]any{
		"new_leader": newLeader,
		"epoch":      epoch,
	})
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
