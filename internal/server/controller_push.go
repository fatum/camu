package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/maksim/camu/internal/coordination"
)

type pushAssignmentRequest struct {
	Topic        string                    `json:"topic"`
	Partition    int                       `json:"partition"`
	Leader       string                    `json:"leader"`
	Epoch        uint64                    `json:"epoch"`
	Replicas     []string                  `json:"replicas"`
	ISR          []string                  `json:"isr"`
	HW           uint64                    `json:"hw"`
	EpochHistory []coordination.EpochEntry `json:"epoch_history,omitempty"`
}

// AssignmentPusher sends assignment changes to nodes over h2c.
type AssignmentPusher struct {
	client *http.Client
}

func NewAssignmentPusher(client *http.Client) *AssignmentPusher {
	return &AssignmentPusher{client: client}
}

// Push sends a single assignment change to a node. Returns error on non-200.
func (p *AssignmentPusher) Push(ctx context.Context, nodeAddr string, req pushAssignmentRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal push request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		"http://"+nodeAddr+"/v1/internal/push-assignments",
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build push request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("push to %s: %w", nodeAddr, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("push to %s: status %d", nodeAddr, resp.StatusCode)
	}
	return nil
}

// handlePushAssignment receives an assignment push from the controller.
func (s *Server) handlePushAssignment(w http.ResponseWriter, r *http.Request) {
	var req pushAssignmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid push assignment request")
		return
	}
	slog.Info("received assignment push",
		"topic", req.Topic, "partition", req.Partition,
		"leader", req.Leader, "epoch", req.Epoch)

	if req.Leader == s.instanceID {
		if err := s.becomeLeader(r.Context(), req.Topic, req.Partition, req); err != nil {
			slog.Error("becomeLeader failed", "topic", req.Topic, "pid", req.Partition, "err", err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else if containsString(req.Replicas, s.instanceID) {
		// This node is a follower — reconfigure fetch loop to point at new leader.
		s.reconfigureFollower(r.Context(), req)
	}

	w.WriteHeader(http.StatusOK)
}

// pushAssignmentToNodes fans out an assignment change to all known nodes.
// Best-effort: errors are logged but do not block.
func (s *Server) pushAssignmentToNodes(topic string, pid int) {
	cs := s.controllerState.Load()
	if cs == nil || s.assignmentPusher == nil {
		return
	}
	meta := cs.GetPartition(topic, pid)
	if meta == nil {
		return
	}
	req := pushAssignmentRequest{
		Topic:        topic,
		Partition:    pid,
		Leader:       meta.Leader,
		Epoch:        meta.Epoch,
		Replicas:     meta.Replicas,
		ISR:          meta.ISR,
		HW:           meta.HW,
		EpochHistory: meta.EpochHistory,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	instanceIDs, err := s.registry.ActiveInstances(ctx)
	if err != nil {
		slog.Warn("pushAssignmentToNodes: list instances", "err", err)
		return
	}
	for _, id := range instanceIDs {
		if id == s.instanceID {
			continue
		}
		go func(instanceID string) {
			pushCtx, pushCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer pushCancel()
			info, err := s.registry.GetInstanceInfo(pushCtx, instanceID)
			if err != nil {
				slog.Warn("push assignment: get instance info", "instance", instanceID, "err", err)
				return
			}
			if info.InternalAddress == "" {
				slog.Warn("push assignment: no internal address", "instance", instanceID)
				return
			}
			if err := s.assignmentPusher.Push(pushCtx, info.InternalAddress, req); err != nil {
				slog.Warn("push assignment failed", "addr", info.InternalAddress, "topic", topic, "pid", pid, "err", err)
			}
		}(id)
	}
}
