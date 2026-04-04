package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// reportFailureToController sends a leader-failure report to the controller so
// it can orchestrate a new leader election.  Returns an error if the controller
// is unreachable or returns a non-200 response (the caller falls back to
// self-election in that case).
func (s *Server) reportFailureToController(topic string, pid int) error {
	controllerAddr := s.getControllerAddr()
	if controllerAddr == "" {
		return fmt.Errorf("no controller available")
	}

	// Capture the failed leader ID from local partition state.
	ps := s.partitionManager.GetPartitionState(topic, pid)
	var failedLeader string
	if ps != nil {
		ps.mu.RLock()
		failedLeader = ps.leaderID
		ps.mu.RUnlock()
	}

	body, _ := json.Marshal(map[string]any{
		"topic":         topic,
		"partition":     pid,
		"failed_leader": failedLeader,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+controllerAddr+"/v1/internal/report-failure",
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build report-failure request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.internalClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST report-failure: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("report-failure returned %d", resp.StatusCode)
	}
	return nil
}

// getControllerAddr resolves the internal address of the current controller.
// Returns "" if this instance is the controller (caller handles locally) or if
// the controller cannot be determined.
func (s *Server) getControllerAddr() string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lease, err := s.leaderElection.GetLeader(ctx)
	if err != nil || lease.InstanceID == "" {
		return ""
	}
	if lease.InstanceID == s.instanceID {
		return "" // we are the controller, handle locally
	}
	info, err := s.registry.GetInstanceInfo(ctx, lease.InstanceID)
	if err != nil {
		return ""
	}
	return info.InternalAddress
}
