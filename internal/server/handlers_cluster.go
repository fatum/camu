package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

type clusterStatusResponse struct {
	Ready                 bool           `json:"ready"`
	Status                string         `json:"status"`
	ActiveInstances       int            `json:"active_instances"`
	ReadyInstances        int            `json:"ready_instances"`
	AssignedPartitions    int            `json:"assigned_partitions"`
	InitializedPartitions int            `json:"initialized_partitions"`
	ExpectedPartitions    int            `json:"expected_partitions"`
	Reasons               []string       `json:"reasons,omitempty"`
	Instances             []instanceInfo `json:"instances"`
}

type instanceInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type routingResponse struct {
	Partitions map[string]routingPartitionInfo `json:"partitions"`
}

type routingReplicaInfo struct {
	InstanceID string `json:"instance_id"`
	Address    string `json:"address"`
}

type routingPartitionInfo struct {
	InstanceID string               `json:"instance_id"`
	Address    string               `json:"address"`
	Replicas   []routingReplicaInfo `json:"replicas,omitempty"`
}

type readyResponse struct {
	Ready  bool   `json:"ready"`
	Status string `json:"status"`
}

type localPartitionReadiness struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Epoch     uint64 `json:"epoch"`
}

type localReadinessResponse struct {
	Ready      bool                      `json:"ready"`
	Partitions []localPartitionReadiness `json:"partitions"`
}

func evaluateClusterReadiness(active, ready, assigned, initialized, expected int, unavailable bool, reasons []string) clusterStatusResponse {
	resp := clusterStatusResponse{ReadyInstances: ready, ActiveInstances: active, AssignedPartitions: assigned, InitializedPartitions: initialized, ExpectedPartitions: expected, Reasons: append([]string(nil), reasons...)}
	resp.Ready = !unavailable && len(resp.Reasons) == 0 && active == ready && assigned == expected && initialized == expected
	switch {
	case resp.Ready:
		resp.Status = "ready"
	case unavailable || active == 0:
		resp.Status = "unavailable"
	case assigned != expected || initialized != expected:
		resp.Status = "rebalancing"
	default:
		resp.Status = "degraded"
	}
	if resp.Status == "rebalancing" && len(resp.Reasons) == 0 {
		resp.Reasons = []string{"cluster assignments or partition initialization are converging"}
	}
	return resp
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown.Load() {
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{
			Ready:  false,
			Status: "shutting_down",
		})
		return
	}
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{
			Ready:  false,
			Status: "initializing",
		})
		return
	}
	writeJSON(w, http.StatusOK, readyResponse{
		Ready:  true,
		Status: "ready",
	})
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	resp := s.clusterStatus(r.Context())
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleClusterReady(w http.ResponseWriter, r *http.Request) {
	resp := s.clusterStatus(r.Context())
	if !resp.Ready {
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleInternalReadiness is intentionally separate from the public status
// endpoint: it reports only local state and never performs cluster fan-out.
func (s *Server) handleInternalReadiness(w http.ResponseWriter, _ *http.Request) {
	resp := localReadinessResponse{Ready: s.ready.Load() && !s.shuttingDown.Load()}
	if s.partitionManager != nil {
		s.partitionManager.mu.RLock()
		for topic, parts := range s.partitionManager.partitions {
			for pid, ps := range parts {
				ps.mu.RLock()
				epoch := ps.epoch
				ps.mu.RUnlock()
				resp.Partitions = append(resp.Partitions, localPartitionReadiness{Topic: topic, Partition: pid, Epoch: epoch})
			}
		}
		s.partitionManager.mu.RUnlock()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) clusterStatus(parent context.Context) clusterStatusResponse {
	resp := clusterStatusResponse{Instances: []instanceInfo{}}
	if s.isQueryMode() {
		resp.Status = "unavailable"
		resp.Reasons = []string{"cluster readiness is not supported in query mode"}
		return resp
	}
	if s.registry == nil || s.assignmentStore == nil || s.topicStore == nil {
		resp.Status = "unavailable"
		resp.Reasons = []string{"cluster coordination is unavailable"}
		return resp
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	infos, err := s.registry.ActiveInstanceInfos(ctx)
	if err != nil {
		resp.Status = "unavailable"
		resp.Reasons = []string{"cannot read active instances: " + err.Error()}
		return resp
	}
	resp.ActiveInstances = len(infos)
	for _, info := range infos {
		resp.Instances = append(resp.Instances, instanceInfo{ID: info.InstanceID, Address: info.Address})
	}
	if len(infos) == 0 {
		resp.Status = "unavailable"
		resp.Reasons = []string{"no active stream instances"}
		return resp
	}

	local := make(map[string]map[string]uint64, len(infos))
	for _, info := range infos {
		addr := info.InternalAddress
		if addr == "" {
			addr = info.Address
		}
		if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
			addr = strings.TrimPrefix(strings.TrimPrefix(addr, "http://"), "https://")
		}
		var lr localReadinessResponse
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/v1/internal/readiness", nil)
		if reqErr != nil {
			resp.Reasons = append(resp.Reasons, fmt.Sprintf("instance %s readiness request: %v", info.InstanceID, reqErr))
			continue
		}
		client := s.internalClient
		if client == nil {
			client = &http.Client{Timeout: 1500 * time.Millisecond}
		}
		httpResp, callErr := client.Do(req)
		if callErr != nil {
			resp.Reasons = append(resp.Reasons, fmt.Sprintf("instance %s unreachable", info.InstanceID))
			continue
		}
		decodeErr := json.NewDecoder(httpResp.Body).Decode(&lr)
		httpResp.Body.Close()
		if httpResp.StatusCode != http.StatusOK || decodeErr != nil {
			resp.Reasons = append(resp.Reasons, fmt.Sprintf("instance %s readiness unavailable", info.InstanceID))
			continue
		}
		if lr.Ready {
			resp.ReadyInstances++
		} else {
			resp.Reasons = append(resp.Reasons, fmt.Sprintf("instance %s is not locally ready", info.InstanceID))
		}
		for _, p := range lr.Partitions {
			if local[info.InstanceID] == nil {
				local[info.InstanceID] = make(map[string]uint64)
			}
			local[info.InstanceID][partitionKey(p.Topic, p.Partition)] = p.Epoch
		}
	}

	topics, err := s.topicStore.List(ctx)
	if err != nil {
		resp.Status = "unavailable"
		resp.Reasons = append(resp.Reasons, "cannot read topics: "+err.Error())
		return resp
	}
	for _, tc := range topics {
		if tc.StorageMode == meta.StorageModeDiskless {
			// Diskless topics are served by any node's engine plus the shared
			// metastore; they have no replica assignments to initialize, so they
			// neither contribute to nor block classic cluster readiness.
			continue
		}
		resp.ExpectedPartitions += tc.Partitions
		assignments, readErr := s.assignmentStore.Read(ctx, tc.Name)
		if readErr != nil {
			if errors.Is(readErr, storage.ErrNotFound) {
				resp.Reasons = append(resp.Reasons, fmt.Sprintf("topic %s assignments are not initialized", tc.Name))
			} else {
				resp.Reasons = append(resp.Reasons, fmt.Sprintf("topic %s assignments unavailable", tc.Name))
			}
			continue
		}
		for pid, assignment := range assignments.Partitions {
			resp.AssignedPartitions++
			initialized := len(assignment.Replicas) > 0
			for _, replica := range assignment.Replicas {
				epoch, ok := local[replica][partitionKey(tc.Name, pid)]
				if !ok || epoch != assignment.LeaderEpoch {
					initialized = false
				}
			}
			if initialized {
				resp.InitializedPartitions++
			} else {
				resp.Reasons = append(resp.Reasons, fmt.Sprintf("topic %s partition %d is not initialized on all replicas", tc.Name, pid))
			}
		}
	}
	unavailable := false
	for _, reason := range resp.Reasons {
		if strings.Contains(reason, "unavailable") || strings.Contains(reason, "unreachable") || strings.Contains(reason, "cannot read") {
			unavailable = true
		}
	}
	evaluated := evaluateClusterReadiness(resp.ActiveInstances, resp.ReadyInstances, resp.AssignedPartitions, resp.InitializedPartitions, resp.ExpectedPartitions, unavailable, resp.Reasons)
	evaluated.Instances = resp.Instances
	return evaluated
}

func partitionKey(topic string, pid int) string { return fmt.Sprintf("%s/%d", topic, pid) }

func (s *Server) handleRouting(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")

	// Validate topic exists.
	_, err := s.topicStore.Get(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	routing := s.getRoutingMap(topicName)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, routing)
}
