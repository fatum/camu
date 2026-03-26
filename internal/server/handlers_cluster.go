package server

import (
	"errors"
	"net/http"

	"github.com/maksim/camu/internal/storage"
)

type clusterStatusResponse struct {
	Instances []instanceInfo `json:"instances"`
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
	resp := clusterStatusResponse{
		Instances: []instanceInfo{
			{
				ID:      s.instanceID,
				Address: s.Address(),
			},
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

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
