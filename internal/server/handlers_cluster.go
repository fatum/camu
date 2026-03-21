package server

import (
	"errors"
	"fmt"
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

type routingPartitionInfo struct {
	InstanceID string `json:"instance_id"`
	Address    string `json:"address"`
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
	tc, err := s.topicStore.Get(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	routing := s.getRoutingMap(topicName)

	// For any partitions without a lease, map them to this instance (fallback).
	for pid := 0; pid < tc.Partitions; pid++ {
		key := fmt.Sprintf("%d", pid)
		if _, exists := routing.Partitions[key]; !exists {
			routing.Partitions[key] = routingPartitionInfo{
				InstanceID: s.instanceID,
				Address:    "http://" + s.Address(),
			}
		}
	}

	w.Header().Set("Cache-Control", "max-age=10")
	writeJSON(w, http.StatusOK, routing)
}
