package server

import "net/http"

type clusterStatusResponse struct {
	Instances []instanceInfo `json:"instances"`
}

type instanceInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
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
