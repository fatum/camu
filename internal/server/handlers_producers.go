package server

import "net/http"

type initProducerResponse struct {
	ProducerID uint64 `json:"producer_id"`
}

func (s *Server) handleInitProducer(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown.Load() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "server is shutting down")
		return
	}

	id, err := s.idempotencyManager.AllocateProducerID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to allocate producer ID: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, initProducerResponse{ProducerID: id})
}
