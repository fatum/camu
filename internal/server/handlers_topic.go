package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

type createTopicRequest struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Retention  string `json:"retention"`
}

type topicResponse struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Retention  string `json:"retention"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func topicToResponse(tc meta.TopicConfig) topicResponse {
	return topicResponse{
		Name:       tc.Name,
		Partitions: tc.Partitions,
		Retention:  tc.Retention.String(),
	}
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req createTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Partitions < 1 {
		writeError(w, http.StatusBadRequest, "partitions must be at least 1")
		return
	}

	retention := 168 * time.Hour // default 7 days
	if req.Retention != "" {
		var err error
		retention, err = time.ParseDuration(req.Retention)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid retention duration")
			return
		}
	}

	tc := meta.TopicConfig{
		Name:       req.Name,
		Partitions: req.Partitions,
		Retention:  retention,
		CreatedAt:  time.Now(),
	}

	if err := s.topicStore.Create(r.Context(), tc); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, topicToResponse(tc))
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics, err := s.topicStore.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := make([]topicResponse, len(topics))
	for i, tc := range topics {
		resp[i] = topicToResponse(tc)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	tc, err := s.topicStore.Get(r.Context(), name)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, topicToResponse(tc))
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")

	// Check if topic exists first.
	_, err := s.topicStore.Get(r.Context(), name)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := s.topicStore.Delete(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
