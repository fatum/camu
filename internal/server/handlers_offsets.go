package server

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ---- Request/Response types ----

type commitOffsetsRequest struct {
	Topics map[string]map[string]uint64 `json:"topics"`
}

type getOffsetsResponse struct {
	Topics map[string]map[string]uint64 `json:"topics"`
}

type commitConsumerOffsetsRequest struct {
	Offsets map[string]uint64 `json:"offsets"`
}

type getConsumerOffsetsResponse struct {
	Offsets map[string]uint64 `json:"offsets"`
}

// ---- Handlers ----

func (s *Server) handleCommitOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group_id")

	var req commitOffsetsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := s.offsetStore.CommitGroupTopics(r.Context(), groupID, parseTopicOffsets(req.Topics)); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit offsets: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group_id")

	offsets, err := s.offsetStore.GetGroupTopics(r.Context(), groupID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get offsets: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, getOffsetsResponse{Topics: formatTopicOffsets(offsets)})
}

func (s *Server) handleCommitConsumerOffsets(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	consumerID := r.PathValue("consumer_id")

	var req commitConsumerOffsetsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	offsets := parseStringOffsets(req.Offsets)

	if err := s.offsetStore.CommitConsumer(r.Context(), consumerID, topicName, offsets); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit offsets: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetConsumerOffsets(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	consumerID := r.PathValue("consumer_id")

	offsets, err := s.offsetStore.GetConsumer(r.Context(), consumerID, topicName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get offsets: "+err.Error())
		return
	}

	strOffsets := make(map[string]uint64, len(offsets))
	for k, v := range offsets {
		strOffsets[fmt.Sprintf("%d", k)] = v
	}

	writeJSON(w, http.StatusOK, getConsumerOffsetsResponse{Offsets: strOffsets})
}

// parseStringOffsets converts map[string]uint64 to map[int]uint64.
func parseStringOffsets(in map[string]uint64) map[int]uint64 {
	out := make(map[int]uint64, len(in))
	for k, v := range in {
		var pid int
		if _, err := fmt.Sscanf(k, "%d", &pid); err == nil {
			out[pid] = v
		}
	}
	return out
}

func parseTopicOffsets(in map[string]map[string]uint64) map[string]map[int]uint64 {
	out := make(map[string]map[int]uint64, len(in))
	for topic, offsets := range in {
		out[topic] = parseStringOffsets(offsets)
	}
	return out
}

func formatTopicOffsets(in map[string]map[int]uint64) map[string]map[string]uint64 {
	out := make(map[string]map[string]uint64, len(in))
	for topic, offsets := range in {
		strOffsets := make(map[string]uint64, len(offsets))
		for k, v := range offsets {
			strOffsets[fmt.Sprintf("%d", k)] = v
		}
		out[topic] = strOffsets
	}
	return out
}
