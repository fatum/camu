package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/log"
)

// ---- Request/Response types ----

type joinGroupRequest struct {
	Topic      string `json:"topic"`
	ConsumerID string `json:"consumer_id"`
}

type joinGroupResponse struct {
	Partitions []int `json:"partitions"`
}

type heartbeatRequest struct {
	ConsumerID string `json:"consumer_id"`
}

type commitOffsetsRequest struct {
	Offsets map[string]uint64 `json:"offsets"`
}

type getOffsetsResponse struct {
	Offsets map[string]uint64 `json:"offsets"`
}

type leaveGroupRequest struct {
	ConsumerID string `json:"consumer_id"`
}

type groupConsumeResponse struct {
	Messages []groupConsumedMessage `json:"messages"`
}

type groupConsumedMessage struct {
	Partition int               `json:"partition"`
	Offset    uint64            `json:"offset"`
	Timestamp int64             `json:"timestamp"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// ---- Handlers ----

func (s *Server) handleJoinGroup(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group_id")

	var req joinGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Topic == "" || req.ConsumerID == "" {
		writeError(w, http.StatusBadRequest, "topic and consumer_id are required")
		return
	}

	// Look up the topic to get partition count.
	tc, err := s.topicStore.Get(r.Context(), req.Topic)
	if err != nil {
		writeError(w, http.StatusNotFound, "topic not found")
		return
	}

	partitions, err := s.groupCoordinator.Join(groupID, req.Topic, req.ConsumerID, tc.Partitions)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, joinGroupResponse{Partitions: partitions})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group_id")

	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ConsumerID == "" {
		writeError(w, http.StatusBadRequest, "consumer_id is required")
		return
	}

	if err := s.groupCoordinator.Heartbeat(groupID, req.ConsumerID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCommitOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group_id")

	var req commitOffsetsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	offsets := parseStringOffsets(req.Offsets)

	// We need the topic for the group. Look up from coordinator state.
	// For simplicity, store with empty topic — the key already includes groupID.
	if err := s.offsetStore.CommitGroup(r.Context(), groupID, "", offsets); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit offsets: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetOffsets(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group_id")

	offsets, err := s.offsetStore.GetGroup(r.Context(), groupID, "")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get offsets: "+err.Error())
		return
	}

	strOffsets := make(map[string]uint64, len(offsets))
	for k, v := range offsets {
		strOffsets[fmt.Sprintf("%d", k)] = v
	}

	writeJSON(w, http.StatusOK, getOffsetsResponse{Offsets: strOffsets})
}

func (s *Server) handleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group_id")

	var req leaveGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ConsumerID == "" {
		writeError(w, http.StatusBadRequest, "consumer_id is required")
		return
	}

	s.groupCoordinator.Leave(groupID, req.ConsumerID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleConsumeHighLevelImpl(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	groupID := r.URL.Query().Get("group")
	consumerID := r.URL.Query().Get("consumer_id")

	if groupID == "" || consumerID == "" {
		writeError(w, http.StatusBadRequest, "group and consumer_id query params are required")
		return
	}

	partitions := s.groupCoordinator.GetAssignment(groupID, consumerID)
	if len(partitions) == 0 {
		writeError(w, http.StatusNotFound, "consumer not a member of group or no partitions assigned")
		return
	}

	// Parse optional limit.
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err == nil && parsed > 0 {
			limit = parsed
		}
		if limit > 1000 {
			limit = 1000
		}
	}

	// Get offsets for this group to determine start positions.
	groupOffsets, _ := s.offsetStore.GetGroup(r.Context(), groupID, "")

	var allMsgs []groupConsumedMessage

	for _, pid := range partitions {
		startOffset := groupOffsets[pid] // 0 if not committed

		index := s.partitionManager.GetIndex(topicName, pid)
		buffer := s.partitionManager.GetBuffer(topicName, pid)

		msgs, _, err := s.fetcher.Fetch(r.Context(), index, buffer, topicName, pid, startOffset, limit)
		if err != nil {
			continue
		}

		for _, m := range msgs {
			allMsgs = append(allMsgs, groupConsumedMessage{
				Partition: pid,
				Offset:    m.Offset,
				Timestamp: m.Timestamp,
				Key:       string(m.Key),
				Value:     tryString(m.Value),
				Headers:   m.Headers,
			})
		}
	}

	writeJSON(w, http.StatusOK, groupConsumeResponse{Messages: allMsgs})
}

func (s *Server) handleStreamWithGroup(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	groupID := r.URL.Query().Get("group")
	consumerID := r.URL.Query().Get("consumer_id")

	if groupID == "" || consumerID == "" {
		writeError(w, http.StatusBadRequest, "group and consumer_id query params are required")
		return
	}

	partitions := s.groupCoordinator.GetAssignment(groupID, consumerID)
	if len(partitions) == 0 {
		writeError(w, http.StatusNotFound, "consumer not a member of group or no partitions assigned")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	// Get group offsets for start positions.
	groupOffsets, _ := s.offsetStore.GetGroup(r.Context(), groupID, "")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Stream from the first assigned partition (simplified).
	// A full implementation would multiplex all partitions.
	pid := partitions[0]
	startOffset := groupOffsets[pid]

	index := s.partitionManager.GetIndex(topicName, pid)
	bufferFn := func() []log.Message {
		return s.partitionManager.GetBuffer(topicName, pid)
	}

	_ = flusher
	consumer.StreamSSE(r.Context(), w, s.fetcher, s.s3Client, s.partitionManager.GetDiskCache(),
		index, bufferFn, topicName, pid, startOffset)
}

func (s *Server) handleCommitConsumerOffsets(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	consumerID := r.PathValue("consumer_id")

	var req commitOffsetsRequest
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

	writeJSON(w, http.StatusOK, getOffsetsResponse{Offsets: strOffsets})
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
