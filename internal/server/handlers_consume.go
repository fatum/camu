package server

import (
	"encoding/base64"
	"net/http"
	"strconv"
)

type consumeResponse struct {
	Messages   []consumedMessage `json:"messages"`
	NextOffset uint64            `json:"next_offset"`
}

type consumedMessage struct {
	Offset    uint64            `json:"offset"`
	Timestamp int64             `json:"timestamp"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

func (s *Server) handleConsumeLowLevel(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")

	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid partition ID")
		return
	}

	// Parse query params.
	var startOffset uint64
	if v := r.URL.Query().Get("offset"); v != "" {
		startOffset, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid offset")
			return
		}
	}

	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 1 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if limit > 1000 {
			limit = 1000
		}
	}

	// Get the partition index.
	index := s.partitionManager.GetIndex(topicName, partitionID)
	if index == nil {
		writeError(w, http.StatusNotFound, "partition not found")
		return
	}

	// Get the unflushed buffer.
	buffer := s.partitionManager.GetBuffer(topicName, partitionID)

	// Call fetcher.
	msgs, nextOffset, err := s.fetcher.Fetch(r.Context(), index, buffer, topicName, partitionID, startOffset, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
		return
	}

	// Build response.
	resp := consumeResponse{
		Messages:   make([]consumedMessage, 0, len(msgs)),
		NextOffset: nextOffset,
	}
	for _, m := range msgs {
		cm := consumedMessage{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       string(m.Key),
			Value:     tryString(m.Value),
			Headers:   m.Headers,
		}
		resp.Messages = append(resp.Messages, cm)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleConsumeHighLevel(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "consumer groups not yet implemented")
}

// tryString returns the string representation of b if it is valid UTF-8,
// otherwise returns a base64-encoded version.
func tryString(b []byte) string {
	s := string(b)
	// Fast path: most values are valid UTF-8 strings.
	for _, r := range s {
		if r == '\uFFFD' {
			return base64.StdEncoding.EncodeToString(b)
		}
	}
	return s
}
