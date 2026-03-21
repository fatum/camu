package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

type produceMessageRequest struct {
	Key   string            `json:"key"`
	Value string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

type produceResponse struct {
	Offsets []offsetInfo `json:"offsets"`
}

type offsetInfo struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

func (s *Server) handleProduceHighLevel(w http.ResponseWriter, r *http.Request) {
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

	// Parse body: either a JSON array or single object.
	var msgs []produceMessageRequest
	decoder := json.NewDecoder(r.Body)
	// Try to detect if it's an array or object by peeking.
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Try array first, then single object.
	if err := json.Unmarshal(raw, &msgs); err != nil {
		var single produceMessageRequest
		if err2 := json.Unmarshal(raw, &single); err2 != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: expected array or object")
			return
		}
		msgs = []produceMessageRequest{single}
	}

	if len(msgs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one message is required")
		return
	}

	router := s.partitionManager.GetRouter(topicName)
	if router == nil {
		writeError(w, http.StatusInternalServerError, "topic not initialized")
		return
	}

	offsets := make([]offsetInfo, 0, len(msgs))
	for _, m := range msgs {
		var key []byte
		if m.Key != "" {
			key = []byte(m.Key)
		}
		partitionID := router.Route(key)

		msg := log.Message{
			Timestamp: time.Now().UnixNano(),
			Key:       key,
			Value:     []byte(m.Value),
			Headers:   m.Headers,
		}

		offset, err := s.partitionManager.Append(r.Context(), topicName, partitionID, msg)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "append failed: "+err.Error())
			return
		}

		offsets = append(offsets, offsetInfo{
			Partition: partitionID,
			Offset:    offset,
		})
	}

	writeJSON(w, http.StatusOK, produceResponse{Offsets: offsets})
}

func (s *Server) handleProduceLowLevel(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	partitionStr := r.PathValue("id")

	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid partition ID")
		return
	}

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

	if partitionID < 0 || partitionID >= tc.Partitions {
		writeError(w, http.StatusBadRequest, "partition ID out of range")
		return
	}

	// Parse body.
	var msgs []produceMessageRequest
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := json.Unmarshal(raw, &msgs); err != nil {
		var single produceMessageRequest
		if err2 := json.Unmarshal(raw, &single); err2 != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: expected array or object")
			return
		}
		msgs = []produceMessageRequest{single}
	}

	if len(msgs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one message is required")
		return
	}

	offsets := make([]offsetInfo, 0, len(msgs))
	for _, m := range msgs {
		var key []byte
		if m.Key != "" {
			key = []byte(m.Key)
		}
		msg := log.Message{
			Timestamp: time.Now().UnixNano(),
			Key:       key,
			Value:     []byte(m.Value),
			Headers:   m.Headers,
		}

		offset, err := s.partitionManager.Append(r.Context(), topicName, partitionID, msg)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "append failed: "+err.Error())
			return
		}

		offsets = append(offsets, offsetInfo{
			Partition: partitionID,
			Offset:    offset,
		})
	}

	writeJSON(w, http.StatusOK, produceResponse{Offsets: offsets})
}
