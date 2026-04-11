package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

type createTopicRequest struct {
	Name                  string `json:"name"`
	Partitions            int    `json:"partitions"`
	Retention             string `json:"retention"`
	ReplicationFactor     int    `json:"replication_factor"`
	MinInsyncReplicas     int    `json:"min_insync_replicas"`
	UncleanLeaderElection bool   `json:"unclean_leader_election"`
	StorageMode           string `json:"storage_mode"`
}

type topicResponse struct {
	Name                  string `json:"name"`
	Partitions            int    `json:"partitions"`
	Retention             string `json:"retention"`
	ReplicationFactor     int    `json:"replication_factor"`
	MinInsyncReplicas     int    `json:"min_insync_replicas"`
	UncleanLeaderElection bool   `json:"unclean_leader_election"`
	StorageMode           string `json:"storage_mode,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func topicToResponse(tc meta.TopicConfig) topicResponse {
	return topicResponse{
		Name:                  tc.Name,
		Partitions:            tc.Partitions,
		Retention:             tc.Retention.String(),
		ReplicationFactor:     tc.ReplicationFactor,
		MinInsyncReplicas:     tc.MinInsyncReplicas,
		UncleanLeaderElection: tc.UncleanLeaderElection,
		StorageMode:           tc.StorageMode,
	}
}

func (s *Server) createTopic(ctx context.Context, req createTopicRequest) (meta.TopicConfig, error) {
	if req.Name == "" {
		return meta.TopicConfig{}, fmt.Errorf("name is required")
	}
	if _, err := s.getTopicDeletion(ctx, req.Name); err == nil {
		return meta.TopicConfig{}, fmt.Errorf("topic %q deletion in progress", req.Name)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return meta.TopicConfig{}, err
	}
	if req.Partitions < 1 {
		return meta.TopicConfig{}, fmt.Errorf("partitions must be at least 1")
	}

	retention := 168 * time.Hour // default 7 days
	if req.Retention != "" {
		var err error
		retention, err = time.ParseDuration(req.Retention)
		if err != nil {
			return meta.TopicConfig{}, fmt.Errorf("invalid retention duration")
		}
	}

	rf := req.ReplicationFactor
	if rf == 0 {
		rf = 1
	}
	minISR := req.MinInsyncReplicas
	if minISR == 0 {
		minISR = 1
	}

	instances, err := s.registry.ActiveInstances(ctx)
	if err != nil {
		return meta.TopicConfig{}, fmt.Errorf("failed to list instances")
	}
	if rf > len(instances) {
		return meta.TopicConfig{}, fmt.Errorf("replication_factor %d exceeds active instances %d", rf, len(instances))
	}
	if minISR > rf {
		return meta.TopicConfig{}, fmt.Errorf("min_insync_replicas cannot exceed replication_factor")
	}

	storageMode := req.StorageMode
	if storageMode == "" {
		storageMode = meta.StorageModeClassic
	}
	if storageMode != meta.StorageModeClassic && storageMode != meta.StorageModeDiskless {
		return meta.TopicConfig{}, fmt.Errorf("invalid storage_mode, must be classic or diskless")
	}

	tc := meta.TopicConfig{
		Name:                  req.Name,
		Partitions:            req.Partitions,
		Retention:             retention,
		CreatedAt:             time.Now(),
		ReplicationFactor:     rf,
		MinInsyncReplicas:     minISR,
		UncleanLeaderElection: req.UncleanLeaderElection,
		StorageMode:           storageMode,
	}

	if err := s.topicStore.Create(ctx, tc); err != nil {
		return meta.TopicConfig{}, err
	}

	if tc.StorageMode == meta.StorageModeDiskless {
		// Diskless topics skip partition manager init but still need local
		// ownership so produce/consume routing works immediately.
		s.assignmentsMu.Lock()
		if s.myPartitions[tc.Name] == nil {
			s.myPartitions[tc.Name] = make(map[int]localPartitionAssignment)
		}
		for pid := 0; pid < tc.Partitions; pid++ {
			s.myPartitions[tc.Name][pid] = localPartitionAssignment{
				Owned:       true,
				LeaderEpoch: 1,
			}
		}
		if s.disklessTopics == nil {
			s.disklessTopics = make(map[string]bool)
		}
		s.disklessTopics[tc.Name] = true
		s.assignmentsMu.Unlock()

		// Publish assignments so Kafka protocol and routing map work immediately.
		s.publishAssignmentsForTopics(ctx, []meta.TopicConfig{tc})
	} else {
		// Acquire leases first so we have epochs for partition init.
		s.AcquireLeasesForTopic(tc)

		epochs := s.getOwnedEpochs(tc.Name)
		if err := s.partitionManager.InitTopic(ctx, tc, epochs); err != nil {
			return meta.TopicConfig{}, fmt.Errorf("topic created but partition init failed: %w", err)
		}

		// In the simple single-broker case, do not wait for the background
		// coordination loop to promote partitions. Make them locally writable and
		// readable before returning from topic creation.
		if tc.ReplicationFactor == 1 {
			s.assignmentsMu.Lock()
			if s.myPartitions[tc.Name] == nil {
				s.myPartitions[tc.Name] = make(map[int]localPartitionAssignment)
			}
			s.assignmentsMu.Unlock()
			for pid := 0; pid < tc.Partitions; pid++ {
				epoch := epochs[pid]
				if epoch == 0 {
					epoch = 1
				}
				s.initPartitionAsLeader(ctx, tc.Name, pid, coordination.PartitionAssignment{
					Replicas:    []string{s.instanceID},
					Leader:      s.instanceID,
					LeaderEpoch: epoch,
				})
				s.assignmentsMu.Lock()
				s.myPartitions[tc.Name][pid] = localPartitionAssignment{
					Owned:       true,
					LeaderEpoch: epoch,
				}
				s.assignmentsMu.Unlock()
			}
		}
	}

	slog.Info("topic_created", "topic", tc.Name, "partitions", tc.Partitions)
	return tc, nil
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req createTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	tc, err := s.createTopic(r.Context(), req)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "already exists"):
			writeError(w, http.StatusConflict, err.Error())
		case strings.Contains(err.Error(), "deletion in progress"):
			writeError(w, http.StatusConflict, err.Error())
		case strings.Contains(err.Error(), "required"),
			strings.Contains(err.Error(), "partitions must be at least 1"),
			strings.Contains(err.Error(), "invalid retention duration"),
			strings.Contains(err.Error(), "replication_factor"),
			strings.Contains(err.Error(), "min_insync_replicas"),
			strings.Contains(err.Error(), "storage_mode"):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			slog.Error("topic_create_failed", "topic", req.Name, "error", err)
			writeError(w, http.StatusInternalServerError, err.Error())
		}
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

func (s *Server) deleteTopic(ctx context.Context, name string) error {
	if _, err := s.getTopicDeletion(ctx, name); err == nil {
		if err := s.topicStore.Delete(ctx, name); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		s.dropTopicRuntime(name)
		return nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}

	tc, err := s.topicStore.Get(ctx, name)
	if err != nil {
		return err
	}

	if err := s.enqueueTopicDeletion(ctx, tc); err != nil {
		return err
	}
	slog.Info("topic_delete_enqueued", "topic", name)
	return nil
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	if err := s.deleteTopic(r.Context(), name); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		slog.Error("topic_delete_failed", "topic", name, "error", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
