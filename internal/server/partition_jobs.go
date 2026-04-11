package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/maksim/camu/internal/storage"
)

const partitionJobPrefix = "_coordination/partition_jobs/"

type PartitionJobType string

const (
	PartitionJobTypeRetention    PartitionJobType = "retention"
	PartitionJobTypeSegmentMerge PartitionJobType = "segment_merge"
)

type PartitionJobState string

const (
	PartitionJobStatePending PartitionJobState = "pending"
	PartitionJobStateRunning PartitionJobState = "running"
)

type PartitionJobPhase string

const (
	PartitionJobPhasePublishData PartitionJobPhase = "publish_data"
	PartitionJobPhasePublishMeta PartitionJobPhase = "publish_metadata"
	PartitionJobPhaseDeleteData  PartitionJobPhase = "delete_data"
	PartitionJobPhaseDeleteMeta  PartitionJobPhase = "delete_metadata"
)

type PartitionJob struct {
	ID            string            `json:"id"`
	Topic         string            `json:"topic"`
	Partition     int               `json:"partition"`
	Type          PartitionJobType  `json:"type"`
	ExpectedOwner string            `json:"expected_owner"`
	ExpectedEpoch uint64            `json:"expected_epoch"`
	State         PartitionJobState `json:"state"`
	Phase         PartitionJobPhase `json:"phase"`
	Payload       json.RawMessage   `json:"payload"`
	StartedAt     time.Time         `json:"started_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

func partitionJobKey(topic string, partition int, jobID string) string {
	return fmt.Sprintf("%s%s/%d/%s.json", partitionJobPrefix, topic, partition, url.PathEscape(jobID))
}

func (s *Server) putPartitionJob(ctx context.Context, job PartitionJob) error {
	job.UpdatedAt = time.Now()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal partition job %q: %w", job.ID, err)
	}
	if err := s.s3Client.Put(ctx, partitionJobKey(job.Topic, job.Partition, job.ID), data, storage.PutOpts{
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("put partition job %q: %w", job.ID, err)
	}
	return nil
}

func (s *Server) deletePartitionJob(ctx context.Context, topic string, partition int, jobID string) error {
	if err := s.s3Client.Delete(ctx, partitionJobKey(topic, partition, jobID)); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return nil
}

func (s *Server) listPartitionJobs(ctx context.Context, topic string, partition int) ([]PartitionJob, error) {
	keys, err := s.s3Client.List(ctx, fmt.Sprintf("%s%s/%d/", partitionJobPrefix, topic, partition))
	if err != nil {
		return nil, err
	}
	jobs := make([]PartitionJob, 0, len(keys))
	for _, key := range keys {
		data, err := s.s3Client.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, err
		}
		var job PartitionJob
		if err := json.Unmarshal(data, &job); err != nil {
			return nil, fmt.Errorf("unmarshal partition job %q: %w", key, err)
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].UpdatedAt.Equal(jobs[j].UpdatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].UpdatedAt.Before(jobs[j].UpdatedAt)
	})
	return jobs, nil
}

func partitionJobID(kind PartitionJobType, key string) string {
	return string(kind) + "/" + strings.TrimSuffix(url.PathEscape(key), ".json")
}
