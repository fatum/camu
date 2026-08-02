package server

import (
	"context"
	"errors"

	"github.com/maksim/camu/internal/jobqueue"
	"github.com/maksim/camu/internal/jobs"
	"github.com/maksim/camu/internal/storage"
)

const partitionJobPrefix = "_coordination/partition_jobs/"

type PartitionJobType = jobs.Type
type PartitionJobState = jobs.State
type PartitionJobPhase = jobs.Phase
type PartitionJob = jobs.Record

const (
	PartitionJobTypeRetention    = jobs.TypeRetention
	PartitionJobTypeSegmentMerge = jobs.TypeSegmentMerge
)

const (
	PartitionJobStatePending = jobs.StatePending
	PartitionJobStateRunning = jobs.StateRunning
)

const (
	PartitionJobPhasePublishData = jobs.PhasePublishData
	PartitionJobPhasePublishMeta = jobs.PhasePublishMeta
	PartitionJobPhaseDeleteData  = jobs.PhaseDeleteData
	PartitionJobPhaseDeleteMeta  = jobs.PhaseDeleteMeta
)

// jobObjectAdapter wraps *storage.S3Client to satisfy jobqueue.ObjectStore,
// translating storage.ErrNotFound into jobqueue.ErrNotFound. Writes are
// stamped with application/json content-type.
type jobObjectAdapter struct{ client *storage.S3Client }

func (a jobObjectAdapter) Put(ctx context.Context, key string, data []byte) error {
	return a.client.Put(ctx, key, data, storage.PutOpts{ContentType: "application/json"})
}

func (a jobObjectAdapter) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := a.client.Get(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, errors.Join(err, jobqueue.ErrNotFound)
	}
	return data, err
}

func (a jobObjectAdapter) Delete(ctx context.Context, key string) error {
	err := a.client.Delete(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return errors.Join(err, jobqueue.ErrNotFound)
	}
	return err
}

func (a jobObjectAdapter) List(ctx context.Context, prefix string) ([]string, error) {
	return a.client.List(ctx, prefix)
}

func (s *Server) partitionJobStore() *jobs.Store {
	return jobs.NewStore(jobObjectAdapter{client: s.s3Client}, partitionJobPrefix)
}

func (s *Server) putPartitionJob(ctx context.Context, job PartitionJob) error {
	return s.partitionJobStore().Put(ctx, job)
}

func (s *Server) deletePartitionJob(ctx context.Context, topic string, partition int, jobID string) error {
	return s.partitionJobStore().Delete(ctx, topic, partition, jobID)
}

func (s *Server) listPartitionJobs(ctx context.Context, topic string, partition int) ([]PartitionJob, error) {
	return s.partitionJobStore().List(ctx, topic, partition)
}

func partitionJobID(kind PartitionJobType, key string) string {
	return jobs.ID(kind, key)
}
