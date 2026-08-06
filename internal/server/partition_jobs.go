package server

import (
	"context"
	"errors"
	"log/slog"

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

// hasActiveSegmentMergeJob reports whether a segment-merge job owned by the
// current leader identity is already in flight for this partition and still
// gates the next merge. A job in the delete_data phase has already published
// its merged ref (publish_meta is done) and only awaits source-data deletion;
// it must not stall the next merge, so it does not count as active. A merge job
// whose expected owner or epoch no longer matches the current leader can never
// run (CanRunOwnerJob requires an exact match), so it is stale: deleting it
// unblocks compaction. Without this, a single orphaned job (e.g. after a node
// restart moved leadership) permanently stalls every future merge for the
// partition, because merge discovery returns early while any merge job exists.
func (s *Server) hasActiveSegmentMergeJob(ctx context.Context, identity PartitionIdentity, jobs []PartitionJob) (bool, error) {
	for _, job := range jobs {
		if job.Type != PartitionJobTypeSegmentMerge {
			continue
		}
		if job.ExpectedOwner == identity.Leader && job.ExpectedEpoch == identity.LeaderEpoch {
			if job.Phase == PartitionJobPhaseDeleteData {
				// The merged ref is already authoritative; source deletion is
				// independent of the next merge, so do not block discovery.
				continue
			}
			return true, nil
		}
		slog.Warn("segment_merge_job_stale",
			"topic", job.Topic, "partition", job.Partition, "job", job.ID,
			"expected_owner", job.ExpectedOwner, "expected_epoch", job.ExpectedEpoch,
			"leader", identity.Leader, "epoch", identity.LeaderEpoch)
		if err := s.deletePartitionJob(ctx, job.Topic, job.Partition, job.ID); err != nil {
			return false, err
		}
	}
	return false, nil
}

func partitionJobID(kind PartitionJobType, key string) string {
	return jobs.ID(kind, key)
}
