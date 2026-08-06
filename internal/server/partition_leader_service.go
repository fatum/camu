package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/meta"
)

type partitionLeaderService struct {
	server *Server
}

func (s *Server) partitionLeader() partitionLeaderService {
	return partitionLeaderService{server: s}
}

func (p partitionLeaderService) runMaintenance(ctx context.Context, topics []meta.TopicConfig, fileIdx *diskless.FileIndex) {
	type task struct {
		tc        meta.TopicConfig
		partition int
	}
	var tasks []task
	for _, tc := range topics {
		for partition := 0; partition < tc.Partitions; partition++ {
			tasks = append(tasks, task{tc: tc, partition: partition})
		}
	}

	maxConcurrency := p.server.cfg.Coordination.MaintenanceMaxConcurrencyValue()
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	runBoundedPartitionTasks(maxConcurrency, tasks, func(task task) {
		p.runPartitionJobDiscovery(ctx, task.tc, task.partition, fileIdx)
	})
}

func (p partitionLeaderService) runJobsForTopic(ctx context.Context, tc meta.TopicConfig, fileIdx *diskless.FileIndex) {
	for partition := 0; partition < tc.Partitions; partition++ {
		p.runPartitionJobDiscovery(ctx, tc, partition, fileIdx)
	}
}

func (p partitionLeaderService) runPartitionJobDiscovery(ctx context.Context, tc meta.TopicConfig, partition int, fileIdx *diskless.FileIndex) {
	identity, err := p.server.ResolvePartitionIdentity(ctx, tc.Name, partition)
	if err != nil {
		slog.Warn("partition_maintenance_identity_failed", "topic", tc.Name, "partition", partition, "error", err)
		return
	}
	if identity.Role != PartitionRoleLeader {
		p.server.stopParquetConsumer(tc.Name, partition)
		return
	}
	p.server.ensureParquetConsumer(tc, identity)

	if tc.StorageMode == meta.StorageModeDiskless {
		p.server.discoverDisklessRetentionJobs(ctx, tc, identity, fileIdx)
		p.server.rollDisklessMetadata(ctx, tc, identity)
	} else {
		p.server.discoverClassicRetentionJobs(ctx, tc, identity)
	}

	jobs, err := p.server.listPartitionJobs(ctx, tc.Name, partition)
	if err != nil {
		slog.Warn("partition_maintenance_list_jobs_failed", "topic", tc.Name, "partition", partition, "error", err)
		return
	}
	if tc.StorageMode == meta.StorageModeClassic {
		p.server.discoverClassicSegmentMergeJobs(ctx, tc, identity, jobs)
		jobs, err = p.server.listPartitionJobs(ctx, tc.Name, partition)
		if err != nil {
			slog.Warn("partition_maintenance_relist_jobs_failed", "topic", tc.Name, "partition", partition, "error", err)
			return
		}
		for _, job := range jobs {
			if err := p.runJob(ctx, job); err != nil {
				slog.Warn("partition_job_failed", "topic", job.Topic, "partition", job.Partition, "job", job.ID, "type", job.Type, "error", err)
			}
		}
		return
	}
	// Diskless: the dedicated compaction loop owns segment-merge discovery and
	// execution (it runs far more often than this pass); the maintenance pass
	// only drives retention here so a merge job is never run by two paths.
	for _, job := range jobs {
		if job.Type == PartitionJobTypeSegmentMerge {
			continue
		}
		if err := p.runJob(ctx, job); err != nil {
			slog.Warn("partition_job_failed", "topic", job.Topic, "partition", job.Partition, "job", job.ID, "type", job.Type, "error", err)
		}
	}
}

func runBoundedPartitionTasks[T any](maxConcurrency int, tasks []T, fn func(T)) {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		sem <- struct{}{}
		go func(task T) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(task)
		}(task)
	}
	wg.Wait()
}

func (p partitionLeaderService) runJob(ctx context.Context, job PartitionJob) error {
	switch job.Type {
	case PartitionJobTypeRetention:
		return p.server.runRetentionJob(ctx, job)
	case PartitionJobTypeSegmentMerge:
		return p.server.runSegmentMergeJob(ctx, job)
	default:
		// Fail loudly on unknown job types: a silent no-op turns a
		// wiring gap (e.g. a declared-but-not-dispatched job type) into
		// a job that vanishes from the queue every tick without effect.
		return fmt.Errorf("partition_job: unhandled type %q for %s/%d (job %s)", job.Type, job.Topic, job.Partition, job.ID)
	}
}
