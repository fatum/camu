package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/meta"
)

// startDisklessCompactionLoop runs a dedicated background loop that drives
// diskless small-segment compaction (merge discovery + execution) far more
// often than the maintenance pass. The maintenance pass fires every 10th
// heartbeat tick, which alone caps compaction at roughly one target-sized merge
// per pass per partition — an order of magnitude below sustained production, so
// compaction falls permanently behind. This loop owns diskless merge work on a
// short, configurable interval so merges pipeline as fast as the executor and
// object store allow.
func (s *Server) startDisklessCompactionLoop() {
	interval, err := s.cfg.Diskless.Compaction.IntervalDuration()
	if err != nil {
		slog.Warn("diskless_compaction_interval_invalid", "error", err, "interval", s.cfg.Diskless.Compaction.Interval, "falling_back", config.DefaultCompactionInterval)
		interval = config.DefaultCompactionInterval
	}
	s.leaseWg.Add(1)
	go func() {
		defer s.leaseWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.leaseStop:
				return
			case <-ticker.C:
				s.runDisklessCompactionTick(s.maintenanceCtx)
			}
		}
	}()
}

// runDisklessCompactionTick runs one compaction pass: for every diskless
// partition this node leads, discover eligible merge runs and execute any merge
// jobs in flight. A tick that finds a prior one still running (e.g. while
// catching up) is skipped, so the cadence degrades gracefully instead of
// stacking ticks. The maintenance pass is the only other writer of partition
// jobs, and it never touches diskless merge work, so a merge job is executed by
// exactly one path at a time.
func (s *Server) runDisklessCompactionTick(ctx context.Context) {
	if !s.disklessCompactionBusy.CompareAndSwap(false, true) {
		return
	}
	defer s.disklessCompactionBusy.Store(false)
	if s.disklessMeta == nil {
		return
	}
	topics, err := s.topicStore.List(ctx)
	if err != nil {
		slog.Warn("diskless_compaction_list_topics_failed", "error", err)
		return
	}
	type task struct {
		tc        meta.TopicConfig
		partition int
	}
	var tasks []task
	for _, tc := range topics {
		if tc.StorageMode != meta.StorageModeDiskless {
			continue
		}
		for partition := 0; partition < tc.Partitions; partition++ {
			tasks = append(tasks, task{tc: tc, partition: partition})
		}
	}
	maxConcurrency := s.cfg.Coordination.MaintenanceMaxConcurrencyValue()
	runBoundedPartitionTasks(maxConcurrency, tasks, func(t task) {
		s.runDisklessCompactionForPartition(ctx, t.tc, t.partition)
	})
}

// runDisklessCompactionForPartition discovers merge runs and executes merge
// jobs for one partition this node leads. Discovery partitions eligible refs
// into disjoint contiguous runs and enqueues one job per run; execution runs
// all in-flight merge jobs concurrently under disklessMergeExecSem. Disjoint
// jobs never touch the same refs, so each ReplaceSegmentRefs stays atomic and
// a per-partition run chain is not needed.
func (s *Server) runDisklessCompactionForPartition(ctx context.Context, tc meta.TopicConfig, partition int) {
	if !s.isOwnedPartition(tc.Name, partition) {
		return
	}
	identity, err := s.ResolvePartitionIdentity(ctx, tc.Name, partition)
	if err != nil {
		slog.Warn("diskless_compaction_identity_failed", "topic", tc.Name, "partition", partition, "error", err)
		return
	}
	if identity.Role != PartitionRoleLeader {
		return
	}
	jobs, err := s.listPartitionJobs(ctx, tc.Name, partition)
	if err != nil {
		slog.Warn("diskless_compaction_list_jobs_failed", "topic", tc.Name, "partition", partition, "error", err)
		return
	}
	s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, jobs)
	jobs, err = s.listPartitionJobs(ctx, tc.Name, partition)
	if err != nil {
		slog.Warn("diskless_compaction_relist_jobs_failed", "topic", tc.Name, "partition", partition, "error", err)
		return
	}
	var wg sync.WaitGroup
	for _, job := range jobs {
		if job.Type != PartitionJobTypeSegmentMerge {
			continue
		}
		wg.Add(1)
		go func(job PartitionJob) {
			defer wg.Done()
			s.disklessMergeExecSem <- struct{}{}
			defer func() { <-s.disklessMergeExecSem }()
			if err := s.partitionLeader().runJob(ctx, job); err != nil {
				slog.Warn("diskless_compaction_job_failed", "topic", job.Topic, "partition", job.Partition, "job", job.ID, "error", err)
			}
		}(job)
	}
	wg.Wait()
}
