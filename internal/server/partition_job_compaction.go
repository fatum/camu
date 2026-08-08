package server

import (
	"context"
	"fmt"
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

// disklessCompactionHint is the per-partition discovery cache. Discovery
// (QueryHeadSegments + run building) is the expensive part of a compaction tick
// and is elided when the committed head is unchanged and no ref can have newly
// crossed the grace boundary since the last scan. Execution of in-flight jobs
// is never gated on this hint.
type disklessCompactionHint struct {
	committed   int64
	lastScanned time.Time
}

// disklessCompactionCanSkipDiscovery reports whether discovery can be skipped
// for a partition. The committed head and the last scan time are the hint: when
// both are set and the head is unchanged, discovery is elided. Because refs
// only become eligible by aging past the grace cutoff (time-based, never
// offset-based), a partition that has not advanced its committed head has no
// NEW refs to merge; the only cost of skipping is that an already-scanned ref
// that crosses the grace boundary is merged on the next scan instead of
// immediately, which is bounded and acceptable for background compaction. The
// grace-window check keeps scans at least once per grace period for idle
// partitions rather than every tick.
func disklessCompactionCanSkipDiscovery(hint disklessCompactionHint, hasHint bool, committed int64, grace time.Duration, now time.Time) bool {
	if !hasHint {
		return false
	}
	if committed != hint.committed {
		return false
	}
	if now.Sub(hint.lastScanned) < grace {
		return true
	}
	return false
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

	// Read the committed head once; it doubles as the discovery-skip hint. A
	// partition whose head has not moved since the last scan (and that was
	// scanned within the grace window) needs no discovery re-read, because the
	// same refs were evaluated already and no new ref can have aged past grace.
	hintKey := fmt.Sprintf("%s/%d", tc.Name, partition)
	cfg := s.cfg.Diskless.Compaction
	grace, graceErr := cfg.GraceDuration()
	committed, committedErr := s.disklessMeta.GetCommittedHead(ctx, tc.Name, partition)
	if committedErr != nil {
		slog.Warn("diskless_compaction_committed_failed", "topic", tc.Name, "partition", partition, "error", committedErr)
		return
	}
	now := time.Now()
	skipDiscovery := false
	if graceErr == nil {
		s.compactionHintMu.Lock()
		hint, hasHint := s.compactionHints[hintKey]
		skipDiscovery = disklessCompactionCanSkipDiscovery(hint, hasHint, committed, grace, now)
		s.compactionHintMu.Unlock()
	}

	jobs, err := s.listPartitionJobs(ctx, tc.Name, partition)
	if err != nil {
		slog.Warn("diskless_compaction_list_jobs_failed", "topic", tc.Name, "partition", partition, "error", err)
		return
	}
	if !skipDiscovery {
		s.discoverDisklessSegmentMergeJobs(ctx, tc, identity, jobs)
		s.compactionHintMu.Lock()
		s.compactionHints[hintKey] = disklessCompactionHint{committed: committed, lastScanned: now}
		s.compactionHintMu.Unlock()
		jobs, err = s.listPartitionJobs(ctx, tc.Name, partition)
		if err != nil {
			slog.Warn("diskless_compaction_relist_jobs_failed", "topic", tc.Name, "partition", partition, "error", err)
			return
		}
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
