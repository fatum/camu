package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/storage"
)

// disklessOrphanGrace is how long an uploaded-but-uncommitted object may
// linger before it is considered orphaned. Uploads are committed within a
// flush (a few seconds), so this is far beyond any in-flight window. A var so
// tests can shorten it.
var disklessOrphanGrace = time.Hour

const (
	// maxDisklessOrphansPerSweep bounds the work of one sweep pass.
	maxDisklessOrphansPerSweep = 1000
)

// disklessCheckpointReaper is implemented by MetaStore backends whose archived
// checkpoints can be orphaned (currently only the S3 metastore).
type disklessCheckpointReaper interface {
	ListOrphanedCheckpoints(ctx context.Context) ([]string, error)
}

// disklessFileIndexer is implemented by MetaStore backends that can build a
// single-pass snapshot of every referenced file and live checkpoint (currently
// the S3 metastore), so the sweeps and retention never re-enumerate all
// partition metadata per file.
type disklessFileIndexer interface {
	BuildFileIndex(ctx context.Context) (*diskless.FileIndex, error)
}

// buildDisklessFileIndex builds the shared per-pass metadata snapshot, or
// (nil, nil) when the metastore cannot (memory/DynamoDB backends). A non-nil
// error means the snapshot is incomplete and every consumer of it must fail
// closed: retention and the orphan sweeps would otherwise treat
// checkpoint-referenced data as unreferenced and delete committed records.
func (s *Server) buildDisklessFileIndex(ctx context.Context) (*diskless.FileIndex, error) {
	if s.disklessMeta == nil {
		return nil, nil
	}
	indexer, ok := s.disklessMeta.(disklessFileIndexer)
	if !ok {
		return nil, nil
	}
	idx, err := indexer.BuildFileIndex(ctx)
	if err != nil {
		slog.Warn("diskless_file_index_build_failed", "error", err)
		return nil, err
	}
	return idx, nil
}

// sweepDisklessArchiveOrphans deletes archived checkpoint objects under
// _diskless_meta/archive/ that no partition head's archive chain reaches and
// that are older than the grace period. They are produced when an archive run's
// checkpoint write succeeds but its head CAS loses (the refs stay in the head
// and the checkpoint is never linked), and they would otherwise accumulate.
// Run by the leader on the same slow cadence as the data orphan sweep.
func (s *Server) sweepDisklessArchiveOrphans(ctx context.Context, fileIdx *diskless.FileIndex) {
	if fileIdx == nil {
		// Fallback for backends without a shared index (memory/DynamoDB).
		s.sweepDisklessArchiveOrphansFallback(ctx)
		return
	}
	keys, err := s.s3Client.List(ctx, "_diskless_meta/archive/")
	if err != nil {
		slog.Warn("diskless_archive_orphan_list_failed", "error", err)
		return
	}
	cutoff := time.Now().Add(-disklessOrphanGrace)
	deleted := 0
	for _, key := range keys {
		if _, live := fileIdx.LiveCheckpoints[key]; live {
			continue
		}
		mod, err := s.s3Client.Stat(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			slog.Warn("diskless_archive_orphan_stat_failed", "key", key, "error", err)
			continue
		}
		if mod.After(cutoff) {
			continue
		}
		if err := s.s3Client.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
			slog.Warn("diskless_archive_orphan_delete_failed", "key", key, "error", err)
			continue
		}
		deleted++
	}
	if deleted > 0 {
		slog.Info("diskless_archive_orphan_sweep_deleted", "count", deleted)
	}
}

func (s *Server) sweepDisklessArchiveOrphansFallback(ctx context.Context) {
	reaper, ok := s.disklessMeta.(disklessCheckpointReaper)
	if !ok {
		return
	}
	orphans, err := reaper.ListOrphanedCheckpoints(ctx)
	if err != nil {
		slog.Warn("diskless_archive_orphan_list_failed", "error", err)
		return
	}
	if len(orphans) == 0 {
		return
	}
	cutoff := time.Now().Add(-disklessOrphanGrace)
	deleted := 0
	for _, key := range orphans {
		mod, err := s.s3Client.Stat(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			slog.Warn("diskless_archive_orphan_stat_failed", "key", key, "error", err)
			continue
		}
		if mod.After(cutoff) {
			continue
		}
		if err := s.s3Client.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
			slog.Warn("diskless_archive_orphan_delete_failed", "key", key, "error", err)
			continue
		}
		deleted++
	}
	if deleted > 0 {
		slog.Info("diskless_archive_orphan_sweep_deleted", "count", deleted)
	}
}

// sweepDisklessOrphans deletes uploaded objects that no partition references
// and that are older than the grace period. It covers both the per-flush data
// objects under _diskless/ (an upload whose commit permanently failed, or a
// tombstoned idempotent retry) and the compaction merge objects under
// _diskless_merge/ (merged artifacts whose merge job died before publishing
// refs, or a deleted topic's merged data). Run by the leader on a slow cadence;
// an object referenced by no head or checkpoint is safe for any node to delete.
func (s *Server) sweepDisklessOrphans(ctx context.Context, fileIdx *diskless.FileIndex) {
	if s.disklessMeta == nil {
		return
	}
	cutoff := time.Now().Add(-disklessOrphanGrace)
	var candidates []string
	var mu sync.Mutex

	// _diskless/ flush objects carry their upload millis in the key
	// (_diskless/{shard}/{node}-{ms}-{seq}.data), so the grace filter needs no
	// per-object Stat. List the fixed shard prefixes in parallel instead of one
	// giant listing; a bounded worker pool keeps the request burst in check.
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for shard := 0; shard < diskless.DisklessShardCount; shard++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			prefix := fmt.Sprintf("_diskless/%03d/", shard)
			keys, err := s.s3Client.List(ctx, prefix)
			if err != nil {
				slog.Warn("diskless_orphan_list_failed", "prefix", prefix, "error", err)
				return
			}
			for _, key := range keys {
				mu.Lock()
				if len(candidates) >= maxDisklessOrphansPerSweep {
					mu.Unlock()
					return
				}
				mu.Unlock()
				ms, ok := disklessFlushUploadMs(key)
				if !ok {
					continue // not a flush object; never treated as a candidate
				}
				if time.UnixMilli(ms).After(cutoff) {
					continue // still inside the in-flight grace window
				}
				mu.Lock()
				candidates = append(candidates, key)
				mu.Unlock()
			}
		}(shard)
	}
	wg.Wait()

	// _diskless_merge/ objects carry no timestamp in their key, so keep the
	// Stat-based grace filter for them; there are orders of magnitude fewer.
	{
		keys, err := s.s3Client.List(ctx, "_diskless_merge/")
		if err != nil {
			slog.Warn("diskless_orphan_list_failed", "prefix", "_diskless_merge/", "error", err)
		} else {
			for _, key := range keys {
				mu.Lock()
				if len(candidates) >= maxDisklessOrphansPerSweep {
					mu.Unlock()
					break
				}
				mu.Unlock()
				mod, err := s.s3Client.Stat(ctx, key)
				if err != nil {
					if errors.Is(err, storage.ErrNotFound) {
						continue
					}
					slog.Warn("diskless_orphan_stat_failed", "key", key, "error", err)
					continue
				}
				if mod.After(cutoff) {
					continue
				}
				mu.Lock()
				candidates = append(candidates, key)
				mu.Unlock()
			}
		}
	}

	if len(candidates) == 0 {
		return
	}
	var unreferenced []string
	if fileIdx != nil {
		for _, key := range candidates {
			if _, ok := fileIdx.ByFile[key]; !ok {
				unreferenced = append(unreferenced, key)
			}
		}
	} else {
		var err error
		unreferenced, err = s.disklessMeta.PlanUnreferencedFileDeletes(ctx, candidates)
		if err != nil {
			slog.Warn("diskless_orphan_plan_failed", "error", err)
			return
		}
	}
	for _, key := range unreferenced {
		if err := s.s3Client.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
			slog.Warn("diskless_orphan_delete_failed", "key", key, "error", err)
		}
	}
	if len(unreferenced) > 0 {
		slog.Info("diskless_orphan_sweep_deleted", "count", len(unreferenced))
	}
}

// disklessFlushUploadMs extracts the upload millis from a raw flush object key
// of the form _diskless/{shard}/{node}-{ms}-{seq}.data. The node id may itself
// contain dashes, so the millis are taken as the second-to-last component.
func disklessFlushUploadMs(key string) (int64, bool) {
	base := strings.TrimSuffix(filepath.Base(key), ".data")
	parts := strings.Split(base, "-")
	if len(parts) < 3 {
		return 0, false
	}
	ms, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
	if err != nil || ms <= 0 {
		return 0, false
	}
	return ms, true
}
