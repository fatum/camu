package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/storage"
)

const (
	// disklessOrphanGrace is how long an uploaded-but-uncommitted object may
	// linger before it is considered orphaned. Uploads are committed within a
	// flush (a few seconds), so this is far beyond any in-flight window.
	disklessOrphanGrace = time.Hour
	// maxDisklessOrphansPerSweep bounds the work of one sweep pass.
	maxDisklessOrphansPerSweep = 1000
)

// sweepDisklessOrphans deletes uploaded objects under _diskless/ that no
// partition manifest references and that are older than the grace period. Such
// objects are produced when a commit permanently fails after upload, or when an
// idempotent retry is tombstoned (deduplicated by producer sequence) and its
// re-upload is never referenced. Run by the leader on a slow cadence; an
// object referenced by no manifest is safe for any node to delete.
func (s *Server) sweepDisklessOrphans(ctx context.Context) {
	if s.disklessMeta == nil {
		return
	}
	keys, err := s.s3Client.List(ctx, "_diskless/")
	if err != nil {
		slog.Warn("diskless_orphan_list_failed", "error", err)
		return
	}
	if len(keys) == 0 {
		return
	}
	// Only consider objects older than the grace period so an in-flight upload
	// (uploaded but about to be committed) is never deleted.
	cutoff := time.Now().Add(-disklessOrphanGrace)
	candidates := make([]string, 0, min(len(keys), maxDisklessOrphansPerSweep))
	for _, key := range keys {
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
		candidates = append(candidates, key)
		if len(candidates) >= maxDisklessOrphansPerSweep {
			break
		}
	}
	if len(candidates) == 0 {
		return
	}
	unreferenced, err := s.disklessMeta.PlanUnreferencedFileDeletes(ctx, candidates)
	if err != nil {
		slog.Warn("diskless_orphan_plan_failed", "error", err)
		return
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
