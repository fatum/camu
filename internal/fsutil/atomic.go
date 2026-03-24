package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// AtomicWriteFile writes data to path via a temp file in the same directory and
// then renames it into place. The temp file is fsynced before rename, and the
// parent directory is fsynced after rename for crash durability.
func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("atomic write mkdir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("atomic write create temp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		tmp.Close()
		_ = os.Remove(tmpPath)
	}

	if err := tmp.Chmod(perm); err != nil {
		cleanup()
		return fmt.Errorf("atomic write chmod: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("atomic write write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("atomic write sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("atomic write close temp: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("atomic write rename: %w", err)
	}

	dirf, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("atomic write open dir: %w", err)
	}
	defer dirf.Close()
	if err := dirf.Sync(); err != nil {
		return fmt.Errorf("atomic write sync dir: %w", err)
	}

	return nil
}
