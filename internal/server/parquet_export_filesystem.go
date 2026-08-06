package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// cleanupExportFilesystem removes temporary Parquet data files left behind by
// an interrupted export pass. Canonical exported data objects live in the
// object store, so any camu-parquet-*.parquet file in the temp directory is a
// leftover from a crash.
func cleanupExportFilesystem(tempDir string) error {
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return fmt.Errorf("create export temp directory: %w", err)
	}
	if err := removeMatchingFiles(tempDir, func(name string) bool {
		return strings.HasPrefix(name, "camu-parquet-") && strings.HasSuffix(name, ".parquet")
	}); err != nil {
		return fmt.Errorf("clean export temp leftovers: %w", err)
	}
	return nil
}

func removeMatchingFiles(dir string, match func(string) bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !match(entry.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
