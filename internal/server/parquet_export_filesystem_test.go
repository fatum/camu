package server

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCleanupExportFilesystemRemovesOnlyCrashLeftovers verifies the export temp
// directory is wiped of temporary Parquet files (crash leftovers) while
// unrelated files are retained.
func TestCleanupExportFilesystemRemovesOnlyCrashLeftovers(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"camu-parquet-123.parquet", "camu-parquet-abc.parquet.tmp", "keep.txt", "other.parquet"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
	}
	if err := cleanupExportFilesystem(dir); err != nil {
		t.Fatalf("cleanupExportFilesystem() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "camu-parquet-123.parquet")); !os.IsNotExist(err) {
		t.Fatalf("crash leftover camu-parquet-123.parquet was not removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "keep.txt")); err != nil {
		t.Fatalf("keep.txt was removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "other.parquet")); err != nil {
		t.Fatalf("other.parquet was removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "camu-parquet-abc.parquet.tmp")); err != nil {
		t.Fatalf("unrelated camu-parquet-abc.parquet.tmp was removed")
	}
}
