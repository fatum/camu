package server

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupSQLFilesystemRemovesOnlyCrashLeftovers(t *testing.T) {
	root := t.TempDir()
	cacheDir := filepath.Join(root, "cache")
	tempDir := filepath.Join(root, "tmp")
	for _, path := range []string{
		filepath.Join(cacheDir, ".camu-cache-interrupted.parquet.tmp"),
		filepath.Join(tempDir, "camu-query-interrupted.parquet"),
		filepath.Join(tempDir, "camu-parquet-interrupted.parquet"),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("leftover"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	kept := []string{
		filepath.Join(cacheDir, "canonical.parquet"),
		filepath.Join(cacheDir, "keep.txt"),
		filepath.Join(tempDir, "duckdb-owned.tmp"),
	}
	for _, path := range kept {
		if err := os.WriteFile(path, []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if err := cleanupSQLFilesystem(cacheDir, tempDir); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		filepath.Join(cacheDir, ".camu-cache-interrupted.parquet.tmp"),
		filepath.Join(tempDir, "camu-query-interrupted.parquet"),
		filepath.Join(tempDir, "camu-parquet-interrupted.parquet"),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("leftover remains at %s: %v", path, err)
		}
	}
	for _, path := range kept {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("kept file removed at %s: %v", path, err)
		}
	}
}
