package fsutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAtomicWriteFile_ReplacesContentsWithoutTempLeak(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch.sidecar")

	if err := AtomicWriteFile(path, []byte("1"), 0o644); err != nil {
		t.Fatalf("AtomicWriteFile(v1): %v", err)
	}
	if err := AtomicWriteFile(path, []byte("2"), 0o644); err != nil {
		t.Fatalf("AtomicWriteFile(v2): %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(): %v", err)
	}
	if got := string(data); got != "2" {
		t.Fatalf("file contents = %q, want %q", got, "2")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(): %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp-") {
			t.Fatalf("unexpected temp file left behind: %s", entry.Name())
		}
	}
}
