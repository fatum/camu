package log

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiskCache_PutAndGet(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskCache(dir, 1024*1024) // 1MB
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	key := "topic/0/100-1.segment"
	data := []byte("hello, segment data")

	if err := cache.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := cache.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(got) != string(data) {
		t.Errorf("Get returned %q, want %q", got, data)
	}
}

func TestDiskCache_Miss(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskCache(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	_, err = cache.Get("nonexistent/key")
	if err != ErrCacheMiss {
		t.Errorf("Get nonexistent key: got %v, want ErrCacheMiss", err)
	}
}

func TestDiskCache_ReadRange(t *testing.T) {
	cache, err := NewDiskCache(t.TempDir(), 1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	if err := cache.Put("segment", []byte("0123456789")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	data, err := cache.ReadRange("segment", 3, 4)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if got, want := string(data), "3456"; got != want {
		t.Errorf("ReadRange = %q, want %q", got, want)
	}
}

func TestDiskCache_Eviction(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskCache(dir, 100) // 100 byte limit
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	key1 := "segment/one"
	data1 := make([]byte, 60)
	for i := range data1 {
		data1[i] = 'a'
	}

	key2 := "segment/two"
	data2 := make([]byte, 60)
	for i := range data2 {
		data2[i] = 'b'
	}

	if err := cache.Put(key1, data1); err != nil {
		t.Fatalf("Put key1: %v", err)
	}

	if err := cache.Put(key2, data2); err != nil {
		t.Fatalf("Put key2: %v", err)
	}

	// key1 should have been evicted (oldest accessed)
	if cache.Has(key1) {
		t.Error("key1 should have been evicted but still exists")
	}

	// key2 should still be present
	got, err := cache.Get(key2)
	if err != nil {
		t.Fatalf("Get key2 after eviction: %v", err)
	}
	if string(got) != string(data2) {
		t.Errorf("key2 data mismatch after eviction")
	}
}

func TestNewDiskCacheRemovesStaleFilesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	stalePath := filepath.Join(dir, hashKey("old-segment"))
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	localDir := filepath.Join(dir, "local")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatal(err)
	}
	localPath := filepath.Join(localDir, "active.log")
	if err := os.WriteFile(localPath, []byte("durable local state"), 0o644); err != nil {
		t.Fatal(err)
	}
	markerPath := filepath.Join(dir, "do-not-delete")
	if err := os.WriteFile(markerPath, []byte("marker"), 0o644); err != nil {
		t.Fatal(err)
	}

	cache, err := NewDiskCache(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if cache.Has("old-segment") {
		t.Fatal("stale cache key was restored into the new process")
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale cache file remains: %v", err)
	}
	if _, err := os.Stat(localPath); err != nil {
		t.Fatalf("local state was removed: %v", err)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("non-cache file was removed: %v", err)
	}
}
