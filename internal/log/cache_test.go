package log

import (
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
