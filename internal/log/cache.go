package log

import (
	"container/list"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrCacheMiss is returned by Get when the requested key is not in the cache.
var ErrCacheMiss = errors.New("cache miss")

// cacheEntry tracks a single cached item for LRU ordering.
type cacheEntry struct {
	key      string
	filename string
	size     int64
}

// DiskCache is a flat-file, LRU-evicting disk cache keyed by arbitrary strings.
// Keys are SHA256-hashed to produce safe filenames, so S3-style keys like
// "topic/0/100-1.segment" are stored without path separator issues.
type DiskCache struct {
	dir     string
	maxSize int64

	mu      sync.Mutex
	current int64
	order   *list.List               // front = most recently accessed
	index   map[string]*list.Element // key -> *list.Element (value = *cacheEntry)
}

// NewDiskCache creates a DiskCache rooted at dir with the given byte limit.
// The directory is created if it does not already exist.
func NewDiskCache(dir string, maxSize int64) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("disk cache: create dir %s: %w", dir, err)
	}
	// Cache keys are held only in memory, so files from a previous process
	// cannot be looked up or included in the new LRU. Remove those hashed cache
	// objects now rather than allowing unreachable files to accumulate forever.
	// Leave non-cache entries alone: callers may keep durable local state in a
	// sibling directory under the same cache root.
	if err := removeStaleCacheFiles(dir); err != nil {
		return nil, err
	}
	return &DiskCache{
		dir:     dir,
		maxSize: maxSize,
		order:   list.New(),
		index:   make(map[string]*list.Element),
	}, nil
}

func removeStaleCacheFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("disk cache: list %s: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !isCacheFilename(entry.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("disk cache: remove stale entry %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func isCacheFilename(name string) bool {
	if len(name) != sha256.Size*2 {
		return false
	}
	for _, ch := range name {
		if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

// hashKey converts an arbitrary string key into a hex-encoded SHA256 filename.
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", sum)
}

// Put writes data to the cache under key, evicting oldest entries if the
// total cached size would exceed maxSize.
func (c *DiskCache) Put(key string, data []byte) error {
	filename := hashKey(key)
	path := filepath.Join(c.dir, filename)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("disk cache: write %s: %w", key, err)
	}

	size := int64(len(data))

	c.mu.Lock()
	defer c.mu.Unlock()

	// If the key already exists, remove the old entry first.
	if elem, ok := c.index[key]; ok {
		old := elem.Value.(*cacheEntry)
		c.current -= old.size
		c.order.Remove(elem)
		delete(c.index, key)
	}

	entry := &cacheEntry{key: key, filename: filename, size: size}
	elem := c.order.PushFront(entry)
	c.index[key] = elem
	c.current += size

	// Evict from the back (least recently used) until we're within budget.
	for c.current > c.maxSize && c.order.Len() > 0 {
		c.evictOldest()
	}

	return nil
}

// evictOldest removes the least-recently-used entry. Must be called with mu held.
func (c *DiskCache) evictOldest() {
	elem := c.order.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(*cacheEntry)
	evictPath := filepath.Join(c.dir, entry.filename)
	_ = os.Remove(evictPath) // best-effort; ignore error
	c.current -= entry.size
	c.order.Remove(elem)
	delete(c.index, entry.key)
}

// Get retrieves data for key. Returns ErrCacheMiss if the key is not cached.
// A successful Get promotes the entry to most-recently-used.
func (c *DiskCache) Get(key string) ([]byte, error) {
	c.mu.Lock()
	elem, ok := c.index[key]
	if !ok {
		c.mu.Unlock()
		return nil, ErrCacheMiss
	}
	entry := elem.Value.(*cacheEntry)
	path := filepath.Join(c.dir, entry.filename)
	c.order.MoveToFront(elem)
	c.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// File was removed externally; clean up tracking.
			c.mu.Lock()
			if el, still := c.index[key]; still {
				e := el.Value.(*cacheEntry)
				c.current -= e.size
				c.order.Remove(el)
				delete(c.index, key)
			}
			c.mu.Unlock()
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("disk cache: read %s: %w", key, err)
	}
	return data, nil
}

// ReadRange retrieves a bounded byte range without materialising the cached
// object in memory. A successful read promotes the entry to most-recently-used.
func (c *DiskCache) ReadRange(key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("disk cache: invalid range offset=%d length=%d", offset, length)
	}

	c.mu.Lock()
	elem, ok := c.index[key]
	if !ok {
		c.mu.Unlock()
		return nil, ErrCacheMiss
	}
	entry := elem.Value.(*cacheEntry)
	path := filepath.Join(c.dir, entry.filename)
	c.order.MoveToFront(elem)
	c.mu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			c.Delete(key)
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("disk cache: open %s: %w", key, err)
	}
	defer file.Close()

	if offset >= entry.size {
		return []byte{}, nil
	}
	if length > entry.size-offset {
		length = entry.size - offset
	}
	data := make([]byte, length)
	n, err := file.ReadAt(data, offset)
	if err != nil && n != len(data) {
		return nil, fmt.Errorf("disk cache: read %s: %w", key, err)
	}
	return data[:n], nil
}

// Has reports whether key is present in the cache without reading its data.
func (c *DiskCache) Has(key string) bool {
	c.mu.Lock()
	_, ok := c.index[key]
	c.mu.Unlock()
	return ok
}

// Delete removes key from the cache if it is present.
func (c *DiskCache) Delete(key string) {
	c.mu.Lock()
	elem, ok := c.index[key]
	if !ok {
		c.mu.Unlock()
		return
	}
	entry := elem.Value.(*cacheEntry)
	delete(c.index, key)
	c.order.Remove(elem)
	c.current -= entry.size
	c.mu.Unlock()

	_ = os.Remove(filepath.Join(c.dir, entry.filename))
}
