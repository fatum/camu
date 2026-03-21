# Camu Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a stateless, S3-backed commit log server in Go with Kafka-style partitioned topics, consumer groups, and comprehensive integration testing.

**Architecture:** Single Go binary with HTTP/REST API. Messages are written to a local WAL (fsync for durability), batched into segments, and flushed to S3. Partitions are owned by instances via S3-based leases with epoch fencing. Consumers read through a tiered path: in-memory buffer → disk cache → S3. Integration tests use real HTTP requests against real camu instances backed by MinIO in Docker.

**Tech Stack:** Go 1.22+, AWS SDK v2 (S3), net/http (stdlib router), testcontainers-go (MinIO), snappy/zstd compression

**Spec:** `docs/superpowers/specs/2026-03-21-camu-design.md`

---

## File Structure

### Core

| File | Responsibility |
|------|---------------|
| `cmd/camu/main.go` | CLI entrypoint: `serve` and `test` subcommands |
| `internal/config/config.go` | YAML/env config parsing and validation |
| `internal/storage/s3.go` | S3-compatible client (Put, Get, Delete, List, ConditionalPut) |
| `internal/log/message.go` | Message type definition |
| `internal/log/segment.go` | Segment binary serialization/deserialization |
| `internal/log/wal.go` | Write-ahead log: append, fsync, replay, truncate |
| `internal/log/partition.go` | Partition state: offset tracking, segment lifecycle, disk cache |
| `internal/log/cache.go` | Disk-based segment cache with LRU eviction |
| `internal/log/index.go` | Partition index: offset-to-segment mapping |
| `internal/meta/topic.go` | Topic metadata CRUD in S3 |
| `internal/producer/router.go` | Key hashing, round-robin partition routing |
| `internal/producer/batcher.go` | Per-partition batching, flush triggers |
| `internal/consumer/fetcher.go` | Tiered read path: buffer → disk cache → S3 |
| `internal/consumer/sse.go` | SSE streaming for consumers |
| `internal/consumer/group.go` | Consumer group coordination and rebalancing |
| `internal/coordination/lease.go` | Partition lease acquire/renew/release with epoch fencing |
| `internal/coordination/registry.go` | Instance registration and heartbeat |
| `internal/coordination/rebalancer.go` | Partition-to-instance assignment |
| `internal/server/server.go` | HTTP server setup, middleware, graceful shutdown |
| `internal/server/routes.go` | Route registration |
| `internal/server/handlers_topic.go` | Topic CRUD handlers |
| `internal/server/handlers_produce.go` | Produce handlers (high-level + low-level) |
| `internal/server/handlers_consume.go` | Consume + SSE handlers |
| `internal/server/handlers_group.go` | Consumer group handlers |
| `internal/server/handlers_cluster.go` | Cluster status + routing handlers |
| `internal/storage/offsets.go` | Offset storage (group + consumer-specific) in S3 |

### Testing

| File | Responsibility |
|------|---------------|
| `pkg/camutest/env.go` | Test environment: MinIO container + camu instances |
| `pkg/camutest/client.go` | HTTP test client with assertion helpers |
| `pkg/camutest/options.go` | Functional options: WithInstances, WithMinIO, etc. |
| `test/integration/topic_test.go` | Topic CRUD integration tests |
| `test/integration/produce_test.go` | Produce integration tests |
| `test/integration/consume_test.go` | Consume integration tests (polling + SSE) |
| `test/integration/group_test.go` | Consumer group integration tests |
| `test/integration/multiinstance_test.go` | Multi-instance routing + rebalance tests |
| `test/integration/durability_test.go` | WAL replay, crash recovery tests |
| `test/integration/chaos_test.go` | Chaos tests (random kills, latency injection) |
| `test/bench/throughput_test.go` | Produce/consume throughput benchmarks |

### Jepsen

| File | Responsibility |
|------|---------------|
| `jepsen/camu/project.clj` | Clojure project definition, Jepsen dependency |
| `jepsen/camu/src/jepsen/camu.clj` | Main test runner, generator, checker composition |
| `jepsen/camu/src/jepsen/camu/client.clj` | HTTP client for produce/consume operations |
| `jepsen/camu/src/jepsen/camu/nemesis.clj` | Fault injection: kill, partition, pause |
| `jepsen/camu/src/jepsen/camu/checker.clj` | Invariant checkers: no data loss, offset monotonicity, no split-brain |
| `jepsen/camu/src/jepsen/camu/db.clj` | Cluster lifecycle: install, start, stop camu on nodes |
| `jepsen/camu/docker-compose.yml` | 5-node Jepsen environment + MinIO |
| `jepsen/camu/run.sh` | Build + run script |

---

### Task 1: Project Scaffolding

**Files:**
- Create: `go.mod`
- Create: `cmd/camu/main.go`
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Create: `camu.yaml.example`

- [ ] **Step 1: Initialize Go module**

```bash
cd /Users/maksim/Projects/camu
go mod init github.com/maksim/camu
```

- [ ] **Step 2: Write config test**

Create `internal/config/config_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	yaml := `
server:
  address: ":9090"
  instance_id: "test-1"
storage:
  bucket: "test-bucket"
  region: "us-west-2"
  endpoint: "http://localhost:9000"
  credentials:
    access_key: "minioadmin"
    secret_key: "minioadmin"
wal:
  directory: "/tmp/wal"
  fsync: true
segments:
  max_size: 4194304
  max_age: "3s"
  compression: "snappy"
cache:
  directory: "/tmp/cache"
  max_size: 1073741824
coordination:
  lease_ttl: "10s"
  heartbeat_interval: "3s"
  rebalance_delay: "5s"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "camu.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Server.Address != ":9090" {
		t.Errorf("Server.Address = %q, want %q", cfg.Server.Address, ":9090")
	}
	if cfg.Storage.Bucket != "test-bucket" {
		t.Errorf("Storage.Bucket = %q, want %q", cfg.Storage.Bucket, "test-bucket")
	}
	if cfg.Segments.MaxSize != 4194304 {
		t.Errorf("Segments.MaxSize = %d, want %d", cfg.Segments.MaxSize, 4194304)
	}
	if cfg.Segments.Compression != "snappy" {
		t.Errorf("Segments.Compression = %q, want %q", cfg.Segments.Compression, "snappy")
	}
}

func TestLoadDefaults(t *testing.T) {
	yaml := `
storage:
  bucket: "my-bucket"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "camu.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Server.Address != ":8080" {
		t.Errorf("default Server.Address = %q, want %q", cfg.Server.Address, ":8080")
	}
	if cfg.WAL.Fsync != true {
		t.Error("default WAL.Fsync should be true")
	}
	if cfg.Segments.MaxSize != 8388608 {
		t.Errorf("default Segments.MaxSize = %d, want %d", cfg.Segments.MaxSize, 8388608)
	}
}

func TestLoadMissingBucket(t *testing.T) {
	yaml := `
server:
  address: ":8080"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "camu.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() should error when bucket is missing")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/config/ -v`
Expected: FAIL — `Load` not defined

- [ ] **Step 4: Implement config**

Create `internal/config/config.go`:

```go
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server       ServerConfig       `yaml:"server"`
	Storage      StorageConfig      `yaml:"storage"`
	WAL          WALConfig          `yaml:"wal"`
	Segments     SegmentsConfig     `yaml:"segments"`
	Cache        CacheConfig        `yaml:"cache"`
	Coordination CoordinationConfig `yaml:"coordination"`
}

type ServerConfig struct {
	Address    string `yaml:"address"`
	InstanceID string `yaml:"instance_id"`
}

type StorageConfig struct {
	Bucket      string            `yaml:"bucket"`
	Region      string            `yaml:"region"`
	Endpoint    string            `yaml:"endpoint"`
	Credentials CredentialsConfig `yaml:"credentials"`
}

type CredentialsConfig struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type WALConfig struct {
	Directory string `yaml:"directory"`
	Fsync     bool   `yaml:"fsync"`
}

type SegmentsConfig struct {
	MaxSize     int64  `yaml:"max_size"`
	MaxAge      string `yaml:"max_age"`
	Compression string `yaml:"compression"`
}

func (s SegmentsConfig) MaxAgeDuration() (time.Duration, error) {
	if s.MaxAge == "" {
		return 5 * time.Second, nil
	}
	return time.ParseDuration(s.MaxAge)
}

type CacheConfig struct {
	Directory string `yaml:"directory"`
	MaxSize   int64  `yaml:"max_size"`
}

type CoordinationConfig struct {
	LeaseTTL          string `yaml:"lease_ttl"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`
	RebalanceDelay    string `yaml:"rebalance_delay"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	cfg := &Config{
		Server: ServerConfig{
			Address: ":8080",
		},
		WAL: WALConfig{
			Directory: "/var/lib/camu/wal",
			Fsync:     true,
		},
		Segments: SegmentsConfig{
			MaxSize:     8388608,
			MaxAge:      "5s",
			Compression: "none",
		},
		Cache: CacheConfig{
			Directory: "/var/lib/camu/cache",
			MaxSize:   10737418240,
		},
		Coordination: CoordinationConfig{
			LeaseTTL:          "10s",
			HeartbeatInterval: "3s",
			RebalanceDelay:    "5s",
		},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if cfg.Storage.Bucket == "" {
		return nil, fmt.Errorf("storage.bucket is required")
	}

	return cfg, nil
}
```

- [ ] **Step 5: Add yaml dependency and run tests**

```bash
cd /Users/maksim/Projects/camu && go get gopkg.in/yaml.v3
go test ./internal/config/ -v
```

Expected: PASS

- [ ] **Step 6: Create example config and CLI skeleton**

Create `camu.yaml.example` with the full config from the spec.

Create `cmd/camu/main.go`:

```go
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: camu <serve|test>\n")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		fmt.Println("camu serve: not yet implemented")
	case "test":
		fmt.Println("camu test: not yet implemented")
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
```

- [ ] **Step 7: Verify build**

```bash
cd /Users/maksim/Projects/camu && go build ./cmd/camu/
```

Expected: Builds successfully

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum cmd/ internal/config/ camu.yaml.example
git commit -m "feat: project scaffolding with config parsing"
```

---

### Task 2: S3 Client

**Files:**
- Create: `internal/storage/s3.go`
- Create: `internal/storage/s3_test.go`

Unit tests use a mock HTTP server to verify S3 request formatting. Integration tests against real MinIO come later via `camutest`.

- [ ] **Step 1: Write S3 client tests**

Create `internal/storage/s3_test.go`:

```go
package storage

import (
	"context"
	"testing"
)

func TestS3Client_PutAndGet(t *testing.T) {
	// Skip if no S3 endpoint — integration tests cover real S3
	client := newTestClient(t)
	ctx := context.Background()

	err := client.Put(ctx, "test/key.txt", []byte("hello"), PutOpts{})
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	data, err := client.Get(ctx, "test/key.txt")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("Get() = %q, want %q", string(data), "hello")
	}
}

func TestS3Client_Delete(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	client.Put(ctx, "test/del.txt", []byte("bye"), PutOpts{})
	err := client.Delete(ctx, "test/del.txt")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	_, err = client.Get(ctx, "test/del.txt")
	if err == nil {
		t.Fatal("Get() after Delete should error")
	}
}

func TestS3Client_List(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	client.Put(ctx, "list/a.txt", []byte("a"), PutOpts{})
	client.Put(ctx, "list/b.txt", []byte("b"), PutOpts{})
	client.Put(ctx, "other/c.txt", []byte("c"), PutOpts{})

	keys, err := client.List(ctx, "list/")
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("List() returned %d keys, want 2", len(keys))
	}
}

func TestS3Client_ConditionalPut(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	// First put — no etag required
	etag1, err := client.ConditionalPut(ctx, "cond/key.txt", []byte("v1"), "")
	if err != nil {
		t.Fatalf("ConditionalPut() error: %v", err)
	}
	if etag1 == "" {
		t.Fatal("ConditionalPut() should return etag")
	}

	// Update with correct etag
	etag2, err := client.ConditionalPut(ctx, "cond/key.txt", []byte("v2"), etag1)
	if err != nil {
		t.Fatalf("ConditionalPut() with correct etag error: %v", err)
	}

	// Update with stale etag should fail
	_, err = client.ConditionalPut(ctx, "cond/key.txt", []byte("v3"), etag1)
	if err == nil {
		t.Fatal("ConditionalPut() with stale etag should error")
	}

	_ = etag2
}

// newTestClient creates a client backed by an in-memory map for unit tests.
// Real S3 testing happens in integration tests.
func newTestClient(t *testing.T) *S3Client {
	t.Helper()
	client, err := NewS3Client(S3Config{
		Bucket:   "test-bucket",
		Region:   "us-east-1",
		Endpoint: "memory://", // signals in-memory backend for tests
	})
	if err != nil {
		t.Fatalf("NewS3Client() error: %v", err)
	}
	return client
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/storage/ -v`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement S3 client**

Create `internal/storage/s3.go` with:
- `S3Config` struct (Bucket, Region, Endpoint, AccessKey, SecretKey)
- `PutOpts` struct (ContentType)
- `S3Client` struct with AWS SDK v2 client
- `NewS3Client(cfg S3Config) (*S3Client, error)` — creates client; if Endpoint is `"memory://"`, uses an in-memory map backend for testing
- `Put(ctx, key, data, opts) error`
- `Get(ctx, key) ([]byte, error)` — returns `ErrNotFound` if key doesn't exist
- `Delete(ctx, key) error`
- `List(ctx, prefix) ([]string, error)`
- `ConditionalPut(ctx, key, data, etag) (newEtag, error)` — returns `ErrConflict` if etag doesn't match
- `GetWithETag(ctx, key) ([]byte, string, error)` — returns data + current etag
- `var ErrNotFound = errors.New("not found")`
- `var ErrConflict = errors.New("conflict: etag mismatch")`

The in-memory backend for unit tests stores `map[string]memObject` where `memObject` has `data []byte` and `etag string`. ConditionalPut checks the etag, generates a new one (e.g., UUID) on success. For real S3, use `aws-sdk-go-v2` with `s3.PutObject`, `s3.GetObject`, `s3.DeleteObject`, `s3.ListObjectsV2`, and `If-Match` header for conditional puts.

- [ ] **Step 4: Add AWS SDK dependency and run tests**

```bash
cd /Users/maksim/Projects/camu
go get github.com/aws/aws-sdk-go-v2
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/service/s3
go get github.com/aws/aws-sdk-go-v2/credentials
go test ./internal/storage/ -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/ go.mod go.sum
git commit -m "feat: S3-compatible storage client with conditional puts"
```

---

### Task 3: Message Type and Segment Serialization

**Files:**
- Create: `internal/log/message.go`
- Create: `internal/log/segment.go`
- Create: `internal/log/segment_test.go`

- [ ] **Step 1: Write segment serialization tests**

Create `internal/log/segment_test.go`:

```go
package log

import (
	"bytes"
	"testing"
)

func TestSegmentRoundTrip(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1"), Headers: map[string]string{"h": "1"}},
		{Offset: 1, Timestamp: 2000, Key: nil, Value: []byte("v2"), Headers: nil},
		{Offset: 2, Timestamp: 3000, Key: []byte("k3"), Value: []byte("v3"), Headers: map[string]string{"a": "b", "c": "d"}},
	}

	var buf bytes.Buffer
	err := WriteSegment(&buf, msgs, CompressionNone)
	if err != nil {
		t.Fatalf("WriteSegment() error: %v", err)
	}

	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment() error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ReadSegment() returned %d messages, want 3", len(got))
	}
	if got[0].Offset != 0 || string(got[0].Key) != "k1" || string(got[0].Value) != "v1" {
		t.Errorf("message 0 mismatch: %+v", got[0])
	}
	if got[1].Key != nil {
		t.Errorf("message 1 key should be nil, got %v", got[1].Key)
	}
	if len(got[2].Headers) != 2 {
		t.Errorf("message 2 headers count = %d, want 2", len(got[2].Headers))
	}
}

func TestSegmentMagicNumber(t *testing.T) {
	_, err := ReadSegment(bytes.NewReader([]byte("garbage")), 7)
	if err == nil {
		t.Fatal("ReadSegment() should error on invalid magic number")
	}
}

func TestSegmentEmpty(t *testing.T) {
	var buf bytes.Buffer
	err := WriteSegment(&buf, []Message{}, CompressionNone)
	if err != nil {
		t.Fatalf("WriteSegment() error: %v", err)
	}

	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment() error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ReadSegment() returned %d messages, want 0", len(got))
	}
}

func TestSegmentReadFromOffset(t *testing.T) {
	msgs := []Message{
		{Offset: 10, Timestamp: 1000, Value: []byte("a")},
		{Offset: 11, Timestamp: 2000, Value: []byte("b")},
		{Offset: 12, Timestamp: 3000, Value: []byte("c")},
	}

	var buf bytes.Buffer
	WriteSegment(&buf, msgs, CompressionNone)

	got, err := ReadSegmentFromOffset(bytes.NewReader(buf.Bytes()), int64(buf.Len()), 11, 10)
	if err != nil {
		t.Fatalf("ReadSegmentFromOffset() error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d messages, want 2", len(got))
	}
	if got[0].Offset != 11 {
		t.Errorf("first message offset = %d, want 11", got[0].Offset)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement message type**

Create `internal/log/message.go`:

```go
package log

// Message is the unit of data in the commit log.
type Message struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}
```

- [ ] **Step 4: Implement segment serialization**

Create `internal/log/segment.go` with:
- Magic number: `0x43414D55` ("CAMU")
- Version: `1`
- `const CompressionNone, CompressionSnappy, CompressionZstd`
- `WriteSegment(w io.Writer, msgs []Message, compression string) error` — writes magic, version, then each message frame using binary.BigEndian for all integers. Header pairs: 4-byte count, then for each pair: 4-byte key len, key bytes, 4-byte val len, val bytes.
- `ReadSegment(r io.ReaderAt, size int64) ([]Message, error)` — validates magic/version, reads all frames
- `ReadSegmentFromOffset(r io.ReaderAt, size int64, startOffset uint64, limit int) ([]Message, error)` — reads frames, skips until `offset >= startOffset`, returns up to `limit` messages

- [ ] **Step 5: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/log/
git commit -m "feat: message type and segment binary serialization"
```

---

### Task 4: Write-Ahead Log (WAL)

**Files:**
- Create: `internal/log/wal.go`
- Create: `internal/log/wal_test.go`

- [ ] **Step 1: Write WAL tests**

Create `internal/log/wal_test.go`:

```go
package log

import (
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}

	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 1, Timestamp: 2000, Key: []byte("k2"), Value: []byte("v2")},
	}

	for _, m := range msgs {
		if err := w.Append(m); err != nil {
			t.Fatalf("Append() error: %v", err)
		}
	}
	w.Close()

	// Replay
	w2, err := OpenWAL(path, true)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error: %v", err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("Replay() returned %d messages, want 2", len(replayed))
	}
	if string(replayed[0].Key) != "k1" {
		t.Errorf("replayed[0].Key = %q, want %q", string(replayed[0].Key), "k1")
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}

	for i := uint64(0); i < 5; i++ {
		w.Append(Message{Offset: i, Timestamp: int64(i * 1000), Value: []byte("v")})
	}

	// Truncate up to offset 3 (keep 3, 4)
	err = w.TruncateBefore(3)
	if err != nil {
		t.Fatalf("TruncateBefore() error: %v", err)
	}
	w.Close()

	w2, _ := OpenWAL(path, true)
	defer w2.Close()
	replayed, _ := w2.Replay()
	if len(replayed) != 2 {
		t.Fatalf("after truncate, Replay() returned %d messages, want 2", len(replayed))
	}
	if replayed[0].Offset != 3 {
		t.Errorf("first message offset = %d, want 3", replayed[0].Offset)
	}
}

func TestWAL_EmptyReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.wal")

	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}
	defer w.Close()

	replayed, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if len(replayed) != 0 {
		t.Errorf("Replay() returned %d messages, want 0", len(replayed))
	}
}

func TestWAL_UnflushedMessages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}
	defer w.Close()

	w.Append(Message{Offset: 0, Value: []byte("a")})
	w.Append(Message{Offset: 1, Value: []byte("b")})
	w.Append(Message{Offset: 2, Value: []byte("c")})

	msgs := w.UnflushedFrom(1)
	if len(msgs) != 2 {
		t.Fatalf("UnflushedFrom(1) returned %d messages, want 2", len(msgs))
	}
	if msgs[0].Offset != 1 {
		t.Errorf("first message offset = %d, want 1", msgs[0].Offset)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestWAL`
Expected: FAIL — `OpenWAL` not defined

- [ ] **Step 3: Implement WAL**

Create `internal/log/wal.go` with:
- `WAL` struct: file handle, in-memory buffer of appended messages, fsync flag
- `OpenWAL(path string, fsync bool) (*WAL, error)` — creates/opens file, replays existing entries into memory
- `Append(msg Message) error` — serializes message frame (same format as segment), appends to file, fsyncs if enabled, adds to in-memory buffer
- `Replay() ([]Message, error)` — reads all message frames from the file
- `TruncateBefore(offset uint64) error` — rewrites the WAL file keeping only messages with offset >= the given offset
- `UnflushedFrom(offset uint64) []Message` — returns in-memory messages from the given offset onward
- `Close() error` — closes the file

WAL entry format: `[4-byte entry length][message frame bytes][4-byte CRC32]`. The CRC ensures corrupt entries (from a crash mid-write) are detected and skipped during replay.

- [ ] **Step 4: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestWAL`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/log/wal.go internal/log/wal_test.go
git commit -m "feat: write-ahead log with append, replay, and truncation"
```

---

### Task 5: Partition Index

**Files:**
- Create: `internal/log/index.go`
- Create: `internal/log/index_test.go`

- [ ] **Step 1: Write index tests**

Create `internal/log/index_test.go`:

```go
package log

import (
	"testing"
)

func TestIndex_AddAndLookup(t *testing.T) {
	idx := NewIndex()
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 99, Epoch: 1, Key: "topic/0/0-1.segment"})
	idx.Add(SegmentRef{BaseOffset: 100, EndOffset: 199, Epoch: 1, Key: "topic/0/100-1.segment"})

	ref, ok := idx.Lookup(50)
	if !ok {
		t.Fatal("Lookup(50) should find segment")
	}
	if ref.BaseOffset != 0 {
		t.Errorf("Lookup(50) base offset = %d, want 0", ref.BaseOffset)
	}

	ref, ok = idx.Lookup(150)
	if !ok {
		t.Fatal("Lookup(150) should find segment")
	}
	if ref.BaseOffset != 100 {
		t.Errorf("Lookup(150) base offset = %d, want 100", ref.BaseOffset)
	}

	_, ok = idx.Lookup(200)
	if ok {
		t.Error("Lookup(200) should not find segment")
	}
}

func TestIndex_MarshalJSON(t *testing.T) {
	idx := NewIndex()
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 99, Epoch: 1, Key: "topic/0/0-1.segment"})

	data, err := idx.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error: %v", err)
	}

	idx2 := NewIndex()
	err = idx2.UnmarshalJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalJSON() error: %v", err)
	}

	ref, ok := idx2.Lookup(50)
	if !ok || ref.Key != "topic/0/0-1.segment" {
		t.Errorf("roundtrip failed: %+v, found=%v", ref, ok)
	}
}

func TestIndex_RemoveBefore(t *testing.T) {
	idx := NewIndex()
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 99, Epoch: 1, Key: "s0"})
	idx.Add(SegmentRef{BaseOffset: 100, EndOffset: 199, Epoch: 1, Key: "s1"})
	idx.Add(SegmentRef{BaseOffset: 200, EndOffset: 299, Epoch: 1, Key: "s2"})

	removed := idx.RemoveBefore(150)
	if len(removed) != 1 {
		t.Fatalf("RemoveBefore(150) removed %d, want 1", len(removed))
	}
	if removed[0].Key != "s0" {
		t.Errorf("removed segment key = %q, want %q", removed[0].Key, "s0")
	}

	_, ok := idx.Lookup(50)
	if ok {
		t.Error("Lookup(50) should not find segment after removal")
	}
}

func TestIndex_NextOffset(t *testing.T) {
	idx := NewIndex()
	if idx.NextOffset() != 0 {
		t.Errorf("empty index NextOffset() = %d, want 0", idx.NextOffset())
	}
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 99, Epoch: 1, Key: "s0"})
	if idx.NextOffset() != 100 {
		t.Errorf("NextOffset() = %d, want 100", idx.NextOffset())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestIndex`
Expected: FAIL

- [ ] **Step 3: Implement index**

Create `internal/log/index.go` with:
- `SegmentRef` struct: BaseOffset, EndOffset (inclusive), Epoch, Key (S3 key)
- `Index` struct: sorted slice of `SegmentRef`
- `NewIndex() *Index`
- `Add(ref SegmentRef)` — inserts in sorted order by BaseOffset
- `Lookup(offset uint64) (SegmentRef, bool)` — binary search for segment containing offset
- `RemoveBefore(offset uint64) []SegmentRef` — removes and returns segments where EndOffset < offset
- `NextOffset() uint64` — returns EndOffset+1 of the last segment, or 0 if empty
- `MarshalJSON() / UnmarshalJSON()` — JSON serialization for S3 storage

- [ ] **Step 4: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestIndex`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/log/index.go internal/log/index_test.go
git commit -m "feat: partition index with offset-to-segment lookup"
```

---

### Task 6: Disk-Based Segment Cache

**Files:**
- Create: `internal/log/cache.go`
- Create: `internal/log/cache_test.go`

- [ ] **Step 1: Write cache tests**

Create `internal/log/cache_test.go`:

```go
package log

import (
	"testing"
)

func TestDiskCache_PutAndGet(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskCache(dir, 1024*1024) // 1MB limit
	if err != nil {
		t.Fatalf("NewDiskCache() error: %v", err)
	}

	data := []byte("segment data here")
	err = cache.Put("topic/0/0-1.segment", data)
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	got, err := cache.Get("topic/0/0-1.segment")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Get() = %q, want %q", string(got), string(data))
	}
}

func TestDiskCache_Miss(t *testing.T) {
	dir := t.TempDir()
	cache, _ := NewDiskCache(dir, 1024*1024)

	_, err := cache.Get("nonexistent")
	if err == nil {
		t.Fatal("Get() should error on cache miss")
	}
}

func TestDiskCache_Eviction(t *testing.T) {
	dir := t.TempDir()
	cache, _ := NewDiskCache(dir, 100) // 100 byte limit

	// Write more than 100 bytes
	cache.Put("seg1", make([]byte, 60))
	cache.Put("seg2", make([]byte, 60))

	// seg1 should have been evicted
	_, err := cache.Get("seg1")
	if err == nil {
		t.Error("seg1 should have been evicted")
	}

	// seg2 should still be there
	_, err = cache.Get("seg2")
	if err != nil {
		t.Errorf("seg2 should still exist: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestDiskCache`
Expected: FAIL

- [ ] **Step 3: Implement disk cache**

Create `internal/log/cache.go` with:
- `DiskCache` struct: directory, maxSize, current size, access-ordered list of entries
- `NewDiskCache(dir string, maxSize int64) (*DiskCache, error)`
- `Put(key string, data []byte) error` — writes to `dir/hash(key)`, updates tracking, evicts oldest if over limit
- `Get(key string) ([]byte, error)` — reads from disk, returns `ErrCacheMiss` if not found, updates access order
- `Has(key string) bool` — check without reading
- Eviction: remove oldest-accessed entries until total size is under maxSize

The key is hashed (SHA256 hex) to create a flat filename in the cache directory, avoiding path separator issues.

- [ ] **Step 4: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestDiskCache`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/log/cache.go internal/log/cache_test.go
git commit -m "feat: disk-based segment cache with LRU eviction"
```

---

### Task 7: Topic Metadata

**Files:**
- Create: `internal/meta/topic.go`
- Create: `internal/meta/topic_test.go`

- [ ] **Step 1: Write topic metadata tests**

Create `internal/meta/topic_test.go`:

```go
package meta

import (
	"context"
	"testing"
	"time"
)

func TestTopicStore_CreateAndGet(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	topic := TopicConfig{
		Name:       "orders",
		Partitions: 4,
		Retention:  7 * 24 * time.Hour,
	}

	err := store.Create(ctx, topic)
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	got, err := store.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.Partitions != 4 {
		t.Errorf("Partitions = %d, want 4", got.Partitions)
	}
}

func TestTopicStore_List(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.Create(ctx, TopicConfig{Name: "t1", Partitions: 1, Retention: time.Hour})
	store.Create(ctx, TopicConfig{Name: "t2", Partitions: 2, Retention: time.Hour})

	topics, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("List() returned %d, want 2", len(topics))
	}
}

func TestTopicStore_Delete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.Create(ctx, TopicConfig{Name: "del-me", Partitions: 1, Retention: time.Hour})
	err := store.Delete(ctx, "del-me")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	_, err = store.Get(ctx, "del-me")
	if err == nil {
		t.Fatal("Get() after Delete should error")
	}
}

func TestTopicStore_CreateDuplicate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.Create(ctx, TopicConfig{Name: "dup", Partitions: 1, Retention: time.Hour})
	err := store.Create(ctx, TopicConfig{Name: "dup", Partitions: 1, Retention: time.Hour})
	if err == nil {
		t.Fatal("Create() duplicate should error")
	}
}
```

`newTestStore` creates a `TopicStore` backed by the in-memory S3 client from Task 2.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/meta/ -v`
Expected: FAIL

- [ ] **Step 3: Implement topic metadata**

Create `internal/meta/topic.go` with:
- `TopicConfig` struct: Name, Partitions (int), Retention (time.Duration), CreatedAt (time.Time)
- `TopicStore` struct: s3 client reference
- `NewTopicStore(s3 *storage.S3Client) *TopicStore`
- `Create(ctx, cfg) error` — marshals to JSON, stores at `_meta/topics/{name}.json`, errors if already exists
- `Get(ctx, name) (TopicConfig, error)` — reads and unmarshals
- `List(ctx) ([]TopicConfig, error)` — lists `_meta/topics/` prefix, reads each
- `Delete(ctx, name) error` — deletes the metadata object

- [ ] **Step 4: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/meta/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/meta/
git commit -m "feat: topic metadata CRUD backed by S3"
```

---

### Task 8: Integration Test Infrastructure (camutest)

**Files:**
- Create: `pkg/camutest/env.go`
- Create: `pkg/camutest/client.go`
- Create: `pkg/camutest/options.go`
- Create: `pkg/camutest/env_test.go`

This task builds the test harness. It co-depends with Task 9 (server skeleton). Build the camutest framework and the minimal server skeleton together — the server must exist for camutest to boot instances.

**Important:** Implement the server skeleton (server.go, routes.go, handlers_cluster.go with health check endpoint) as Step 2 of this task, BEFORE writing the camutest env. This resolves the bootstrap dependency — camutest can boot real server instances that respond to health checks.

- [ ] **Step 1: Write camutest test**

Create `pkg/camutest/env_test.go`:

```go
//go:build integration

package camutest

import (
	"testing"
)

func TestEnv_SingleInstance(t *testing.T) {
	env := New(t, WithInstances(1), WithMinIO())
	defer env.Cleanup()

	client := env.Client()

	// Health check
	status, err := client.ClusterStatus()
	if err != nil {
		t.Fatalf("ClusterStatus() error: %v", err)
	}
	if len(status.Instances) != 1 {
		t.Errorf("expected 1 instance, got %d", len(status.Instances))
	}
}

func TestEnv_MultiInstance(t *testing.T) {
	env := New(t, WithInstances(3), WithMinIO())
	defer env.Cleanup()

	client := env.Client()
	status, err := client.ClusterStatus()
	if err != nil {
		t.Fatalf("ClusterStatus() error: %v", err)
	}
	if len(status.Instances) != 3 {
		t.Errorf("expected 3 instances, got %d", len(status.Instances))
	}
}
```

- [ ] **Step 2: Implement options**

Create `pkg/camutest/options.go`:

```go
package camutest

type Options struct {
	Instances int
	UseMinIO  bool
}

type Option func(*Options)

func WithInstances(n int) Option {
	return func(o *Options) { o.Instances = n }
}

func WithMinIO() Option {
	return func(o *Options) { o.UseMinIO = true }
}
```

- [ ] **Step 3: Implement test client**

Create `pkg/camutest/client.go` with:
- `Client` struct: baseURL, http.Client
- `NewClient(baseURL string) *Client` — for remote deployments
- `ClusterStatus() (*ClusterStatusResponse, error)` — `GET /v1/cluster/status`
- `CreateTopic(name string, partitions int, retention time.Duration) error` — `POST /v1/topics`
- `ListTopics() ([]TopicInfo, error)` — `GET /v1/topics`
- `DeleteTopic(name string) error` — `DELETE /v1/topics/{name}`
- `Produce(topic string, msgs []ProduceMessage) (*ProduceResponse, error)` — `POST /v1/topics/{topic}/messages`
- `Consume(topic string, partition int, offset uint64, limit int) (*ConsumeResponse, error)` — `GET /v1/topics/{topic}/partitions/{id}/messages`
- `JoinGroup(groupID, topic string) (*JoinGroupResponse, error)`
- `CommitOffsets(groupID string, offsets map[int]uint64) error`
- `GetOffsets(groupID string) (map[int]uint64, error)`

Each method makes a real HTTP request and returns parsed JSON.

- [ ] **Step 4: Implement test environment**

Create `pkg/camutest/env.go` with:
- `Env` struct: t *testing.T, minio container, camu instances (in-process servers on random ports), S3 config
- `New(t, ...Option) *Env` — starts MinIO via testcontainers-go, creates bucket, boots N camu server instances on random ports
- `Client() *Client` — returns a client pointed at the first instance
- `ClientFor(instance int) *Client` — returns a client for a specific instance
- `Cleanup()` — stops instances, terminates MinIO container

Add dependency: `go get github.com/testcontainers/testcontainers-go`

- [ ] **Step 5: Commit**

```bash
git add pkg/camutest/ go.mod go.sum
git commit -m "feat: camutest integration test infrastructure with MinIO"
```

Note: The `env_test.go` tests won't pass until the server is implemented in Task 9. They serve as the acceptance test for the test framework itself.

---

### Task 9: HTTP Server Skeleton and Topic CRUD

**Files:**
- Create: `internal/server/server.go`
- Create: `internal/server/routes.go`
- Create: `internal/server/handlers_topic.go`
- Create: `internal/server/handlers_cluster.go`
- Create: `test/integration/topic_test.go`

- [ ] **Step 1: Implement server skeleton**

Create `internal/server/server.go` with:
- `Server` struct: config, httpServer, topicStore, s3Client, instanceID, mux
- `New(cfg *config.Config) (*Server, error)` — initializes S3 client, topic store, generates instance ID (UUID if not configured)
- `Start() error` — starts HTTP server
- `Shutdown(ctx) error` — graceful shutdown
- Middleware: adds `X-Camu-Instance-ID` header to every response, JSON content-type, request logging

- [ ] **Step 2: Implement routes**

Create `internal/server/routes.go`:

```go
package server

import "net/http"

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// Topic management
	mux.HandleFunc("POST /v1/topics", s.handleCreateTopic)
	mux.HandleFunc("GET /v1/topics", s.handleListTopics)
	mux.HandleFunc("GET /v1/topics/{topic}", s.handleGetTopic)
	mux.HandleFunc("DELETE /v1/topics/{topic}", s.handleDeleteTopic)

	// Cluster
	mux.HandleFunc("GET /v1/cluster/status", s.handleClusterStatus)

	return s.withMiddleware(mux)
}
```

- [ ] **Step 3: Implement cluster status handler**

Create `internal/server/handlers_cluster.go`:
- `handleClusterStatus` — returns `{"instances": [{"id": "...", "address": "..."}]}`

- [ ] **Step 4: Implement topic handlers**

Create `internal/server/handlers_topic.go`:
- `handleCreateTopic` — parses `{"name": "...", "partitions": N, "retention": "168h"}`, calls topicStore.Create
- `handleListTopics` — calls topicStore.List
- `handleGetTopic` — calls topicStore.Get
- `handleDeleteTopic` — calls topicStore.Delete

All handlers return JSON with appropriate status codes: 201 for create, 200 for get/list, 204 for delete, 404 for not found, 409 for duplicate.

- [ ] **Step 5: Write integration tests**

Create `test/integration/topic_test.go`:

```go
//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestTopicCRUD(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	// Create
	err := client.CreateTopic("test-topic", 4, 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	// Get
	topic, err := client.GetTopic("test-topic")
	if err != nil {
		t.Fatalf("GetTopic() error: %v", err)
	}
	if topic.Partitions != 4 {
		t.Errorf("Partitions = %d, want 4", topic.Partitions)
	}

	// List
	topics, err := client.ListTopics()
	if err != nil {
		t.Fatalf("ListTopics() error: %v", err)
	}
	found := false
	for _, tp := range topics {
		if tp.Name == "test-topic" {
			found = true
		}
	}
	if !found {
		t.Error("test-topic not found in list")
	}

	// Delete
	err = client.DeleteTopic("test-topic")
	if err != nil {
		t.Fatalf("DeleteTopic() error: %v", err)
	}

	_, err = client.GetTopic("test-topic")
	if err == nil {
		t.Error("GetTopic() after delete should error")
	}
}

func TestTopicCreateDuplicate(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("dup-topic", 1, time.Hour)
	err := client.CreateTopic("dup-topic", 1, time.Hour)
	if err == nil {
		t.Error("creating duplicate topic should error")
	}
}
```

- [ ] **Step 6: Run integration tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/integration/ -v -run TestTopic
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/server/ test/integration/topic_test.go
git commit -m "feat: HTTP server with topic CRUD and cluster status"
```

---

### Task 10: Producer — Partition Router

**Files:**
- Create: `internal/producer/router.go`
- Create: `internal/producer/router_test.go`

- [ ] **Step 1: Write router tests**

Create `internal/producer/router_test.go`:

```go
package producer

import (
	"testing"
)

func TestRouter_KeyRouting(t *testing.T) {
	r := NewRouter(4)

	// Same key always maps to same partition
	p1 := r.Route([]byte("user-123"))
	p2 := r.Route([]byte("user-123"))
	if p1 != p2 {
		t.Errorf("same key routed to different partitions: %d vs %d", p1, p2)
	}

	// Different keys can map to different partitions (probabilistic — just check range)
	p3 := r.Route([]byte("user-456"))
	if p3 >= 4 {
		t.Errorf("partition %d out of range [0, 4)", p3)
	}
}

func TestRouter_NilKeyRoundRobin(t *testing.T) {
	r := NewRouter(4)

	seen := make(map[int]bool)
	for i := 0; i < 8; i++ {
		p := r.Route(nil)
		seen[p] = true
	}

	// Round-robin should hit at least 2 different partitions in 8 tries
	if len(seen) < 2 {
		t.Errorf("round-robin only hit %d partitions in 8 tries", len(seen))
	}
}

func TestRouter_SinglePartition(t *testing.T) {
	r := NewRouter(1)
	if r.Route([]byte("any")) != 0 {
		t.Error("single partition should always return 0")
	}
	if r.Route(nil) != 0 {
		t.Error("single partition should always return 0")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/producer/ -v`
Expected: FAIL

- [ ] **Step 3: Implement router**

Create `internal/producer/router.go`:
- `Router` struct: numPartitions int, roundRobinCounter atomic uint64
- `NewRouter(numPartitions int) *Router`
- `Route(key []byte) int` — if key is nil, atomic increment counter mod numPartitions (round-robin); if key is non-nil, `fnv32a(key) % numPartitions`

- [ ] **Step 4: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/producer/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/producer/
git commit -m "feat: partition router with key hashing and round-robin"
```

---

### Task 11: Producer — Batcher and Flush Pipeline

**Files:**
- Create: `internal/producer/batcher.go`
- Create: `internal/producer/batcher_test.go`

- [ ] **Step 1: Write batcher tests**

Create `internal/producer/batcher_test.go`:

```go
package producer

import (
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
)

func TestBatcher_FlushOnSize(t *testing.T) {
	var mu sync.Mutex
	var flushed [][]log.Message

	b := NewBatcher(BatcherConfig{
		MaxSize: 100, // small threshold
		MaxAge:  10 * time.Second,
		OnFlush: func(partitionID int, msgs []log.Message) error {
			mu.Lock()
			flushed = append(flushed, msgs)
			mu.Unlock()
			return nil
		},
	})
	defer b.Stop()

	// Append enough to trigger flush
	for i := 0; i < 10; i++ {
		b.Append(0, log.Message{Offset: uint64(i), Value: make([]byte, 20)})
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(flushed) == 0 {
		t.Fatal("expected at least one flush")
	}
}

func TestBatcher_FlushOnTime(t *testing.T) {
	var mu sync.Mutex
	var flushed [][]log.Message

	b := NewBatcher(BatcherConfig{
		MaxSize: 1 << 30, // huge — won't trigger
		MaxAge:  50 * time.Millisecond,
		OnFlush: func(partitionID int, msgs []log.Message) error {
			mu.Lock()
			flushed = append(flushed, msgs)
			mu.Unlock()
			return nil
		},
	})
	defer b.Stop()

	b.Append(0, log.Message{Offset: 0, Value: []byte("hello")})

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(flushed) == 0 {
		t.Fatal("expected time-based flush")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/producer/ -v -run TestBatcher`
Expected: FAIL

- [ ] **Step 3: Implement batcher**

Create `internal/producer/batcher.go` with:
- `BatcherConfig` struct: MaxSize (int64), MaxAge (time.Duration), OnFlush callback `func(partitionID int, msgs []log.Message) error`
- `Batcher` struct: per-partition buffers (map[int]*partitionBuffer), config
- `partitionBuffer` struct: msgs []Message, size int64, timer *time.Timer, mu sync.Mutex
- `NewBatcher(cfg BatcherConfig) *Batcher`
- `Append(partitionID int, msg Message)` — adds to partition buffer, checks size threshold, resets timer
- When size threshold hit or timer fires → calls `OnFlush(partitionID, msgs)`, clears buffer
- `Stop()` — flushes all remaining buffers
- `Flush(partitionID int) error` — manually flush a partition

- [ ] **Step 4: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/producer/ -v -run TestBatcher`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/producer/batcher.go internal/producer/batcher_test.go
git commit -m "feat: per-partition batcher with size and time flush triggers"
```

---

### Task 12: Produce HTTP Handlers + Integration Tests

**Files:**
- Create: `internal/server/handlers_produce.go`
- Create: `test/integration/produce_test.go`

This wires up the full produce path: HTTP handler → router → WAL append → respond. The batcher runs in the background flushing to S3.

**Single-instance mode:** In Tasks 10-14, the server runs as a single instance that implicitly owns ALL partitions (no lease coordination needed). Multi-instance lease-based ownership is added in Task 15. This allows the produce/consume path to be tested end-to-end before adding coordination complexity.

- [ ] **Step 1: Implement partition manager**

Create `internal/server/partition_manager.go`:
- `PartitionManager` struct: manages per-partition state (WAL, offset counter, in-memory buffer)
- `NewPartitionManager(cfg, s3Client, diskCache) *PartitionManager`
- `Init(ctx, topic TopicConfig) error` — for each partition in the topic:
  - Load partition index from S3 (or create empty index if not found)
  - Open WAL file at `{wal_dir}/{topic}/{partition_id}.wal`
  - Replay WAL to recover unflushed messages
  - Initialize next offset from max(index.NextOffset(), last WAL offset + 1)
  - Create router with topic's partition count
  - Start batcher with OnFlush callback that:
    1. Serializes messages into segment binary format
    2. Uploads segment to S3 at `{topic}/{partition_id}/{base_offset}-{epoch}.segment`
    3. Writes segment to disk cache
    4. Updates partition index via conditional put
    5. Truncates WAL up to flushed offset
- `Append(topic, partitionID, msg) (offset uint64, error)` — appends to WAL, fsyncs, assigns offset
- `IsOwned(topic, partitionID) bool` — returns true (single-instance owns all; overridden in Task 15)
- `GetBuffer(topic, partitionID) []Message` — returns unflushed messages for consumer reads
- `Shutdown(ctx) error` — flushes all batchers, closes WALs

- [ ] **Step 2: Implement produce handlers**

Create `internal/server/handlers_produce.go`:
- `handleProduceHighLevel` — `POST /v1/topics/{topic}/messages`
  - Parses request body: single message `{"key": "base64", "value": "base64", "headers": {}}` or array of messages
  - Validates topic exists via topicStore
  - Routes each message to partition via router
  - Checks `partitionManager.IsOwned()` — if not owned, returns 421 with routing map
  - Calls `partitionManager.Append()` for each message
  - Responds with `{"offsets": [{"partition": N, "offset": M}]}`
- `handleProduceLowLevel` — `POST /v1/topics/{topic}/partitions/{id}/messages`
  - Same but partition is explicit from URL

- [ ] **Step 3: Register routes**

Add to `internal/server/routes.go`:
```go
mux.HandleFunc("POST /v1/topics/{topic}/messages", s.handleProduceHighLevel)
mux.HandleFunc("POST /v1/topics/{topic}/partitions/{id}/messages", s.handleProduceLowLevel)
```

- [ ] **Step 4: Wire up partition manager in server startup**

Update `internal/server/server.go`:
- Create `PartitionManager` during `New()`
- On startup, load all topics from `topicStore.List()`, call `partitionManager.Init()` for each
- When a new topic is created via API, call `partitionManager.Init()` for it
- On shutdown, call `partitionManager.Shutdown()`

- [ ] **Step 5: Write integration tests**

Create `test/integration/produce_test.go`:

```go
//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestProduceSingleMessage(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("produce-test", 4, 24*time.Hour)

	resp, err := client.Produce("produce-test", []camutest.ProduceMessage{
		{Key: "user-1", Value: "hello world"},
	})
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}
	if len(resp.Offsets) != 1 {
		t.Fatalf("expected 1 offset, got %d", len(resp.Offsets))
	}
	if resp.Offsets[0].Offset != 0 {
		t.Errorf("first message offset = %d, want 0", resp.Offsets[0].Offset)
	}
}

func TestProduceBatch(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("batch-test", 1, 24*time.Hour)

	msgs := make([]camutest.ProduceMessage, 100)
	for i := range msgs {
		msgs[i] = camutest.ProduceMessage{Key: "same-key", Value: "msg"}
	}

	resp, err := client.Produce("batch-test", msgs)
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}
	if len(resp.Offsets) != 100 {
		t.Errorf("expected 100 offsets, got %d", len(resp.Offsets))
	}
}

func TestProduceToNonexistentTopic(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	_, err := client.Produce("ghost-topic", []camutest.ProduceMessage{
		{Value: "nope"},
	})
	if err == nil {
		t.Fatal("producing to nonexistent topic should error")
	}
}

func TestProduceKeyRouting(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("routing-test", 4, 24*time.Hour)

	// Same key should always go to same partition
	resp1, _ := client.Produce("routing-test", []camutest.ProduceMessage{{Key: "k1", Value: "a"}})
	resp2, _ := client.Produce("routing-test", []camutest.ProduceMessage{{Key: "k1", Value: "b"}})

	if resp1.Offsets[0].Partition != resp2.Offsets[0].Partition {
		t.Errorf("same key routed to different partitions: %d vs %d",
			resp1.Offsets[0].Partition, resp2.Offsets[0].Partition)
	}
}
```

- [ ] **Step 6: Run integration tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/integration/ -v -run TestProduce
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/server/partition_manager.go internal/server/handlers_produce.go internal/server/routes.go internal/server/server.go test/integration/produce_test.go
git commit -m "feat: produce HTTP handlers with partition manager, WAL write, and S3 flush"
```

---

### Task 13: Consumer — Fetcher and Consume Handlers

**Files:**
- Create: `internal/consumer/fetcher.go`
- Create: `internal/consumer/fetcher_test.go`
- Create: `internal/server/handlers_consume.go`
- Create: `test/integration/consume_test.go`

- [ ] **Step 1: Write fetcher tests**

Create `internal/consumer/fetcher_test.go`:

```go
package consumer

import (
	"context"
	"testing"

	"github.com/maksim/camu/internal/log"
)

func TestFetcher_ReadFromSegmentCache(t *testing.T) {
	f := newTestFetcher(t) // sets up in-memory S3 + disk cache + index with one segment
	ctx := context.Background()

	msgs, nextOffset, err := f.Fetch(ctx, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch() error: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("Fetch() returned no messages")
	}
	if nextOffset <= 0 {
		t.Error("nextOffset should be > 0")
	}
}

func TestFetcher_ReadFromS3(t *testing.T) {
	f := newTestFetcher(t) // S3 has segment, cache is empty
	ctx := context.Background()

	msgs, _, err := f.Fetch(ctx, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch() error: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected messages from S3")
	}

	// Second fetch should come from cache
	msgs2, _, err := f.Fetch(ctx, "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("second Fetch() error: %v", err)
	}
	if len(msgs2) != len(msgs) {
		t.Error("cached fetch returned different count")
	}
}

func TestFetcher_OffsetBeyondAvailable(t *testing.T) {
	f := newTestFetcher(t)
	ctx := context.Background()

	msgs, _, err := f.Fetch(ctx, "test-topic", 0, 99999, 10)
	if err != nil {
		t.Fatalf("Fetch() error: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for future offset, got %d", len(msgs))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/consumer/ -v`
Expected: FAIL

- [ ] **Step 3: Implement fetcher**

Create `internal/consumer/fetcher.go` with:
- `Fetcher` struct: s3Client, diskCache, partition indexes (map), in-memory buffers reference (for owner reads)
- `NewFetcher(s3 *storage.S3Client, cache *log.DiskCache) *Fetcher`
- `Fetch(ctx, topic, partitionID, startOffset, limit) ([]Message, nextOffset uint64, error)`:
  1. Check if this instance has an in-memory buffer with data >= startOffset → serve from buffer
  2. Load partition index from cache (or fetch from S3)
  3. Lookup segment ref for startOffset
  4. Try disk cache for segment
  5. Fetch from S3, cache to disk
  6. Deserialize segment, return messages from startOffset up to limit
  7. Return nextOffset = last returned offset + 1

- [ ] **Step 4: Implement consume handlers**

Create `internal/server/handlers_consume.go`:
- `handleConsumeLowLevel` — `GET /v1/topics/{topic}/partitions/{id}/messages?offset=N&limit=100`
  - Parses query params, calls fetcher.Fetch, returns JSON array of messages + next_offset
- `handleConsumeHighLevel` — `GET /v1/topics/{topic}/consume?group={g}`
  - Placeholder for now — will be implemented with consumer groups in Task 17

Register routes in `routes.go`.

- [ ] **Step 5: Write integration tests**

Create `test/integration/consume_test.go`:

```go
//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestProduceAndConsume(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("pc-test", 1, 24*time.Hour)

	// Produce
	client.Produce("pc-test", []camutest.ProduceMessage{
		{Key: "k1", Value: "hello"},
		{Key: "k1", Value: "world"},
	})

	// Wait for flush to S3
	time.Sleep(6 * time.Second)

	// Consume
	resp, err := client.Consume("pc-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(resp.Messages))
	}
	if resp.Messages[0].Value != "hello" {
		t.Errorf("first message = %q, want %q", resp.Messages[0].Value, "hello")
	}
	if resp.NextOffset != 2 {
		t.Errorf("NextOffset = %d, want 2", resp.NextOffset)
	}
}

func TestConsumeFromOffset(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("offset-test", 1, 24*time.Hour)

	for i := 0; i < 5; i++ {
		client.Produce("offset-test", []camutest.ProduceMessage{
			{Key: "k", Value: "msg"},
		})
	}

	time.Sleep(6 * time.Second)

	resp, err := client.Consume("offset-test", 0, 3, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("expected 2 messages from offset 3, got %d", len(resp.Messages))
	}
	if resp.Messages[0].Offset != 3 {
		t.Errorf("first message offset = %d, want 3", resp.Messages[0].Offset)
	}
}

func TestConsumeEmptyTopic(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("empty-test", 1, 24*time.Hour)

	resp, err := client.Consume("empty-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(resp.Messages))
	}
}
```

- [ ] **Step 6: Run integration tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/integration/ -v -run TestProduce -run TestConsume -timeout 60s
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/consumer/ internal/server/handlers_consume.go internal/server/routes.go test/integration/consume_test.go
git commit -m "feat: consumer fetcher with tiered read path and consume handlers"
```

---

### Task 14: SSE Streaming

**Files:**
- Create: `internal/consumer/sse.go`
- Create: `internal/consumer/sse_test.go`
- Update: `internal/server/handlers_consume.go`
- Update: `test/integration/consume_test.go`

- [ ] **Step 1: Write SSE tests**

Create `internal/consumer/sse_test.go` — unit test that verifies SSE event formatting:

```go
package consumer

import (
	"bytes"
	"testing"

	"github.com/maksim/camu/internal/log"
)

func TestFormatSSEEvent(t *testing.T) {
	msg := log.Message{
		Offset:    42,
		Timestamp: 1000,
		Key:       []byte("k1"),
		Value:     []byte("hello"),
	}

	var buf bytes.Buffer
	err := WriteSSEEvent(&buf, msg)
	if err != nil {
		t.Fatalf("WriteSSEEvent() error: %v", err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("id: 42")) {
		t.Error("SSE event missing id field")
	}
	if !bytes.Contains([]byte(output), []byte("data: ")) {
		t.Error("SSE event missing data field")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/consumer/ -v -run TestFormatSSE`
Expected: FAIL

- [ ] **Step 3: Implement SSE**

Create `internal/consumer/sse.go` with:
- `WriteSSEEvent(w io.Writer, msg Message) error` — formats as `id: {offset}\ndata: {json}\n\n`
- `StreamSSE(ctx, w http.ResponseWriter, flusher http.Flusher, fetcher, topic, partition, startOffset)` — loops: fetch messages, write SSE events, flush, if no new messages sleep briefly and retry. Respects context cancellation.

- [ ] **Step 4: Add SSE handler**

Update `internal/server/handlers_consume.go`:
- `handleStreamLowLevel` — `GET /v1/topics/{topic}/partitions/{id}/stream?offset=N`
  - Sets `Content-Type: text/event-stream`, `Cache-Control: no-cache`
  - Calls `consumer.StreamSSE()`
  - Supports `Last-Event-ID` header for reconnection

Register route: `mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/stream", s.handleStreamLowLevel)`

- [ ] **Step 5: Add SSE integration test**

Add to `test/integration/consume_test.go`:

```go
func TestSSEStreaming(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("sse-test", 1, 24*time.Hour)

	// Produce messages
	client.Produce("sse-test", []camutest.ProduceMessage{
		{Key: "k1", Value: "msg1"},
		{Key: "k1", Value: "msg2"},
	})

	time.Sleep(6 * time.Second)

	// Read via SSE
	events, err := client.StreamSSE("sse-test", 0, 0, 2, 5*time.Second)
	if err != nil {
		t.Fatalf("StreamSSE() error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 SSE events, got %d", len(events))
	}
}
```

Add `StreamSSE` method to `pkg/camutest/client.go` that opens an SSE connection, reads N events or times out.

- [ ] **Step 6: Run integration tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/integration/ -v -run TestSSE -timeout 60s
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/consumer/sse.go internal/consumer/sse_test.go internal/server/handlers_consume.go internal/server/routes.go test/integration/consume_test.go pkg/camutest/client.go
git commit -m "feat: SSE streaming for consumers"
```

---

### Task 15: Coordination — Leases and Partition Ownership

This task replaces the single-instance "own everything" model from Task 12 with S3-based lease coordination. After this task, `PartitionManager.IsOwned()` checks actual lease state, and the server acquires/renews/releases leases.

**Files:**
- Create: `internal/coordination/lease.go`
- Create: `internal/coordination/lease_test.go`
- Create: `internal/coordination/registry.go`
- Create: `internal/coordination/rebalancer.go`
- Create: `internal/coordination/rebalancer_test.go`
- Modify: `internal/server/partition_manager.go` — integrate lease checks into `IsOwned()`, add lease acquisition on startup, WAL discard on epoch mismatch

- [ ] **Step 1: Write lease tests**

Create `internal/coordination/lease_test.go`:

```go
package coordination

import (
	"context"
	"testing"
	"time"
)

func TestLease_AcquireAndRenew(t *testing.T) {
	store := newTestLeaseStore(t) // backed by in-memory S3
	ctx := context.Background()

	lease, err := store.Acquire(ctx, "topic1", 0, "instance-a", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire() error: %v", err)
	}
	if lease.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1", lease.Epoch)
	}

	err = store.Renew(ctx, lease)
	if err != nil {
		t.Fatalf("Renew() error: %v", err)
	}
}

func TestLease_ConflictingAcquire(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	store.Acquire(ctx, "topic1", 0, "instance-a", 5*time.Second)

	// Another instance tries to acquire — should fail (lease not expired)
	_, err := store.Acquire(ctx, "topic1", 0, "instance-b", 5*time.Second)
	if err == nil {
		t.Fatal("second Acquire() should fail while lease is active")
	}
}

func TestLease_AcquireExpired(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	store.Acquire(ctx, "topic1", 0, "instance-a", 1*time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	// Lease expired — another instance can acquire
	lease, err := store.Acquire(ctx, "topic1", 0, "instance-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire() expired lease error: %v", err)
	}
	if lease.Epoch != 2 {
		t.Errorf("Epoch = %d, want 2", lease.Epoch)
	}
	if lease.InstanceID != "instance-b" {
		t.Errorf("InstanceID = %q, want %q", lease.InstanceID, "instance-b")
	}
}

func TestLease_Release(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	lease, _ := store.Acquire(ctx, "topic1", 0, "instance-a", 5*time.Second)
	err := store.Release(ctx, lease)
	if err != nil {
		t.Fatalf("Release() error: %v", err)
	}

	// Should be acquirable now
	lease2, err := store.Acquire(ctx, "topic1", 0, "instance-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire() after Release error: %v", err)
	}
	if lease2.Epoch != 2 {
		t.Errorf("Epoch = %d, want 2", lease2.Epoch)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/coordination/ -v`
Expected: FAIL

- [ ] **Step 3: Implement lease store**

Create `internal/coordination/lease.go` with:
- `Lease` struct: Topic, PartitionID, InstanceID, Epoch (uint64), ExpiresAt (time.Time), ETag (string)
- `LeaseStore` struct: s3Client
- `NewLeaseStore(s3 *storage.S3Client) *LeaseStore`
- `Acquire(ctx, topic, partitionID, instanceID, ttl) (Lease, error)` — reads current lease from S3, checks if expired or absent, increments epoch, writes with conditional put
- `Renew(ctx, lease) error` — extends ExpiresAt, writes with conditional put using current etag
- `Release(ctx, lease) error` — deletes the lease object
- `Get(ctx, topic, partitionID) (Lease, error)` — reads current lease
- `ListForTopic(ctx, topic) ([]Lease, error)` — lists all partition leases for a topic

- [ ] **Step 4: Write rebalancer tests**

Create `internal/coordination/rebalancer_test.go`:

```go
package coordination

import (
	"testing"
)

func TestRebalancer_RoundRobin(t *testing.T) {
	assignments := Assign([]string{"i1", "i2", "i3"}, 9)

	// 9 partitions, 3 instances = 3 each
	if len(assignments["i1"]) != 3 {
		t.Errorf("i1 got %d partitions, want 3", len(assignments["i1"]))
	}
	if len(assignments["i2"]) != 3 {
		t.Errorf("i2 got %d partitions, want 3", len(assignments["i2"]))
	}
}

func TestRebalancer_UnevenDistribution(t *testing.T) {
	assignments := Assign([]string{"i1", "i2"}, 5)

	total := len(assignments["i1"]) + len(assignments["i2"])
	if total != 5 {
		t.Errorf("total partitions = %d, want 5", total)
	}
	// One gets 3, other gets 2
	if len(assignments["i1"]) < 2 || len(assignments["i1"]) > 3 {
		t.Errorf("i1 got %d partitions, expected 2 or 3", len(assignments["i1"]))
	}
}

func TestRebalancer_SingleInstance(t *testing.T) {
	assignments := Assign([]string{"i1"}, 4)
	if len(assignments["i1"]) != 4 {
		t.Errorf("single instance got %d partitions, want 4", len(assignments["i1"]))
	}
}
```

- [ ] **Step 5: Implement rebalancer and registry**

Create `internal/coordination/rebalancer.go`:
- `Assign(instances []string, numPartitions int) map[string][]int` — round-robin assignment

Create `internal/coordination/registry.go`:
- `Registry` struct: s3Client, instanceID, address
- `NewRegistry(s3, instanceID, address) *Registry`
- Instance discovery via listing lease objects — instances that hold active leases are considered alive

- [ ] **Step 6: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/coordination/ -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/coordination/
git commit -m "feat: S3-based lease coordination with epoch fencing and round-robin rebalancer"
```

---

### Task 16: Multi-Instance Routing and Integration Tests

**Files:**
- Update: `internal/server/server.go`
- Update: `internal/server/handlers_cluster.go`
- Create: `test/integration/multiinstance_test.go`

- [ ] **Step 1: Add routing endpoint**

Update `internal/server/handlers_cluster.go`:
- `handleRouting` — `GET /v1/topics/{topic}/routing` — returns partition → instance mapping with `Cache-Control: max-age=10`
- Update `handleClusterStatus` to include partition assignments

Register route: `mux.HandleFunc("GET /v1/topics/{topic}/routing", s.handleRouting)`

- [ ] **Step 2: Integrate coordination into server**

Update `internal/server/server.go`:
- On startup: initialize lease store, acquire leases for available partitions
- Background goroutine: renew leases on heartbeat interval
- On produce: check ownership, return 421 with routing map if not owner
- On shutdown: flush buffers, release leases

- [ ] **Step 3: Write multi-instance integration tests**

Create `test/integration/multiinstance_test.go`:

```go
//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestMultiInstance_PartitionDistribution(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(3), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("multi-test", 6, 24*time.Hour)
	time.Sleep(3 * time.Second) // wait for lease acquisition

	routing, err := client.GetRouting("multi-test")
	if err != nil {
		t.Fatalf("GetRouting() error: %v", err)
	}

	// Each of 3 instances should own ~2 partitions
	instanceCounts := make(map[string]int)
	for _, info := range routing.Partitions {
		instanceCounts[info.InstanceID]++
	}
	if len(instanceCounts) != 3 {
		t.Errorf("expected 3 instances in routing, got %d", len(instanceCounts))
	}
}

func TestMultiInstance_MisdirectedWrite(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(2), camutest.WithMinIO())
	defer env.Cleanup()

	client := env.Client()
	client.CreateTopic("misdirect-test", 2, 24*time.Hour)
	time.Sleep(3 * time.Second)

	// Get routing to find which instance owns which partition
	routing, _ := client.GetRouting("misdirect-test")

	// Find a partition NOT owned by instance 0
	var targetPartition int
	for p, info := range routing.Partitions {
		if info.Address != env.InstanceAddress(0) {
			targetPartition = p
			break
		}
	}

	// Produce to wrong instance — should get 421 with routing map
	resp, err := env.ClientFor(0).ProduceToPartition("misdirect-test", targetPartition, []camutest.ProduceMessage{{Value: "test"}})
	if err == nil && resp.StatusCode != 421 {
		t.Errorf("expected 421 misdirected, got %d", resp.StatusCode)
	}
}

func TestMultiInstance_ProduceConsumeAcrossInstances(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(2), camutest.WithMinIO())
	defer env.Cleanup()

	client := env.Client()
	client.CreateTopic("cross-test", 4, 24*time.Hour)
	time.Sleep(3 * time.Second)

	// Produce 20 messages with different keys
	for i := 0; i < 20; i++ {
		client.Produce("cross-test", []camutest.ProduceMessage{
			{Key: string(rune('a' + i%4)), Value: "msg"},
		})
	}

	time.Sleep(6 * time.Second) // wait for flush

	// Consume from all partitions via any instance
	total := 0
	for p := 0; p < 4; p++ {
		resp, err := env.ClientFor(1).Consume("cross-test", p, 0, 100)
		if err != nil {
			t.Fatalf("Consume(partition=%d) error: %v", p, err)
		}
		total += len(resp.Messages)
	}

	if total != 20 {
		t.Errorf("total consumed = %d, want 20", total)
	}
}
```

- [ ] **Step 4: Add routing client method to camutest**

Add to `pkg/camutest/client.go`:
- `GetRouting(topic string) (*RoutingResponse, error)` — `GET /v1/topics/{topic}/routing`

- [ ] **Step 5: Run integration tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/integration/ -v -run TestMultiInstance -timeout 120s
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/server/ test/integration/multiinstance_test.go pkg/camutest/client.go
git commit -m "feat: multi-instance coordination with routing API and lease management"
```

---

### Task 17: Consumer Groups

**Files:**
- Create: `internal/consumer/group.go`
- Create: `internal/consumer/group_test.go`
- Create: `internal/server/handlers_group.go`
- Create: `internal/storage/offsets.go`
- Create: `test/integration/group_test.go`

- [ ] **Step 1: Write offset storage tests**

Create `internal/storage/offsets_test.go`:

```go
package storage

import (
	"context"
	"testing"
)

func TestOffsetStore_CommitAndGet(t *testing.T) {
	store := newTestOffsetStore(t)
	ctx := context.Background()

	offsets := map[int]uint64{0: 100, 1: 200}
	err := store.CommitGroup(ctx, "group1", "topic1", offsets)
	if err != nil {
		t.Fatalf("CommitGroup() error: %v", err)
	}

	got, err := store.GetGroup(ctx, "group1", "topic1")
	if err != nil {
		t.Fatalf("GetGroup() error: %v", err)
	}
	if got[0] != 100 || got[1] != 200 {
		t.Errorf("offsets = %v, want {0:100, 1:200}", got)
	}
}

func TestOffsetStore_ConsumerSpecific(t *testing.T) {
	store := newTestOffsetStore(t)
	ctx := context.Background()

	offsets := map[int]uint64{0: 50}
	store.CommitConsumer(ctx, "consumer1", "topic1", offsets)

	got, err := store.GetConsumer(ctx, "consumer1", "topic1")
	if err != nil {
		t.Fatalf("GetConsumer() error: %v", err)
	}
	if got[0] != 50 {
		t.Errorf("offset = %d, want 50", got[0])
	}
}
```

- [ ] **Step 2: Implement offset storage**

Create `internal/storage/offsets.go`:
- `OffsetStore` struct: s3Client
- `NewOffsetStore(s3 *S3Client) *OffsetStore`
- `CommitGroup(ctx, groupID, topic, offsets map[int]uint64) error` — stores at `_coordination/groups/{groupID}/offsets.json`
- `GetGroup(ctx, groupID, topic) (map[int]uint64, error)`
- `CommitConsumer(ctx, consumerID, topic, offsets) error` — stores at `_coordination/consumers/{consumerID}/offsets.json`
- `GetConsumer(ctx, consumerID, topic) (map[int]uint64, error)`

- [ ] **Step 3: Write consumer group tests**

Create `internal/consumer/group_test.go`:

```go
package consumer

import (
	"testing"
)

func TestGroupCoordinator_JoinAndAssign(t *testing.T) {
	gc := NewGroupCoordinator()

	assignment1, err := gc.Join("group1", "topic1", "consumer-a", 4)
	if err != nil {
		t.Fatalf("Join() error: %v", err)
	}
	if len(assignment1) != 4 {
		t.Errorf("single consumer should get all 4 partitions, got %d", len(assignment1))
	}

	assignment2, err := gc.Join("group1", "topic1", "consumer-b", 4)
	if err != nil {
		t.Fatalf("Join() error: %v", err)
	}

	// After rebalance, each should get ~2 partitions
	total := len(assignment1) + len(assignment2)
	// Re-fetch assignment1 since rebalance happened
	assignment1 = gc.GetAssignment("group1", "consumer-a")
	assignment2 = gc.GetAssignment("group1", "consumer-b")
	total = len(assignment1) + len(assignment2)
	if total != 4 {
		t.Errorf("total assigned = %d, want 4", total)
	}
}

func TestGroupCoordinator_Leave(t *testing.T) {
	gc := NewGroupCoordinator()

	gc.Join("group1", "topic1", "consumer-a", 4)
	gc.Join("group1", "topic1", "consumer-b", 4)
	gc.Leave("group1", "consumer-a")

	assignment := gc.GetAssignment("group1", "consumer-b")
	if len(assignment) != 4 {
		t.Errorf("after leave, remaining consumer should get all 4, got %d", len(assignment))
	}
}
```

- [ ] **Step 4: Implement consumer group coordinator**

Create `internal/consumer/group.go`:
- `GroupCoordinator` struct: groups map (groupID → group state)
- Group state: topic, members map (consumerID → memberInfo), assignments map, heartbeat deadlines
- `Join(groupID, topic, consumerID, numPartitions) ([]int, error)` — adds member, triggers rebalance, returns assigned partitions
- `Leave(groupID, consumerID)` — removes member, triggers rebalance
- `Heartbeat(groupID, consumerID) error` — updates last heartbeat time
- `GetAssignment(groupID, consumerID) []int`
- `CheckExpired(timeout time.Duration) []expiredMember` — finds members past heartbeat deadline
- Internal rebalance: round-robin assignment of partitions to members

- [ ] **Step 5: Implement group handlers**

Create `internal/server/handlers_group.go`:
- `handleJoinGroup` — `POST /v1/groups/{group_id}/join` — body: `{"topic": "...", "consumer_id": "..."}`
- `handleHeartbeat` — `POST /v1/groups/{group_id}/heartbeat` — body: `{"consumer_id": "..."}`
- `handleCommitOffsets` — `POST /v1/groups/{group_id}/commit` — body: `{"consumer_id": "...", "offsets": {0: 100}}`
- `handleGetOffsets` — `GET /v1/groups/{group_id}/offsets`
- `handleLeaveGroup` — `POST /v1/groups/{group_id}/leave` — body: `{"consumer_id": "..."}`
- `handleConsumeWithGroup` — wire up `GET /v1/topics/{topic}/consume?group={g}` to fetch from assigned partitions
- `handleStreamWithGroup` — wire up `GET /v1/topics/{topic}/stream?group={g}` to SSE stream from assigned partitions (uses the SSE infrastructure from Task 14, iterates over assigned partitions)
- `handleCommitConsumerOffsets` — `POST /v1/topics/{topic}/offsets/{consumer_id}`
- `handleGetConsumerOffsets` — `GET /v1/topics/{topic}/offsets/{consumer_id}`

Register all routes.

- [ ] **Step 6: Write integration tests**

Create `test/integration/group_test.go`:

```go
//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestConsumerGroup_JoinAndConsume(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("group-test", 2, 24*time.Hour)

	// Produce messages
	for i := 0; i < 10; i++ {
		client.Produce("group-test", []camutest.ProduceMessage{
			{Key: "k", Value: "msg"},
		})
	}
	time.Sleep(6 * time.Second)

	// Join group
	join, err := client.JoinGroup("test-group", "group-test", "consumer-1")
	if err != nil {
		t.Fatalf("JoinGroup() error: %v", err)
	}
	if len(join.Partitions) == 0 {
		t.Fatal("expected partition assignments")
	}

	// Consume with group
	resp, err := client.ConsumeWithGroup("group-test", "test-group")
	if err != nil {
		t.Fatalf("ConsumeWithGroup() error: %v", err)
	}
	if len(resp.Messages) == 0 {
		t.Error("expected messages")
	}
}

func TestConsumerGroup_CommitAndGetOffsets(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("offset-group-test", 2, 24*time.Hour)
	client.JoinGroup("offset-group", "offset-group-test", "c1")

	err := client.CommitOffsets("offset-group", map[int]uint64{0: 50, 1: 30})
	if err != nil {
		t.Fatalf("CommitOffsets() error: %v", err)
	}

	offsets, err := client.GetOffsets("offset-group")
	if err != nil {
		t.Fatalf("GetOffsets() error: %v", err)
	}
	if offsets[0] != 50 {
		t.Errorf("partition 0 offset = %d, want 50", offsets[0])
	}
}

func TestConsumerSpecificOffsets(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("standalone-test", 1, 24*time.Hour)

	err := client.CommitConsumerOffsets("standalone-test", "my-consumer", map[int]uint64{0: 42})
	if err != nil {
		t.Fatalf("CommitConsumerOffsets() error: %v", err)
	}

	offsets, err := client.GetConsumerOffsets("standalone-test", "my-consumer")
	if err != nil {
		t.Fatalf("GetConsumerOffsets() error: %v", err)
	}
	if offsets[0] != 42 {
		t.Errorf("offset = %d, want 42", offsets[0])
	}
}
```

- [ ] **Step 7: Run integration tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/integration/ -v -run "TestConsumerGroup|TestConsumerSpecific" -timeout 120s
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add internal/consumer/group.go internal/consumer/group_test.go internal/server/handlers_group.go internal/storage/offsets.go internal/storage/offsets_test.go internal/server/routes.go test/integration/group_test.go pkg/camutest/client.go
git commit -m "feat: consumer groups with offset tracking and standalone consumer offsets"
```

---

### Task 18: Retention Cleanup

**Files:**
- Update: `internal/log/partition.go`
- Create: `internal/log/retention.go`
- Create: `internal/log/retention_test.go`

- [ ] **Step 1: Write retention tests**

Create `internal/log/retention_test.go`:

```go
package log

import (
	"testing"
	"time"
)

func TestRetention_RemovesExpiredSegments(t *testing.T) {
	idx := NewIndex()
	now := time.Now()

	// Old segment
	idx.Add(SegmentRef{
		BaseOffset: 0, EndOffset: 99, Epoch: 1,
		Key: "topic/0/0-1.segment",
		CreatedAt: now.Add(-48 * time.Hour),
	})
	// Recent segment
	idx.Add(SegmentRef{
		BaseOffset: 100, EndOffset: 199, Epoch: 1,
		Key: "topic/0/100-1.segment",
		CreatedAt: now.Add(-1 * time.Hour),
	})

	expired := idx.RemoveExpired(24 * time.Hour)
	if len(expired) != 1 {
		t.Fatalf("RemoveExpired() removed %d, want 1", len(expired))
	}
	if expired[0].Key != "topic/0/0-1.segment" {
		t.Errorf("removed wrong segment: %q", expired[0].Key)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestRetention`
Expected: FAIL

- [ ] **Step 3: Implement retention**

Update `SegmentRef` to include `CreatedAt time.Time`.

Add `RemoveExpired(retention time.Duration) []SegmentRef` to Index — removes segments where `CreatedAt + retention < now`.

Create `internal/log/retention.go`:
- `RetentionCleaner` struct: s3Client, leaseStore, interval, retention
- `NewRetentionCleaner(s3, interval, retention) *RetentionCleaner`
- `Start(ctx)` — background goroutine: for each owned partition, load index, call RemoveExpired, update index (conditional put), delete S3 objects for removed segments
- `Stop()`

- [ ] **Step 4: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestRetention`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/log/retention.go internal/log/retention_test.go internal/log/index.go
git commit -m "feat: time-based retention cleanup with index-first deletion"
```

---

### Task 19: Backpressure and Graceful Shutdown

**Files:**
- Update: `internal/server/server.go`
- Update: `internal/producer/batcher.go`

- [ ] **Step 1: Implement backpressure**

Update batcher to track total buffer size across all partitions. When total exceeds a configurable high-water mark, `Append` returns `ErrBackpressure`.

Update produce handlers: if `ErrBackpressure`, return `503 Service Unavailable` with `Retry-After: 1` header.

- [ ] **Step 2: Implement graceful shutdown**

Update `internal/server/server.go` Shutdown:
1. Stop accepting new writes (set a flag checked by produce handlers)
2. Flush all batcher buffers — call `batcher.Stop()` which flushes remaining WAL entries to S3
3. Release all leases — call `leaseStore.Release()` for each owned partition
4. Shutdown HTTP server with context deadline

- [ ] **Step 3: Commit**

```bash
git add internal/server/server.go internal/producer/batcher.go
git commit -m "feat: backpressure (503) and graceful shutdown with flush and lease release"
```

---

### Task 20: CLI — Serve and Test Commands

**Files:**
- Update: `cmd/camu/main.go`

- [ ] **Step 1: Implement serve command**

Update `cmd/camu/main.go`:
- `serve` subcommand: `--config` flag (default `camu.yaml`), loads config, creates and starts server, handles SIGINT/SIGTERM for graceful shutdown

- [ ] **Step 2: Implement test command**

- `test` subcommand: `--endpoint` flag (for remote), `--instances` (default 1), `--category` (filter), `--bench`, `--chaos`, `--duration`, `--auth-token`
- When no `--endpoint`: starts MinIO + N instances locally, runs integration tests via `go test`
- When `--endpoint` provided: runs tests against remote endpoint
- Maps flags to `go test` build tags and env vars

- [ ] **Step 3: Verify end-to-end**

```bash
cd /Users/maksim/Projects/camu && go build ./cmd/camu/
./camu serve --config camu.yaml.example &
# (would need real S3/MinIO to actually work — just verify it starts and responds to health check)
```

- [ ] **Step 4: Commit**

```bash
git add cmd/camu/main.go
git commit -m "feat: CLI with serve and test subcommands"
```

---

### Task 21: Durability Integration Tests

**Files:**
- Create: `test/integration/durability_test.go`

- [ ] **Step 1: Write durability tests**

Create `test/integration/durability_test.go`:

```go
//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestDurability_WALReplay(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("wal-test", 1, 24*time.Hour)

	// Produce messages
	client.Produce("wal-test", []camutest.ProduceMessage{
		{Key: "k1", Value: "before-crash-1"},
		{Key: "k1", Value: "before-crash-2"},
	})

	// Kill instance without graceful shutdown (skips flush)
	env.KillInstance(0)

	// Restart instance — WAL should replay
	env.RestartInstance(0)

	// Wait for WAL replay + flush
	time.Sleep(6 * time.Second)

	resp, err := client.Consume("wal-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Errorf("expected 2 messages after WAL replay, got %d", len(resp.Messages))
	}
}

func TestDurability_LeaseEpochFencing(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(2), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("fence-test", 1, 24*time.Hour)
	time.Sleep(3 * time.Second) // wait for lease acquisition

	// Produce to the owner
	client.Produce("fence-test", []camutest.ProduceMessage{{Key: "k", Value: "v1"}})

	// Kill the owner
	routing, _ := client.GetRouting("fence-test")
	ownerIdx := env.InstanceIndex(routing.Partitions[0].InstanceID)
	env.KillInstance(ownerIdx)

	// Wait for lease expiry + acquisition by other instance
	time.Sleep(15 * time.Second)

	// Produce via the new owner
	client.Produce("fence-test", []camutest.ProduceMessage{{Key: "k", Value: "v2"}})
	time.Sleep(6 * time.Second)

	// Verify new messages are readable
	resp, err := client.Consume("fence-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) == 0 {
		t.Error("expected messages after failover")
	}
}
```

- [ ] **Step 2: Add KillInstance/RestartInstance to camutest**

Update `pkg/camutest/env.go`:
- `KillInstance(idx int)` — forcefully stops the instance (no graceful shutdown)
- `RestartInstance(idx int)` — starts a new instance with the same config
- `InstanceIndex(instanceID string) int` — finds instance by ID

- [ ] **Step 3: Run durability tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/integration/ -v -run TestDurability -timeout 120s
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add test/integration/durability_test.go pkg/camutest/env.go
git commit -m "test: durability tests for WAL replay and lease epoch fencing"
```

---

### Task 22: Benchmarks

**Files:**
- Create: `test/bench/throughput_test.go`

- [ ] **Step 1: Write benchmarks**

Create `test/bench/throughput_test.go`:

```go
//go:build integration

package bench

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func BenchmarkProduceThroughput(b *testing.B) {
	env := camutest.New(b, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("bench-produce", 4, 24*time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Produce("bench-produce", []camutest.ProduceMessage{
			{Key: fmt.Sprintf("key-%d", i), Value: "benchmark payload data"},
		})
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

func BenchmarkProduceBatchThroughput(b *testing.B) {
	env := camutest.New(b, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("bench-batch", 4, 24*time.Hour)

	batchSize := 100
	batch := make([]camutest.ProduceMessage, batchSize)
	for i := range batch {
		batch[i] = camutest.ProduceMessage{Key: fmt.Sprintf("k%d", i), Value: "payload"}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Produce("bench-batch", batch)
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "msgs/sec")
}

func BenchmarkConsumeThroughput(b *testing.B) {
	env := camutest.New(b, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("bench-consume", 1, 24*time.Hour)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		client.Produce("bench-consume", []camutest.ProduceMessage{
			{Key: "k", Value: "benchmark payload data"},
		})
	}
	time.Sleep(6 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Consume("bench-consume", 0, 0, 100)
	}
}

func BenchmarkParallelProduce(b *testing.B) {
	env := camutest.New(b, camutest.WithInstances(1), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("bench-parallel", 8, 24*time.Hour)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client.Produce("bench-parallel", []camutest.ProduceMessage{
				{Key: fmt.Sprintf("key-%d", i), Value: "payload"},
			})
			i++
		}
	})
}
```

- [ ] **Step 2: Run benchmarks**

```bash
cd /Users/maksim/Projects/camu && go test -tags integration ./test/bench/ -bench=. -benchtime=10s -timeout 300s
```

- [ ] **Step 3: Commit**

```bash
git add test/bench/
git commit -m "test: produce and consume throughput benchmarks"
```

---

### Task 23: Chaos Tests

**Files:**
- Create: `test/integration/chaos_test.go`

- [ ] **Step 1: Write chaos tests**

Create `test/integration/chaos_test.go`:

```go
//go:build integration && chaos

package integration

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestChaos_RandomKillsDuringProduce(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(3), camutest.WithMinIO())
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("chaos-test", 6, 24*time.Hour)
	time.Sleep(5 * time.Second)

	// Start continuous producing in background
	var wg sync.WaitGroup
	stop := make(chan struct{})
	produced := int64(0)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, err := client.Produce("chaos-test", []camutest.ProduceMessage{
					{Key: "k", Value: "chaos-msg"},
				})
				if err == nil {
					atomic.AddInt64(&produced, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Randomly kill and restart instances
	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second)
		victim := rand.Intn(3)
		t.Logf("killing instance %d", victim)
		env.KillInstance(victim)
		time.Sleep(3 * time.Second)
		t.Logf("restarting instance %d", victim)
		env.RestartInstance(victim)
	}

	close(stop)
	wg.Wait()

	t.Logf("produced %d messages during chaos", produced)

	// Wait for everything to settle
	time.Sleep(10 * time.Second)

	// Verify: all acknowledged messages should be consumable
	// (messages that got errors are not counted in 'produced')
	total := 0
	for p := 0; p < 6; p++ {
		resp, _ := client.Consume("chaos-test", p, 0, 10000)
		if resp != nil {
			total += len(resp.Messages)
		}
	}

	if int64(total) < produced {
		t.Errorf("consumed %d but produced %d — data loss detected", total, produced)
	}
}
```

- [ ] **Step 2: Run chaos tests**

```bash
cd /Users/maksim/Projects/camu && go test -tags "integration chaos" ./test/integration/ -v -run TestChaos -timeout 300s
```

- [ ] **Step 3: Commit**

```bash
git add test/integration/chaos_test.go
git commit -m "test: chaos tests with random instance kills during produce"
```

---

### Task 24: Segment Compression

**Files:**
- Update: `internal/log/segment.go`
- Update: `internal/log/segment_test.go`

- [ ] **Step 1: Add compression tests**

Add to `internal/log/segment_test.go`:

```go
func TestSegmentRoundTrip_Snappy(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k"), Value: []byte("hello world")},
	}

	var buf bytes.Buffer
	err := WriteSegment(&buf, msgs, CompressionSnappy)
	if err != nil {
		t.Fatalf("WriteSegment(snappy) error: %v", err)
	}

	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment(snappy) error: %v", err)
	}
	if string(got[0].Value) != "hello world" {
		t.Errorf("value = %q, want %q", string(got[0].Value), "hello world")
	}
}

func TestSegmentRoundTrip_Zstd(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: []byte("k"), Value: []byte("hello world")},
	}

	var buf bytes.Buffer
	err := WriteSegment(&buf, msgs, CompressionZstd)
	if err != nil {
		t.Fatalf("WriteSegment(zstd) error: %v", err)
	}

	got, err := ReadSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("ReadSegment(zstd) error: %v", err)
	}
	if string(got[0].Value) != "hello world" {
		t.Errorf("value = %q, want %q", string(got[0].Value), "hello world")
	}
}
```

- [ ] **Step 2: Implement compression**

Update `internal/log/segment.go`:
- Add a 1-byte compression flag after the version byte in the segment header
- `WriteSegment`: serialize message frames to a buffer, then compress the entire payload (snappy or zstd) before writing
- `ReadSegment`: read compression flag, decompress payload, then parse message frames
- Add dependencies: `go get github.com/klauspost/compress`

- [ ] **Step 3: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestSegment`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/log/segment.go internal/log/segment_test.go go.mod go.sum
git commit -m "feat: snappy and zstd segment compression"
```

---

### Task 25: Orphan Segment Garbage Collector

**Files:**
- Create: `internal/log/gc.go`
- Create: `internal/log/gc_test.go`

- [ ] **Step 1: Write GC tests**

Create `internal/log/gc_test.go`:

```go
package log

import (
	"context"
	"testing"
)

func TestGC_RemovesOrphanedSegments(t *testing.T) {
	gc := newTestGC(t)
	ctx := context.Background()

	// Index only references segment "0-1.segment"
	// But S3 also has "100-1.segment" (orphaned — index update failed)
	orphans, err := gc.FindOrphans(ctx, "topic1", 0)
	if err != nil {
		t.Fatalf("FindOrphans() error: %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("expected 1 orphan, got %d", len(orphans))
	}

	err = gc.CleanOrphans(ctx, "topic1", 0)
	if err != nil {
		t.Fatalf("CleanOrphans() error: %v", err)
	}
}
```

- [ ] **Step 2: Implement GC**

Create `internal/log/gc.go`:
- `GarbageCollector` struct: s3Client
- `FindOrphans(ctx, topic, partitionID) ([]string, error)` — lists all `.segment` objects for the partition, compares against index entries, returns keys not in the index
- `CleanOrphans(ctx, topic, partitionID) error` — deletes orphaned segments
- `StartBackground(ctx, interval)` — runs periodically

- [ ] **Step 3: Run tests**

Run: `cd /Users/maksim/Projects/camu && go test ./internal/log/ -v -run TestGC`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/log/gc.go internal/log/gc_test.go
git commit -m "feat: orphan segment garbage collector"
```

---

### Task 26: Jepsen Verification

**Post-MVP:** This task requires Tasks 1-20 to be complete (a fully functional multi-instance camu cluster). Run Jepsen after the core system is working end-to-end. Jepsen validates correctness under faults — it is not needed for initial development but is critical before any production use.

**Prerequisites:** JVM 11+, Leiningen, Docker

**Files:**
- Create: `jepsen/camu/project.clj`
- Create: `jepsen/camu/src/jepsen/camu.clj`
- Create: `jepsen/camu/src/jepsen/camu/client.clj`
- Create: `jepsen/camu/src/jepsen/camu/nemesis.clj`
- Create: `jepsen/camu/src/jepsen/camu/checker.clj`
- Create: `jepsen/camu/src/jepsen/camu/db.clj`
- Create: `jepsen/camu/README.md`
- Create: `jepsen/camu/docker-compose.yml`

This task sets up a full Jepsen test suite for camu. Jepsen runs concurrent operations against a camu cluster while injecting faults, records a complete operation history, and verifies consistency invariants.

**Prerequisites:** JVM, Leiningen, Docker (for test nodes)

- [ ] **Step 1: Set up Jepsen project**

Create `jepsen/camu/project.clj`:

```clojure
(defproject jepsen.camu "0.1.0"
  :description "Jepsen tests for Camu commit log"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.12.0"]]
  :main jepsen.camu
  :jvm-opts ["-Xmx4g"])
```

- [ ] **Step 2: Implement db (cluster lifecycle)**

Create `jepsen/camu/src/jepsen/camu/db.clj`:

```clojure
(ns jepsen.camu.db
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]
            [jepsen.control.util :as cu]))

(defn db
  "Manages camu lifecycle on a Jepsen node."
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (info "Installing camu on" node)
      ;; Upload camu binary (pre-built)
      (c/upload (:camu-binary opts) "/usr/local/bin/camu")
      (c/exec :chmod "+x" "/usr/local/bin/camu")
      ;; Create directories
      (c/exec :mkdir :-p "/var/lib/camu/wal")
      (c/exec :mkdir :-p "/var/lib/camu/cache")
      ;; Write config
      (let [config (str "server:\n"
                        "  address: \"0.0.0.0:8080\"\n"
                        "  instance_id: \"" node "\"\n"
                        "storage:\n"
                        "  bucket: \"" (:bucket opts) "\"\n"
                        "  region: \"us-east-1\"\n"
                        "  endpoint: \"" (:s3-endpoint opts) "\"\n"
                        "  credentials:\n"
                        "    access_key: \"minioadmin\"\n"
                        "    secret_key: \"minioadmin\"\n"
                        "wal:\n"
                        "  directory: \"/var/lib/camu/wal\"\n"
                        "  fsync: true\n"
                        "segments:\n"
                        "  max_size: 1048576\n"
                        "  max_age: \"2s\"\n"
                        "  compression: \"none\"\n"
                        "cache:\n"
                        "  directory: \"/var/lib/camu/cache\"\n"
                        "  max_size: 1073741824\n"
                        "coordination:\n"
                        "  lease_ttl: \"10s\"\n"
                        "  heartbeat_interval: \"3s\"\n"
                        "  rebalance_delay: \"5s\"\n")]
        (cu/write-file! config "/etc/camu.yaml"))
      ;; Start camu
      (cu/start-daemon!
        {:logfile "/var/log/camu.log"
         :pidfile "/var/run/camu.pid"
         :chdir   "/"}
        "/usr/local/bin/camu"
        :serve :--config "/etc/camu.yaml"))

    (teardown! [_ test node]
      (info "Tearing down camu on" node)
      (cu/stop-daemon! "/var/run/camu.pid")
      (c/exec :rm :-rf "/var/lib/camu"))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/camu.log"])))
```

- [ ] **Step 3: Implement client (camu HTTP operations)**

Create `jepsen/camu/src/jepsen/camu/client.clj`:

```clojure
(ns jepsen.camu.client
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]])
  (:import (java.net ConnectException SocketTimeoutException)))

(defn base-url [node]
  (str "http://" node ":8080"))

(defn produce!
  "Produce a message with the given key and value. Returns the assigned offset."
  [node topic key value]
  (let [resp (http/post (str (base-url node) "/v1/topics/" topic "/messages")
                        {:content-type :json
                         :body (json/generate-string
                                 [{:key key :value value}])
                         :socket-timeout 5000
                         :connection-timeout 5000
                         :throw-exceptions false})]
    (when (= 200 (:status resp))
      (-> resp :body (json/parse-string true) :offsets first))))

(defn consume!
  "Consume messages from a partition starting at offset."
  [node topic partition offset limit]
  (let [resp (http/get (str (base-url node) "/v1/topics/" topic
                            "/partitions/" partition
                            "/messages")
                       {:query-params {:offset offset :limit limit}
                        :socket-timeout 5000
                        :connection-timeout 5000
                        :throw-exceptions false})]
    (when (= 200 (:status resp))
      (-> resp :body (json/parse-string true)))))

(defrecord CamuClient [node topic]
  client/Client
  (open! [this test node]
    (assoc this :node node))

  (setup! [this test]
    ;; Create topic once
    (try
      (http/post (str (base-url node) "/v1/topics")
                 {:content-type :json
                  :body (json/generate-string
                          {:name topic :partitions 4 :retention "24h"})
                  :throw-exceptions false})
      (catch Exception _)))

  (invoke! [this test op]
    (try
      (case (:f op)
        :produce
        (let [result (produce! node topic
                               (str (:key (:value op)))
                               (str (:val (:value op))))]
          (if result
            (assoc op :type :ok :value (assoc (:value op)
                                              :offset (:offset result)
                                              :partition (:partition result)))
            (assoc op :type :fail :error :produce-failed)))

        :consume
        (let [partition (:partition (:value op))
              offset    (or (:offset (:value op)) 0)
              result    (consume! node topic partition offset 1000)]
          (if result
            (assoc op :type :ok :value (:messages result))
            (assoc op :type :fail :error :consume-failed))))

      (catch ConnectException _
        (assoc op :type :fail :error :connection-refused))
      (catch SocketTimeoutException _
        (assoc op :type :info :error :timeout))))

  (teardown! [this test])
  (close! [this test]))

(defn client
  "Creates a new CamuClient."
  [topic]
  (map->CamuClient {:topic topic}))
```

- [ ] **Step 4: Implement nemeses (fault injection)**

Create `jepsen/camu/src/jepsen/camu/nemesis.clj`:

```clojure
(ns jepsen.camu.nemesis
  (:require [jepsen [nemesis :as nemesis]
                    [generator :as gen]
                    [net :as net]]))

(defn nemesis-package
  "Returns a nemesis and generator for camu testing.
   Supports:
   - :kill    — SIGKILL random camu processes
   - :partition — network partitions between nodes
   - :pause   — SIGSTOP/SIGCONT camu processes"
  [opts]
  (let [faults (:faults opts #{:kill :partition})]
    {:nemesis
     (nemesis/compose
       (merge
         (when (:kill faults)
           {#{:kill} (nemesis/node-start-stopper
                       rand-nth
                       (fn start [test node]
                         (jepsen.control/exec :pkill :-9 :camu)
                         [:killed node])
                       (fn stop [test node]
                         (jepsen.control/exec
                           :bash :-c
                           "nohup /usr/local/bin/camu serve --config /etc/camu.yaml > /var/log/camu.log 2>&1 &")
                         [:restarted node]))})
         (when (:partition faults)
           {#{:partition} (nemesis/partition-random-halves)})
         (when (:pause faults)
           {#{:pause} (nemesis/hammer-time "camu")})))

     :generator
     (gen/phases
       ;; Let the cluster stabilize
       (gen/sleep 10)
       ;; Run faults interleaved with client operations
       (->> (gen/mix
              (concat
                (when (:kill faults)
                  [{:type :info :f :kill}])
                (when (:partition faults)
                  [{:type :info :f :partition}])
                (when (:pause faults)
                  [{:type :info :f :pause}])))
            (gen/stagger 5)
            (gen/time-limit (:time-limit opts 60)))
       ;; Recover
       (gen/nemesis (gen/once {:type :info :f :recover}))
       (gen/sleep 15))}))
```

- [ ] **Step 5: Implement checker (invariant verification)**

Create `jepsen/camu/src/jepsen/camu/checker.clj`:

```clojure
(ns jepsen.camu.checker
  (:require [jepsen [checker :as checker]]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]))

(defn no-data-loss-checker
  "Verifies that every acknowledged produce is eventually consumable.
   After all operations complete, consumes all partitions and checks
   that every :ok produce offset appears in the consumed data."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [;; All acknowledged produces
            acked-produces (->> history
                                (filter #(and (= :ok (:type %))
                                              (= :produce (:f %))))
                                (map :value))
            ;; Group by partition
            by-partition (group-by :partition acked-produces)
            ;; All successful consumes (the final drain read)
            consumed (->> history
                          (filter #(and (= :ok (:type %))
                                        (= :consume (:f %))))
                          (mapcat :value))
            consumed-offsets (set (map (fn [m] [(:partition m) (:offset m)]) consumed))
            ;; Check each acked produce exists in consumed
            missing (->> acked-produces
                         (filter #(not (consumed-offsets [(:partition %) (:offset %)]))))]
        {:valid? (empty? missing)
         :acked-count (count acked-produces)
         :consumed-count (count consumed)
         :missing-count (count missing)
         :missing (take 10 missing)}))))

(defn offset-monotonicity-checker
  "Verifies that offsets within each partition are strictly increasing
   with no gaps and no duplicates."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [consumed (->> history
                          (filter #(and (= :ok (:type %))
                                        (= :consume (:f %))))
                          (mapcat :value))
            by-partition (group-by :partition consumed)
            violations (for [[partition msgs] by-partition
                             :let [offsets (sort (map :offset msgs))
                                   dups (- (count offsets) (count (distinct offsets)))
                                   gaps (for [[a b] (partition 2 1 (distinct offsets))
                                              :when (> (- b a) 1)]
                                          {:from a :to b :gap (- b a 1)})]
                             :when (or (pos? dups) (seq gaps))]
                         {:partition partition
                          :duplicate-count dups
                          :gaps gaps})]
        {:valid? (empty? violations)
         :violations violations}))))

(defn no-split-brain-checker
  "Verifies that no two messages in the same partition have the same offset
   but different values (indicating two instances wrote to the same partition)."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [consumed (->> history
                          (filter #(and (= :ok (:type %))
                                        (= :consume (:f %))))
                          (mapcat :value))
            by-partition (group-by :partition consumed)
            conflicts (for [[partition msgs] by-partition
                            :let [by-offset (group-by :offset msgs)]
                            [offset offset-msgs] by-offset
                            :when (> (count (distinct (map :value offset-msgs))) 1)]
                        {:partition partition
                         :offset offset
                         :values (map :value offset-msgs)})]
        {:valid? (empty? conflicts)
         :conflict-count (count conflicts)
         :conflicts (take 10 conflicts)}))))
```

- [ ] **Step 6: Implement main test runner**

Create `jepsen/camu/src/jepsen/camu.clj`:

```clojure
(ns jepsen.camu
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.camu [client :as client]
                         [db :as db]
                         [nemesis :as nemesis]
                         [checker :as camu-checker]]))

(defn camu-test
  "Constructs a Jepsen test map for camu."
  [opts]
  (let [topic "jepsen-test"
        nem   (nemesis/nemesis-package
                {:faults (:faults opts #{:kill :partition})
                 :time-limit (:time-limit opts 60)})
        n     (atom 0)]
    (merge tests/noop-test
           opts
           {:name      "camu"
            :os        debian/os
            :db        (db/db {:camu-binary (:camu-binary opts "./camu")
                               :bucket      "jepsen-bucket"
                               :s3-endpoint (:s3-endpoint opts "http://minio:9000")})
            :client    (client/client topic)
            :nemesis   (:nemesis nem)
            :checker   (checker/compose
                         {:no-data-loss       (camu-checker/no-data-loss-checker)
                          :offset-monotonicity (camu-checker/offset-monotonicity-checker)
                          :no-split-brain     (camu-checker/no-split-brain-checker)
                          :timeline           (checker/timeline)
                          :stats              (checker/stats)
                          :perf               (checker/perf)})
            :generator (gen/phases
                         ;; Client operations + nemesis interleaved
                         (->> (gen/mix
                                [{:type :invoke :f :produce
                                  :value {:key (str "k-" (swap! n inc))
                                          :val (str "v-" @n)}}])
                              (gen/stagger 1/10)
                              (gen/nemesis (:generator nem))
                              (gen/time-limit (:time-limit opts 60)))
                         ;; Recovery
                         (gen/nemesis (gen/once {:type :info :f :recover}))
                         (gen/sleep 15)
                         ;; Final drain — consume all partitions
                         (gen/clients
                           (gen/each-thread
                             (gen/once
                               (fn [_ _]
                                 (for [p (range 4)]
                                   {:type :invoke :f :consume
                                    :value {:partition p :offset 0}}))))))})))

(defn -main
  "CLI entry point."
  [& args]
  (cli/run!
    (merge (cli/single-test-cmd {:test-fn camu-test})
           (cli/serve-cmd))
    args))
```

- [ ] **Step 7: Create Docker Compose for Jepsen nodes**

Create `jepsen/camu/docker-compose.yml`:

```yaml
version: "3.8"
services:
  control:
    image: jepsen-control
    build: .
    volumes:
      - ./:/jepsen/camu
      - ../../:/camu-src
    depends_on:
      - minio
      - n1
      - n2
      - n3
      - n4
      - n5

  minio:
    image: minio/minio
    command: server /data
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin

  n1:
    image: jepsen-node
    hostname: n1
    privileged: true

  n2:
    image: jepsen-node
    hostname: n2
    privileged: true

  n3:
    image: jepsen-node
    hostname: n3
    privileged: true

  n4:
    image: jepsen-node
    hostname: n4
    privileged: true

  n5:
    image: jepsen-node
    hostname: n5
    privileged: true
```

- [ ] **Step 8: Add Jepsen run script**

Create `jepsen/camu/run.sh`:

```bash
#!/bin/bash
set -e

# Build camu binary for Linux
echo "Building camu..."
cd ../../
GOOS=linux GOARCH=amd64 go build -o jepsen/camu/camu ./cmd/camu/
cd jepsen/camu

# Start Docker environment
echo "Starting Jepsen environment..."
docker-compose up -d

# Run Jepsen tests
echo "Running Jepsen tests..."
docker-compose exec control \
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --time-limit 120 \
    --s3-endpoint http://minio:9000 \
    --camu-binary /jepsen/camu/camu

echo "Results in jepsen/camu/store/latest/"
```

- [ ] **Step 9: Verify Jepsen project compiles**

```bash
cd /Users/maksim/Projects/camu/jepsen/camu && lein deps && lein compile
```

Expected: Compiles without errors

- [ ] **Step 10: Commit**

```bash
git add jepsen/
git commit -m "feat: Jepsen test suite for consistency verification"
```
