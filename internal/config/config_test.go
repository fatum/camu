package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
)

func TestLoadFromFile(t *testing.T) {
	content := `
server:
  address: ":9090"
  instance_id: "node-1"
storage:
  bucket: "my-bucket"
  region: "us-west-2"
  endpoint: "https://s3.example.com"
  credentials:
    access_key: "AKID"
    secret_key: "SECRET"
segments:
  max_size: 1048576
  max_age: "10s"
  compression: "snappy"
  record_batch_target_size: 32768
  index_interval_bytes: 8192
cache:
  directory: "/tmp/cache"
  max_size: 5368709120
sql:
  cache_directory: "/tmp/sql-cache"
  cache_max_size: 268435456
  duckdb_temp_directory: "/tmp/sql-tmp"
  duckdb_memory_limit: "512MB"
  max_concurrency: 8
coordination:
  lease_ttl: "20s"
  heartbeat_interval: "5s"
  rebalance_delay: "15s"
  instance_ttl: "12s"
  maintenance_max_concurrency: 7
`
	f, err := os.CreateTemp("", "camu-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	cfg, err := config.Load(f.Name())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Address != ":9090" {
		t.Errorf("Server.Address = %q, want %q", cfg.Server.Address, ":9090")
	}
	if cfg.Server.InstanceID != "node-1" {
		t.Errorf("Server.InstanceID = %q, want %q", cfg.Server.InstanceID, "node-1")
	}
	if cfg.Storage.Bucket != "my-bucket" {
		t.Errorf("Storage.Bucket = %q, want %q", cfg.Storage.Bucket, "my-bucket")
	}
	if cfg.Storage.Region != "us-west-2" {
		t.Errorf("Storage.Region = %q, want %q", cfg.Storage.Region, "us-west-2")
	}
	if cfg.Storage.Endpoint != "https://s3.example.com" {
		t.Errorf("Storage.Endpoint = %q, want %q", cfg.Storage.Endpoint, "https://s3.example.com")
	}
	if cfg.Storage.Credentials.AccessKey != "AKID" {
		t.Errorf("Storage.Credentials.AccessKey = %q, want %q", cfg.Storage.Credentials.AccessKey, "AKID")
	}
	if cfg.Storage.Credentials.SecretKey != "SECRET" {
		t.Errorf("Storage.Credentials.SecretKey = %q, want %q", cfg.Storage.Credentials.SecretKey, "SECRET")
	}
	if cfg.Segments.MaxSize != 1048576 {
		t.Errorf("Segments.MaxSize = %d, want %d", cfg.Segments.MaxSize, 1048576)
	}
	if cfg.Segments.MaxAge != "10s" {
		t.Errorf("Segments.MaxAge = %q, want %q", cfg.Segments.MaxAge, "10s")
	}
	if cfg.Segments.Compression != "snappy" {
		t.Errorf("Segments.Compression = %q, want %q", cfg.Segments.Compression, "snappy")
	}
	if cfg.Segments.RecordBatchTargetSize != 32768 {
		t.Errorf("Segments.RecordBatchTargetSize = %d, want %d", cfg.Segments.RecordBatchTargetSize, 32768)
	}
	if cfg.Segments.IndexIntervalBytes != 8192 {
		t.Errorf("Segments.IndexIntervalBytes = %d, want %d", cfg.Segments.IndexIntervalBytes, 8192)
	}
	if cfg.Cache.Directory != "/tmp/cache" {
		t.Errorf("Cache.Directory = %q, want %q", cfg.Cache.Directory, "/tmp/cache")
	}
	if cfg.Cache.MaxSize != 5368709120 {
		t.Errorf("Cache.MaxSize = %d, want %d", cfg.Cache.MaxSize, 5368709120)
	}
	if cfg.SQL.CacheDirectory != "/tmp/sql-cache" {
		t.Errorf("SQL.CacheDirectory = %q, want %q", cfg.SQL.CacheDirectory, "/tmp/sql-cache")
	}
	if cfg.SQL.CacheDirectoryValue() != "/tmp/sql-cache" {
		t.Errorf("SQL.CacheDirectoryValue() = %q, want %q", cfg.SQL.CacheDirectoryValue(), "/tmp/sql-cache")
	}
	if cfg.SQL.CacheMaxSize != 268435456 {
		t.Errorf("SQL.CacheMaxSize = %d, want %d", cfg.SQL.CacheMaxSize, 268435456)
	}
	if cfg.SQL.TempDirectory != "/tmp/sql-tmp" {
		t.Errorf("SQL.TempDirectory = %q, want %q", cfg.SQL.TempDirectory, "/tmp/sql-tmp")
	}
	if cfg.SQL.TempDirectoryValue() != "/tmp/sql-tmp" {
		t.Errorf("SQL.TempDirectoryValue() = %q, want %q", cfg.SQL.TempDirectoryValue(), "/tmp/sql-tmp")
	}
	if cfg.SQL.MemoryLimit != "512MB" {
		t.Errorf("SQL.MemoryLimit = %q, want %q", cfg.SQL.MemoryLimit, "512MB")
	}
	if cfg.SQL.MaxConcurrency != 8 {
		t.Errorf("SQL.MaxConcurrency = %d, want %d", cfg.SQL.MaxConcurrency, 8)
	}
	if cfg.Coordination.LeaseTTL != "20s" {
		t.Errorf("Coordination.LeaseTTL = %q, want %q", cfg.Coordination.LeaseTTL, "20s")
	}
	if cfg.Coordination.HeartbeatInterval != "5s" {
		t.Errorf("Coordination.HeartbeatInterval = %q, want %q", cfg.Coordination.HeartbeatInterval, "5s")
	}
	if cfg.Coordination.RebalanceDelay != "15s" {
		t.Errorf("Coordination.RebalanceDelay = %q, want %q", cfg.Coordination.RebalanceDelay, "15s")
	}
	if cfg.Coordination.InstanceTTL != "12s" {
		t.Errorf("Coordination.InstanceTTL = %q, want %q", cfg.Coordination.InstanceTTL, "12s")
	}
	if cfg.Coordination.MaintenanceMaxConcurrency != 7 {
		t.Errorf("Coordination.MaintenanceMaxConcurrency = %d, want %d", cfg.Coordination.MaintenanceMaxConcurrency, 7)
	}
}

func TestLoadDefaults(t *testing.T) {
	content := `
storage:
  bucket: "default-bucket"
`
	f, err := os.CreateTemp("", "camu-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	cfg, err := config.Load(f.Name())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Address != ":8080" {
		t.Errorf("Server.Address = %q, want %q", cfg.Server.Address, ":8080")
	}
	if cfg.Server.Mode != config.ServerModeStream {
		t.Errorf("Server.Mode = %q, want %q", cfg.Server.Mode, config.ServerModeStream)
	}
	if !cfg.Server.IsStreamMode() {
		t.Error("Server.IsStreamMode() = false, want true")
	}
	if cfg.Segments.RecordBatchTargetSize != 16*1024 {
		t.Errorf("Segments.RecordBatchTargetSize = %d, want %d", cfg.Segments.RecordBatchTargetSize, 16*1024)
	}
	if cfg.Segments.IndexIntervalBytes != 4096 {
		t.Errorf("Segments.IndexIntervalBytes = %d, want %d", cfg.Segments.IndexIntervalBytes, 4096)
	}
	if cfg.Segments.MaxSize != 8388608 {
		t.Errorf("Segments.MaxSize = %d, want %d", cfg.Segments.MaxSize, 8388608)
	}
	if cfg.SQL.CacheDirectory != "/var/lib/camu/sql-cache" {
		t.Errorf("SQL.CacheDirectory = %q, want %q", cfg.SQL.CacheDirectory, "/var/lib/camu/sql-cache")
	}
	if cfg.SQL.CacheDirectoryValue() != "/var/lib/camu/sql-cache" {
		t.Errorf("SQL.CacheDirectoryValue() = %q, want %q", cfg.SQL.CacheDirectoryValue(), "/var/lib/camu/sql-cache")
	}
	if cfg.SQL.CacheMaxSize != 5*1024*1024*1024 {
		t.Errorf("SQL.CacheMaxSize = %d, want %d", cfg.SQL.CacheMaxSize, 5*1024*1024*1024)
	}
	if cfg.SQL.TempDirectory != "/var/lib/camu/sql-tmp" {
		t.Errorf("SQL.TempDirectory = %q, want %q", cfg.SQL.TempDirectory, "/var/lib/camu/sql-tmp")
	}
	if cfg.SQL.TempDirectoryValue() != "/var/lib/camu/sql-tmp" {
		t.Errorf("SQL.TempDirectoryValue() = %q, want %q", cfg.SQL.TempDirectoryValue(), "/var/lib/camu/sql-tmp")
	}
	if cfg.SQL.MaxConcurrency != 4 {
		t.Errorf("SQL.MaxConcurrency = %d, want %d", cfg.SQL.MaxConcurrency, 4)
	}
	if cfg.Coordination.MaintenanceMaxConcurrency != 4 {
		t.Errorf("Coordination.MaintenanceMaxConcurrency = %d, want %d", cfg.Coordination.MaintenanceMaxConcurrency, 4)
	}
	instanceTTL, err := cfg.Coordination.InstanceTTLDuration()
	if err != nil {
		t.Fatalf("InstanceTTLDuration() error = %v", err)
	}
	if instanceTTL != 90*time.Second {
		t.Errorf("InstanceTTLDuration() = %v, want %v", instanceTTL, 90*time.Second)
	}
}

func TestLoadQueryMode(t *testing.T) {
	content := `
server:
  mode: "query"
storage:
  bucket: "bucket"
`
	f, err := os.CreateTemp("", "camu-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	cfg, err := config.Load(f.Name())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Server.Mode != config.ServerModeQuery {
		t.Fatalf("Server.Mode = %q, want %q", cfg.Server.Mode, config.ServerModeQuery)
	}
	if !cfg.Server.IsQueryMode() {
		t.Fatal("Server.IsQueryMode() = false, want true")
	}
}

func TestLoadQueryModeWithSQLDisabled(t *testing.T) {
	content := `
server:
  mode: "query"
storage:
  bucket: "bucket"
sql:
  enabled: false
`
	f, err := os.CreateTemp("", "camu-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	_, err = config.Load(f.Name())
	if err == nil {
		t.Fatal("Load() error = nil, want query mode SQL validation error")
	}
}

func TestLoadInvalidServerMode(t *testing.T) {
	content := `
server:
  mode: "weird"
storage:
  bucket: "bucket"
`
	f, err := os.CreateTemp("", "camu-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	_, err = config.Load(f.Name())
	if err == nil {
		t.Fatal("Load() error = nil, want invalid server.mode error")
	}
}

func TestLoadMissingBucket(t *testing.T) {
	content := `
server:
  address: ":8080"
`
	f, err := os.CreateTemp("", "camu-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	_, err = config.Load(f.Name())
	if err == nil {
		t.Error("Load() expected error for missing bucket, got nil")
	}
}

func TestCompactionConfigBounds(t *testing.T) {
	if got := (config.CompactionConfig{}).MinSegmentsValue(); got != 4 {
		t.Fatalf("default MinSegmentsValue = %d, want 4", got)
	}
	if got := (config.CompactionConfig{MinSegments: 1}).MinSegmentsValue(); got != 2 {
		t.Fatalf("MinSegmentsValue(1) = %d, want 2 (clamped to minimum)", got)
	}
	if got := (config.CompactionConfig{MinSegments: 5}).MinSegmentsValue(); got != 5 {
		t.Fatalf("MinSegmentsValue(5) = %d, want 5", got)
	}

	if got := (config.CompactionConfig{}).MaxSegmentsPerMergeValue(); got != 90 {
		t.Fatalf("default MaxSegmentsPerMergeValue = %d, want 90", got)
	}
	if got := (config.CompactionConfig{MaxSegmentsPerMerge: 100}).MaxSegmentsPerMergeValue(); got != 99 {
		t.Fatalf("MaxSegmentsPerMergeValue(100) = %d, want 99 (clamped under the DynamoDB 100-op transaction limit)", got)
	}
	if got := (config.CompactionConfig{MaxSegmentsPerMerge: 50}).MaxSegmentsPerMergeValue(); got != 50 {
		t.Fatalf("MaxSegmentsPerMergeValue(50) = %d, want 50", got)
	}
}
