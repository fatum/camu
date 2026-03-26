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
wal:
  directory: "/tmp/wal"
  fsync: false
  chunk_size: 8192
segments:
  max_size: 1048576
  max_age: "10s"
  compression: "snappy"
  record_batch_target_size: 32768
  index_interval_bytes: 8192
cache:
  directory: "/tmp/cache"
  max_size: 5368709120
coordination:
  lease_ttl: "20s"
  heartbeat_interval: "5s"
  rebalance_delay: "15s"
  instance_ttl: "12s"
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
	if cfg.WAL.Directory != "/tmp/wal" {
		t.Errorf("WAL.Directory = %q, want %q", cfg.WAL.Directory, "/tmp/wal")
	}
	if cfg.WAL.Fsync != false {
		t.Errorf("WAL.Fsync = %v, want false", cfg.WAL.Fsync)
	}
	if cfg.WAL.ChunkSize != 8192 {
		t.Errorf("WAL.ChunkSize = %d, want %d", cfg.WAL.ChunkSize, 8192)
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
	if cfg.WAL.Fsync != true {
		t.Errorf("WAL.Fsync = %v, want true", cfg.WAL.Fsync)
	}
	if cfg.WAL.ChunkSize != 64*1024*1024 {
		t.Errorf("WAL.ChunkSize = %d, want %d", cfg.WAL.ChunkSize, 64*1024*1024)
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
	instanceTTL, err := cfg.Coordination.InstanceTTLDuration()
	if err != nil {
		t.Fatalf("InstanceTTLDuration() error = %v", err)
	}
	if instanceTTL != 90*time.Second {
		t.Errorf("InstanceTTLDuration() = %v, want %v", instanceTTL, 90*time.Second)
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
