package camutest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/server"
	"github.com/maksim/camu/internal/storage"
)

// instanceConfig holds the config used to start a particular instance so it
// can be reused on restart.
type instanceConfig struct {
	cfg     *config.Config
	walDir  string
	cacheDir string
}

// Env is a test environment with one or more camu server instances sharing
// an in-memory S3 backend.
type Env struct {
	t         testing.TB
	instances []*server.Server
	configs   []instanceConfig
	s3Client  *storage.S3Client
}

// New creates a new test environment. It starts the requested number of
// server instances (default 1), each listening on a random port and sharing
// a single in-memory S3 backend.
func New(t testing.TB, opts ...Option) *Env {
	t.Helper()

	o := Options{Instances: 1}
	for _, fn := range opts {
		fn(&o)
	}
	if o.Instances < 1 {
		o.Instances = 1
	}

	// Always use in-memory S3 for now (MinIO support is a future stub).
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test-bucket",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("camutest: creating S3 client: %v", err)
	}

	env := &Env{
		t:        t,
		s3Client: s3Client,
	}

	for i := 0; i < o.Instances; i++ {
		walDir := t.TempDir()
		cacheDir := t.TempDir()
		cfg := &config.Config{
			Server: config.ServerConfig{
				Address: ":0",
			},
			Storage: config.StorageConfig{
				Bucket:   "test-bucket",
				Endpoint: "memory://",
			},
			WAL: config.WALConfig{
				Directory: walDir,
				Fsync:     false,
			},
			Segments: config.SegmentsConfig{
				MaxSize:     8388608,
				MaxAge:      "5s",
				Compression: "none",
			},
			Cache: config.CacheConfig{
				Directory: cacheDir,
				MaxSize:   104857600, // 100 MB for tests
			},
		}

		srv, err := server.NewWithS3Client(cfg, s3Client)
		if err != nil {
			t.Fatalf("camutest: creating server instance %d: %v", i, err)
		}

		if err := srv.StartOnPort(0); err != nil {
			t.Fatalf("camutest: starting server instance %d: %v", i, err)
		}

		env.instances = append(env.instances, srv)
		env.configs = append(env.configs, instanceConfig{
			cfg:      cfg,
			walDir:   walDir,
			cacheDir: cacheDir,
		})
	}

	return env
}

// Client returns an HTTP client pointed at the first server instance.
func (e *Env) Client() *Client {
	return e.ClientFor(0)
}

// ClientFor returns an HTTP client pointed at the server instance at the given index.
func (e *Env) ClientFor(idx int) *Client {
	return NewClient("http://" + e.InstanceAddress(idx))
}

// InstanceAddress returns the listening address of the server at the given index.
func (e *Env) InstanceAddress(idx int) string {
	if idx < 0 || idx >= len(e.instances) {
		e.t.Fatalf("camutest: instance index %d out of range (have %d)", idx, len(e.instances))
	}
	return e.instances[idx].Address()
}

// KillInstance forcefully stops the server instance at idx without a graceful
// shutdown (no WAL flush). It uses a zero-timeout context so the HTTP listener
// is closed immediately.
func (e *Env) KillInstance(idx int) {
	e.t.Helper()
	if idx < 0 || idx >= len(e.instances) {
		e.t.Fatalf("camutest: KillInstance: index %d out of range (have %d)", idx, len(e.instances))
	}
	// Use an already-cancelled context so Shutdown returns immediately without
	// waiting for in-flight requests or flushing the batcher/WAL.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	// Ignore errors — the context expiry causes expected deadline-exceeded errors.
	_ = e.instances[idx].Shutdown(ctx)
	e.instances[idx] = nil
}

// RestartInstance creates a new server with the same config (and same WAL
// directory) as the instance at idx and starts it. This causes WAL replay on
// the new instance. The instance slot is updated in-place so subsequent calls
// to ClientFor/InstanceAddress use the new address.
func (e *Env) RestartInstance(idx int) {
	e.t.Helper()
	if idx < 0 || idx >= len(e.configs) {
		e.t.Fatalf("camutest: RestartInstance: index %d out of range (have %d)", idx, len(e.configs))
	}

	ic := e.configs[idx]

	// Build a fresh config reusing the same WAL and cache directories so WAL
	// replay can recover unflushed data, and previously flushed segments remain
	// in the disk cache.
	cfg := &config.Config{
		Server: config.ServerConfig{
			Address: ":0", // new random port
		},
		Storage: config.StorageConfig{
			Bucket:   ic.cfg.Storage.Bucket,
			Endpoint: ic.cfg.Storage.Endpoint,
		},
		WAL: config.WALConfig{
			Directory: ic.walDir,
			Fsync:     ic.cfg.WAL.Fsync,
		},
		Segments: ic.cfg.Segments,
		Cache: config.CacheConfig{
			Directory: ic.cacheDir,
			MaxSize:   ic.cfg.Cache.MaxSize,
		},
	}

	srv, err := server.NewWithS3Client(cfg, e.s3Client)
	if err != nil {
		e.t.Fatalf("camutest: RestartInstance %d: creating server: %v", idx, err)
	}

	if err := srv.StartOnPort(0); err != nil {
		e.t.Fatalf("camutest: RestartInstance %d: starting server: %v", idx, err)
	}

	e.instances[idx] = srv
	// Update stored config to reflect the new instance's config.
	e.configs[idx] = instanceConfig{
		cfg:      cfg,
		walDir:   ic.walDir,
		cacheDir: ic.cacheDir,
	}
}

// InstanceIndex returns the slice index of the instance with the given instanceID,
// or -1 if not found.
func (e *Env) InstanceIndex(instanceID string) int {
	for i, srv := range e.instances {
		if srv != nil && srv.InstanceID() == instanceID {
			return i
		}
	}
	return -1
}

// Cleanup shuts down all server instances.
func (e *Env) Cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range e.instances {
		if srv != nil {
			srv.Shutdown(ctx)
		}
	}
}

// WaitForInstance polls until the instance at idx responds to HTTP or the
// timeout elapses. Callers should use this after RestartInstance when they need
// the server to be fully ready before making requests.
func (e *Env) WaitForInstance(idx int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := e.ClientFor(idx)
	for time.Now().Before(deadline) {
		_, err := client.ListTopics()
		if err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("instance %d not ready after %v", idx, timeout)
}
