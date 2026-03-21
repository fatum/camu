package camutest

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/server"
	"github.com/maksim/camu/internal/storage"
)

// Env is a test environment with one or more camu server instances sharing
// an in-memory S3 backend.
type Env struct {
	t         testing.TB
	instances []*server.Server
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

// Cleanup shuts down all server instances.
func (e *Env) Cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range e.instances {
		srv.Shutdown(ctx)
	}
}
