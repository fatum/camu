// Package iceberg owns the analytical projection of Camu topics: the Parquet
// data files and the self-managed Iceberg table metadata (manifests, manifest
// lists, metadata.json) that make those files queryable by external engines.
// It is decoupled from the server package and depends only on a small
// ObjectStore interface plus a Fencer for topic-deletion safety.
package iceberg

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned by an ObjectStore when the requested key
	// does not exist.
	ErrNotFound = errors.New("iceberg: not found")

	// ErrConflict is returned by an ObjectStore when a ConditionalPut
	// precondition (ETag mismatch) fails.
	ErrConflict = errors.New("iceberg: etag conflict")

	// ErrFenced is returned by metadata writers when the topic is marked
	// for deletion. The write is refused so that a stale in-flight export
	// job cannot resurrect state that topic cleanup is about to delete.
	ErrFenced = errors.New("iceberg: publication fenced")
)

// ObjectStore is the minimal object-storage contract the Iceberg metadata
// layer needs. Implementations must translate backend-specific errors into
// this package's ErrNotFound / ErrConflict via error wrapping (errors.Is is
// used by callers).
type ObjectStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetWithETag(ctx context.Context, key string) ([]byte, string, error)
	ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	// ListEach streams keys with the given prefix to fn one page at a time so a
	// cleanup never holds the full key set in memory. Stops at fn's first error.
	ListEach(ctx context.Context, prefix string, fn func(key string) error) error
	// DeleteMany deletes the given keys in batches (idempotent).
	DeleteMany(ctx context.Context, keys []string) error
}

// Fencer reports whether Iceberg writes for a topic must be refused because
// topic deletion is in progress. A minimal seam so the package does not need
// to know about server-side topic lifecycle state.
type Fencer interface {
	TopicDeletionPending(ctx context.Context, topic string) bool
}

// NoFencer is a Fencer that never fences. Use in tooling or tests.
type NoFencer struct{}

func (NoFencer) TopicDeletionPending(context.Context, string) bool { return false }
