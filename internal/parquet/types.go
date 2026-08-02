// Package parquet provides the durable metadata layer for Camu's Parquet
// export pipeline: manifest publication with CAS and crash-safe compaction.
// It is decoupled from the server package and
// depends only on a small ObjectStore interface plus a Fencer for
// topic-deletion safety.
//
// The package is designed to be usable outside the streaming server —
// e.g. by query-only nodes that need to resolve manifests, or by offline
// tooling that rebuilds Parquet state from the canonical log.
package parquet

import (
	"errors"
	"time"
)

var (
	// ErrNotFound is returned by an ObjectStore when the requested key
	// does not exist. Parquet code calls errors.Is against this.
	ErrNotFound = errors.New("parquet: not found")

	// ErrConflict is returned by an ObjectStore when a ConditionalPut
	// precondition (ETag mismatch) fails.
	ErrConflict = errors.New("parquet: etag conflict")

	// ErrFenced is returned by manifest/checkpoint writers when the
	// topic is marked for deletion. The write is refused so that a stale
	// in-flight export job cannot resurrect state that topic cleanup is
	// about to delete.
	ErrFenced = errors.New("parquet: publication fenced")
)

// Entry is a single Parquet file reference inside a bucket manifest.
type Entry struct {
	ObjectKey     string `json:"object_key"`
	BaseOffset    int64  `json:"base_offset"`
	EndOffset     int64  `json:"end_offset"`
	SchemaVersion int    `json:"schema_version"`
	// SourceKey and SourceEpoch identify the immutable classic-log segment
	// that produced this file. They let an authoritative leader replace a
	// divergent overlapping projection without retaining duplicate rows.
	SourceKey   string `json:"source_key"`
	SourceEpoch uint64 `json:"source_epoch"`
}

// Manifest describes the set of Parquet files published in a single
// (topic, partition, dt/hour) bucket. The manifest is the atomic unit of
// visibility — queries read it to enumerate which Parquet files to scan,
// and compaction rewrites it to swap small files for larger ones.
type Manifest struct {
	Generation    uint64    `json:"generation"`
	Topic         string    `json:"topic"`
	Partition     int       `json:"partition"`
	Date          string    `json:"dt"`
	Hour          string    `json:"hour"`
	SchemaVersion int       `json:"schema_version"`
	Entries       []Entry   `json:"entries"`
	UpdatedAt     time.Time `json:"updated_at"`
}
