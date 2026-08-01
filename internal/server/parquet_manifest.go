package server

// This file delegates Parquet metadata operations to internal/parquet. All
// manifest publication and compaction logic
// topic-scoped listing and deletion) lives in internal/parquet. The
// server package delegates so the parquet code can be used standalone
// by query-only nodes and offline tooling.

import (
	"context"
	"time"

	"github.com/maksim/camu/internal/parquet"
)

// Server-side type aliases preserve existing call sites and tests
// verbatim while the underlying definitions live in internal/parquet.
type (
	ParquetManifestEntry = parquet.Entry
	ParquetManifest      = parquet.Manifest
)

// errParquetPublishFenced is a package-local alias so existing callers
// using `errors.Is(err, errParquetPublishFenced)` continue to work. It
// is the same error value as parquet.ErrFenced.
var errParquetPublishFenced = parquet.ErrFenced

// parquetStoreFor returns a parquet.Store bound to this server's
// S3 client and topic-deletion fence. The store is lightweight and
// safe to construct per-call; callers that invoke it frequently can
// cache the result on *Server if desired.
func (s *Server) parquetStoreFor() *parquet.Store {
	return s.newParquetStore()
}

func (s *Server) getParquetManifest(ctx context.Context, topic string, partition int, ingestTime time.Time) (ParquetManifest, error) {
	return s.parquetStoreFor().GetManifest(ctx, topic, partition, ingestTime)
}

func (s *Server) publishParquetManifest(ctx context.Context, manifest ParquetManifest) (ParquetManifest, error) {
	return s.parquetStoreFor().PublishManifest(ctx, manifest)
}

func (s *Server) compactParquetBucket(ctx context.Context, topic string, partition int, date, hour string, removeKeys []string, addEntries []ParquetManifestEntry) (ParquetManifest, error) {
	return s.parquetStoreFor().CompactBucket(ctx, topic, partition, date, hour, removeKeys, addEntries)
}

func (s *Server) replaceOverlappingParquetEntries(ctx context.Context, topic string, partition int, date, hour string, addEntries []ParquetManifestEntry) (ParquetManifest, error) {
	return s.parquetStoreFor().ReplaceOverlappingEntries(ctx, topic, partition, date, hour, addEntries)
}

func (s *Server) listParquetManifestsForTopic(ctx context.Context, topic string, from, to time.Time) ([]ParquetManifest, error) {
	return s.parquetStoreFor().ListTopicManifests(ctx, topic, from, to)
}

func (s *Server) deleteParquetTopicMetadata(ctx context.Context, topic string) error {
	return s.parquetStoreFor().DeleteTopicMetadata(ctx, topic)
}

func (s *Server) resolveParquetEntriesFromManifest(ctx context.Context, topic string, partition int, ingestTime time.Time) ([]ParquetManifestEntry, ParquetManifest, error) {
	return s.parquetStoreFor().ResolveEntriesForTime(ctx, topic, partition, ingestTime)
}
