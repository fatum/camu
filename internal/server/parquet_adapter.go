package server

import (
	"context"
	"errors"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/storage"
)

// parquetObjectAdapter wraps the server's *storage.S3Client so it
// satisfies iceberg.ObjectStore, translating backend-specific errors
// (storage.ErrNotFound / storage.ErrConflict) into the iceberg package's
// own error values. This is the seam that keeps internal/iceberg free of
// any storage package dependency.
type parquetObjectAdapter struct {
	client *storage.S3Client
}

func (a parquetObjectAdapter) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := a.client.Get(ctx, key)
	return data, translateStorageErr(err)
}

func (a parquetObjectAdapter) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := a.client.GetWithETag(ctx, key)
	return data, etag, translateStorageErr(err)
}

func (a parquetObjectAdapter) ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error) {
	newETag, err := a.client.ConditionalPut(ctx, key, data, etag)
	return newETag, translateStorageErr(err)
}

func (a parquetObjectAdapter) Delete(ctx context.Context, key string) error {
	return translateStorageErr(a.client.Delete(ctx, key))
}

func (a parquetObjectAdapter) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := a.client.List(ctx, prefix)
	return keys, translateStorageErr(err)
}

func (a parquetObjectAdapter) ListEach(ctx context.Context, prefix string, fn func(key string) error) error {
	return translateStorageErr(a.client.ListEach(ctx, prefix, fn))
}

func (a parquetObjectAdapter) DeleteMany(ctx context.Context, keys []string) error {
	return translateStorageErr(a.client.DeleteMany(ctx, keys))
}

func translateStorageErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrNotFound):
		return errors.Join(err, iceberg.ErrNotFound)
	case errors.Is(err, storage.ErrConflict):
		return errors.Join(err, iceberg.ErrConflict)
	default:
		return err
	}
}

// serverFencer adapts the server's topic-deletion state into the
// iceberg.Fencer interface.
type serverFencer struct{ s *Server }

func (f serverFencer) TopicDeletionPending(ctx context.Context, topic string) bool {
	return f.s.topicDeletionPending(ctx, topic)
}
