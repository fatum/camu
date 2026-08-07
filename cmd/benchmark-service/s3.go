package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/maksim/camu/internal/storage"
)

var s3c *storage.S3Client

func initS3() {
	if s3c != nil {
		return
	}
	accessKey := env("CAMU_STORAGE_ACCESS_KEY", os.Getenv("AWS_ACCESS_KEY_ID"))
	secretKey := env("CAMU_STORAGE_SECRET_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY"))
	bucket := env("S3_BUCKET", "")
	if bucket == "" || accessKey == "" {
		return
	}
	client, err := storage.NewS3Client(storage.S3Config{
		Bucket:    bucket,
		Region:    env("S3_REGION", "us-east-1"),
		Endpoint:  env("S3_ENDPOINT", ""),
		AccessKey: accessKey,
		SecretKey: secretKey,
	})
	if err != nil {
		slog.Warn("s3_client_init_failed", "error", err)
		return
	}
	s3c = client
}

func s3PutRetry(key string, data []byte) error {
	if s3c == nil {
		return fmt.Errorf("s3 client not initialized")
	}
	backoff := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := s3c.Put(ctx, key, data, storage.PutOpts{})
		cancel()
		if err == nil {
			return nil
		}
		slog.Warn("s3_put_retry", "key", key, "attempt", attempt+1, "error", err)
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
	return fmt.Errorf("s3 put %q: max retries exceeded", key)
}

func s3ListRetry(prefix string) ([]string, error) {
	if s3c == nil {
		return nil, fmt.Errorf("s3 client not initialized")
	}
	backoff := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		keys, err := s3c.List(ctx, prefix)
		cancel()
		if err == nil {
			return keys, nil
		}
		slog.Warn("s3_list_retry", "prefix", prefix, "attempt", attempt+1, "error", err)
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
	return nil, fmt.Errorf("s3 list %q: max retries exceeded", prefix)
}

func s3GetRetry(key string) ([]byte, error) {
	if s3c == nil {
		return nil, fmt.Errorf("s3 client not initialized")
	}
	backoff := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		data, err := s3c.Get(ctx, key)
		cancel()
		if err == nil {
			return data, nil
		}
		slog.Warn("s3_get_retry", "key", key, "attempt", attempt+1, "error", err)
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
	return nil, fmt.Errorf("s3 get %q: max retries exceeded", key)
}
