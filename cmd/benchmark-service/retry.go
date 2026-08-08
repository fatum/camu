package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// retryWithBackoff calls fn repeatedly with exponential backoff until success,
// ctx cancellation, or maxAttempts exhausted. It returns the last error.
func retryWithBackoff(ctx context.Context, op string, maxAttempts int, fn func() error) error {
	backoff := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = fn()
		if lastErr == nil {
			if attempt > 0 {
				slog.Info("retry_succeeded", "op", op, "attempt", attempt+1)
			}
			return nil
		}
		slog.Warn("retry_failed", "op", op, "attempt", attempt+1, "error", lastErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
	return fmt.Errorf("%s: max retries exceeded: %w", op, lastErr)
}
