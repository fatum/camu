//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestDurability_WALReplay(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	if err := client.CreateTopic("wal-test", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	// Produce messages before the crash.
	if _, err := client.Produce("wal-test", []camutest.ProduceMessage{
		{Key: "k1", Value: "before-crash-1"},
		{Key: "k1", Value: "before-crash-2"},
	}); err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	// Kill instance without graceful shutdown (skips flush).
	env.KillInstance(0)

	// Restart instance — WAL should replay and recover the unflushed messages.
	env.RestartInstance(0)

	// Wait for the new instance to be ready.
	if err := env.WaitForInstance(0, 5*time.Second); err != nil {
		t.Fatalf("instance not ready after restart: %v", err)
	}

	// After restart the client must point to the new address.
	client = env.Client()

	// Messages should be available from WAL replay (served from buffer).
	resp, err := client.Consume("wal-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) < 2 {
		t.Errorf("expected at least 2 messages after WAL replay, got %d", len(resp.Messages))
	}
}

func TestDurability_FlushPersistence(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	if err := client.CreateTopic("flush-test", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	if _, err := client.Produce("flush-test", []camutest.ProduceMessage{
		{Key: "k", Value: "persisted"},
	}); err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	// Wait for the batcher to flush the segment to S3 (MaxAge is 5 s in tests).
	time.Sleep(6 * time.Second)

	// Kill and restart.
	env.KillInstance(0)
	env.RestartInstance(0)

	if err := env.WaitForInstance(0, 5*time.Second); err != nil {
		t.Fatalf("instance not ready after restart: %v", err)
	}

	client = env.Client()

	// Data should be available from S3 (loaded via the index on init).
	resp, err := client.Consume("flush-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Errorf("expected 1 message from S3, got %d", len(resp.Messages))
	}
}
