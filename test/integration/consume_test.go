//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestProduceAndConsume(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("pc-test", 1, 24*time.Hour)

	// Produce messages.
	_, err := client.Produce("pc-test", []camutest.ProduceMessage{
		{Key: "k1", Value: "hello"},
		{Key: "k1", Value: "world"},
	})
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	// Consume immediately — should read from in-memory buffer (no flush needed).
	resp, err := client.Consume("pc-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(resp.Messages))
	}
	if resp.Messages[0].Value != "hello" {
		t.Errorf("first message = %q, want %q", resp.Messages[0].Value, "hello")
	}
	if resp.Messages[1].Value != "world" {
		t.Errorf("second message = %q, want %q", resp.Messages[1].Value, "world")
	}
	if resp.NextOffset != 2 {
		t.Errorf("NextOffset = %d, want 2", resp.NextOffset)
	}
}

func TestProduceAndConsumeAfterFlush(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("pcf-test", 1, 24*time.Hour)

	// Produce messages.
	_, err := client.Produce("pcf-test", []camutest.ProduceMessage{
		{Key: "k1", Value: "hello"},
		{Key: "k1", Value: "world"},
	})
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	// Wait for flush to S3.
	time.Sleep(6 * time.Second)

	// Consume — should read from S3/cache after flush.
	resp, err := client.Consume("pcf-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(resp.Messages))
	}
	if resp.Messages[0].Value != "hello" {
		t.Errorf("first message = %q, want %q", resp.Messages[0].Value, "hello")
	}
	if resp.NextOffset != 2 {
		t.Errorf("NextOffset = %d, want 2", resp.NextOffset)
	}
}

func TestConsumeFromOffset(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("cfo-test", 1, 24*time.Hour)

	// Produce 5 messages.
	msgs := make([]camutest.ProduceMessage, 5)
	for i := range msgs {
		msgs[i] = camutest.ProduceMessage{
			Key:   "k1",
			Value: "msg-" + string(rune('A'+i)),
		}
	}
	_, err := client.Produce("cfo-test", msgs)
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	// Consume from offset 3 — should get 2 messages from buffer.
	resp, err := client.Consume("cfo-test", 0, 3, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(resp.Messages))
	}
	if resp.Messages[0].Offset != 3 {
		t.Errorf("first message offset = %d, want 3", resp.Messages[0].Offset)
	}
	if resp.NextOffset != 5 {
		t.Errorf("NextOffset = %d, want 5", resp.NextOffset)
	}
}

func TestSSEStreaming(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("sse-test", 1, 24*time.Hour)

	client.Produce("sse-test", []camutest.ProduceMessage{
		{Key: "k1", Value: "msg1"},
		{Key: "k1", Value: "msg2"},
	})

	// SSE should read from buffer immediately (no flush wait needed)
	events, err := client.StreamSSE("sse-test", 0, 0, 2, 5*time.Second)
	if err != nil {
		t.Fatalf("StreamSSE() error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 SSE events, got %d", len(events))
	}
	if events[0].Value != "msg1" {
		t.Errorf("first event value = %q, want %q", events[0].Value, "msg1")
	}
}

func TestConsumeEmptyTopic(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("empty-test", 1, 24*time.Hour)

	// Consume from empty topic.
	resp, err := client.Consume("empty-test", 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if len(resp.Messages) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(resp.Messages))
	}
	if resp.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0", resp.NextOffset)
	}
}
