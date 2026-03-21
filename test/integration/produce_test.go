//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestProduceSingleMessage(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("produce-test", 4, 24*time.Hour)

	resp, err := client.Produce("produce-test", []camutest.ProduceMessage{
		{Key: "user-1", Value: "hello world"},
	})
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}
	if len(resp.Offsets) != 1 {
		t.Fatalf("expected 1 offset, got %d", len(resp.Offsets))
	}
	if resp.Offsets[0].Offset != 0 {
		t.Errorf("first message offset = %d, want 0", resp.Offsets[0].Offset)
	}
}

func TestProduceBatch(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("batch-test", 1, 24*time.Hour)

	msgs := make([]camutest.ProduceMessage, 100)
	for i := range msgs {
		msgs[i] = camutest.ProduceMessage{Key: "same-key", Value: "msg"}
	}

	resp, err := client.Produce("batch-test", msgs)
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}
	if len(resp.Offsets) != 100 {
		t.Errorf("expected 100 offsets, got %d", len(resp.Offsets))
	}
}

func TestProduceToNonexistentTopic(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	_, err := client.Produce("ghost-topic", []camutest.ProduceMessage{{Value: "nope"}})
	if err == nil {
		t.Fatal("producing to nonexistent topic should error")
	}
}

func TestProduceKeyRouting(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("routing-test", 4, 24*time.Hour)

	resp1, err := client.Produce("routing-test", []camutest.ProduceMessage{{Key: "k1", Value: "a"}})
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}
	resp2, err := client.Produce("routing-test", []camutest.ProduceMessage{{Key: "k1", Value: "b"}})
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	if resp1.Offsets[0].Partition != resp2.Offsets[0].Partition {
		t.Errorf("same key routed to different partitions: %d vs %d",
			resp1.Offsets[0].Partition, resp2.Offsets[0].Partition)
	}
}
