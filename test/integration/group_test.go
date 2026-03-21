//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestGroupOffsets_CommitAndGet(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	topic := "cg-offset-test"
	client.CreateTopic(topic, 4, 24*time.Hour)

	// Commit offsets.
	offsets := map[int]uint64{0: 100, 1: 200, 2: 50, 3: 0}
	if err := client.CommitOffsets("offset-group", offsets); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// Get offsets.
	got, err := client.GetOffsets("offset-group")
	if err != nil {
		t.Fatalf("GetOffsets: %v", err)
	}

	for k, want := range offsets {
		if got[k] != want {
			t.Errorf("partition %d: got %d, want %d", k, got[k], want)
		}
	}
}

func TestConsumerSpecificOffsets(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	topic := "consumer-offset-test"
	client.CreateTopic(topic, 2, 24*time.Hour)

	// Commit consumer-specific offsets.
	offsets := map[int]uint64{0: 42, 1: 99}
	if err := client.CommitConsumerOffsets(topic, "standalone-consumer", offsets); err != nil {
		t.Fatalf("CommitConsumerOffsets: %v", err)
	}

	// Get consumer offsets.
	got, err := client.GetConsumerOffsets(topic, "standalone-consumer")
	if err != nil {
		t.Fatalf("GetConsumerOffsets: %v", err)
	}

	for k, want := range offsets {
		if got[k] != want {
			t.Errorf("partition %d: got %d, want %d", k, got[k], want)
		}
	}

	// Different consumer should have empty offsets.
	other, err := client.GetConsumerOffsets(topic, "other-consumer")
	if err != nil {
		t.Fatalf("GetConsumerOffsets other: %v", err)
	}
	if len(other) != 0 {
		t.Errorf("expected empty offsets for other consumer, got %v", other)
	}
}
