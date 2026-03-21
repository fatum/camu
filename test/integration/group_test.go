//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestConsumerGroup_JoinAndConsume(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	topic := "cg-join-test"
	client.CreateTopic(topic, 4, 24*time.Hour)

	// Produce 10 messages.
	var msgs []camutest.ProduceMessage
	for i := 0; i < 10; i++ {
		msgs = append(msgs, camutest.ProduceMessage{
			Key:   "k",
			Value: "msg-" + time.Now().Format("150405.000") + "-" + string(rune('0'+i)),
		})
	}
	_, err := client.Produce(topic, msgs)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Join group with one consumer.
	jr, err := client.JoinGroup("test-group", topic, "consumer-1")
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if len(jr.Partitions) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(jr.Partitions))
	}

	// Consume with group.
	cr, err := client.ConsumeWithGroup(topic, "test-group", "consumer-1")
	if err != nil {
		t.Fatalf("ConsumeWithGroup: %v", err)
	}

	if len(cr.Messages) == 0 {
		t.Fatal("expected messages, got 0")
	}
	t.Logf("consumed %d messages via group", len(cr.Messages))

	// Heartbeat should work.
	if err := client.Heartbeat("test-group", "consumer-1"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Leave group.
	if err := client.LeaveGroup("test-group", "consumer-1"); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}
}

func TestConsumerGroup_CommitAndGetOffsets(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	topic := "cg-offset-test"
	client.CreateTopic(topic, 4, 24*time.Hour)

	// Join group.
	_, err := client.JoinGroup("offset-group", topic, "c1")
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}

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
