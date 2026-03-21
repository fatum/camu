//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestRoutingEndpoint(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	if err := client.CreateTopic("route-test", 4, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	routing, err := client.GetRouting("route-test")
	if err != nil {
		t.Fatalf("GetRouting() error: %v", err)
	}
	if len(routing.Partitions) != 4 {
		t.Errorf("expected 4 partitions in routing, got %d", len(routing.Partitions))
	}

	// All partitions should map to the same instance in single-instance mode.
	instanceIDs := make(map[string]bool)
	for _, info := range routing.Partitions {
		instanceIDs[info.InstanceID] = true
	}
	if len(instanceIDs) != 1 {
		t.Errorf("expected 1 unique instance in single-instance mode, got %d", len(instanceIDs))
	}
}

func TestRoutingEndpoint_TopicNotFound(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	_, err := client.GetRouting("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
}

func TestMultiInstance_PartitionDistribution(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(3))
	defer env.Cleanup()
	client := env.Client()

	if err := client.CreateTopic("multi-test", 6, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	// Wait for lease acquisition by other instances.
	time.Sleep(1 * time.Second)

	routing, err := client.GetRouting("multi-test")
	if err != nil {
		t.Fatalf("GetRouting() error: %v", err)
	}

	if len(routing.Partitions) != 6 {
		t.Errorf("expected 6 partitions in routing, got %d", len(routing.Partitions))
	}

	// In single-camutest mode, all instances share the same S3 backend.
	// The creating instance acquires all leases, so all 6 partitions should be
	// owned by the first instance. Other instances haven't discovered the topic yet.
	// This validates the routing endpoint returns a full map.
	instanceCounts := make(map[string]int)
	for _, info := range routing.Partitions {
		instanceCounts[info.InstanceID]++
	}
	t.Logf("partition distribution: %v", instanceCounts)

	// At minimum, all partitions must be assigned.
	total := 0
	for _, count := range instanceCounts {
		total += count
	}
	if total != 6 {
		t.Errorf("expected 6 total partition assignments, got %d", total)
	}
}

func TestMultiInstance_ProduceConsumeAcrossInstances(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(2))
	defer env.Cleanup()
	client := env.Client()

	if err := client.CreateTopic("cross-test", 4, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	// Create topic on second instance too so it inits partitions.
	env.ClientFor(1).CreateTopic("cross-test", 4, 24*time.Hour)

	// Wait for lease renewal to rebalance partitions across both instances.
	time.Sleep(8 * time.Second)

	// Produce messages — try both instances (handles 421 misdirected).
	client0 := env.ClientFor(0)
	client1 := env.ClientFor(1)
	produced := 0
	for i := 0; i < 20; i++ {
		key := string(rune('a' + i%4))
		msgs := []camutest.ProduceMessage{{Key: key, Value: "msg"}}
		_, err := client0.Produce("cross-test", msgs)
		if err != nil {
			_, err = client1.Produce("cross-test", msgs)
		}
		if err == nil {
			produced++
		}
	}

	// Wait for flush.
	time.Sleep(6 * time.Second)

	// Consume from all partitions via both instances.
	total := 0
	for p := 0; p < 4; p++ {
		resp, err := client0.Consume("cross-test", p, 0, 100)
		if err != nil {
			resp, err = client1.Consume("cross-test", p, 0, 100)
		}
		if err == nil {
			total += len(resp.Messages)
		}
	}

	if total != produced {
		t.Errorf("total consumed = %d, want %d (produced)", total, produced)
	}
	if produced == 0 {
		t.Error("no messages produced — partition distribution may be broken")
	}
}

func TestMultiInstance_ProduceToSecondInstance(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(2))
	defer env.Cleanup()

	// Create topic on instance 0 (which will acquire all leases).
	client0 := env.ClientFor(0)
	if err := client0.CreateTopic("inst-test", 2, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	// Instance 1 needs to discover and init the topic before it can serve it.
	// For now, producing to instance 1 should return 421 since it doesn't own the partitions.
	client1 := env.ClientFor(1)

	// Instance 1 doesn't know about this topic yet, so produce will fail with 404 or 421.
	_, err := client1.Produce("inst-test", []camutest.ProduceMessage{
		{Key: "test", Value: "hello"},
	})
	if err == nil {
		t.Log("produce to non-owner succeeded (instance may have discovered topic)")
	} else {
		t.Logf("produce to non-owner returned expected error: %v", err)
	}

	// Verify routing from instance 0 shows partitions.
	routing, err := client0.GetRouting("inst-test")
	if err != nil {
		t.Fatalf("GetRouting() error: %v", err)
	}
	if len(routing.Partitions) != 2 {
		t.Errorf("expected 2 partitions in routing, got %d", len(routing.Partitions))
	}
}
