//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestLeadership_ReassignmentPreservesCommittedData_RF3(t *testing.T) {
	env := camutest.New(t,
		camutest.WithInstances(3),
		camutest.WithInstanceIDs("127.0.0.3", "127.0.0.1", "127.0.0.2"),
	)
	defer env.Cleanup()

	const (
		topic   = "leadership-rf3"
		partID  = 0
		partKey = "0"
	)

	if err := env.Client().CreateTopicWithReplication(topic, 1, 24*time.Hour, 3, 2); err != nil {
		t.Fatalf("CreateTopicWithReplication() error: %v", err)
	}

	// Topic discovery and follower fetch loops happen on the lease-renewal cycle.
	time.Sleep(12 * time.Second)

	leaderIdx, leaderClient := waitForLeader(t, env, topic, partKey, map[int]bool{})
	values := []string{"v0", "v1", "v2", "v3", "v4"}
	for _, value := range values {
		if _, err := leaderClient.ProduceToPartition(topic, partID, []camutest.ProduceMessage{{Key: "k", Value: value}}); err != nil {
			t.Fatalf("Produce(%q) via leader instance %d error: %v", value, leaderIdx, err)
		}
	}

	env.StopInstance(leaderIdx)

	killed := map[int]bool{leaderIdx: true}
	_, _, resp := waitForReadableLeader(t, env, topic, partID, killed, len(values))

	if got, want := len(resp.Messages), len(values); got != want {
		t.Fatalf("consumed %d messages, want %d", got, want)
	}
	for i, msg := range resp.Messages {
		if got, want := msg.Offset, uint64(i); got != want {
			t.Fatalf("message %d offset = %d, want %d", i, got, want)
		}
		if got, want := msg.Value, fmt.Sprintf("v%d", i); got != want {
			t.Fatalf("message %d value = %q, want %q", i, got, want)
		}
	}

	if got, want := resp.NextOffset, uint64(len(values)); got != want {
		t.Fatalf("NextOffset = %d, want %d", got, want)
	}
}

func waitForLeader(t *testing.T, env *camutest.Env, topic, partition string, excluded map[int]bool) (int, *camutest.Client) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for idx := 0; idx < 3; idx++ {
			if excluded[idx] {
				continue
			}
			client := env.ClientFor(idx)
			routing, err := client.GetRouting(topic)
			if err != nil {
				continue
			}
			info, ok := routing.Partitions[partition]
			if !ok || info.InstanceID == "" {
				continue
			}
			candidate := env.InstanceIndex(info.InstanceID)
			if candidate >= 0 && !excluded[candidate] {
				return candidate, env.ClientFor(candidate)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("leader for topic %q not found before timeout", topic)
	return -1, nil
}

func waitForReadableLeader(t *testing.T, env *camutest.Env, topic string, partition int, excluded map[int]bool, wantMessages int) (int, *camutest.Client, *camutest.ConsumeResponse) {
	t.Helper()

	deadline := time.Now().Add(35 * time.Second)
	for time.Now().Before(deadline) {
		for idx := 0; idx < 3; idx++ {
			if excluded[idx] {
				continue
			}
			client := env.ClientFor(idx)
			resp, err := client.Consume(topic, partition, 0, wantMessages+4)
			if err == nil && len(resp.Messages) >= wantMessages {
				return idx, client, resp
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("no readable leader for topic %q before timeout", topic)
	return -1, nil, nil
}
