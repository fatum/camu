//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

// TestLeaderKill_TailDurability reproduces the leader-kill tail-loss bug: acked
// records that still live only in active (unflushed) segments must survive
// repeated leader-kill failover without creating holes in the committed log.
//
// The Jepsen matrix previously showed this as committed-durability violations
// under repeated leader kills (a follower that replicated the committed tail
// could be truncated, or a sealed-but-unpublished segment could be dropped,
// losing acked records).
func TestLeaderKill_TailDurability(t *testing.T) {
	instanceIDs := []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}
	env := camutest.New(t,
		camutest.WithInstances(len(instanceIDs)),
		camutest.WithInstanceIDs(instanceIDs...),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			// Keep records in active segments for the whole test so the committed
			// tail exists only in active segments at kill time, like the Jepsen
			// runs where segment-max-age exceeded the failover window.
			cfg.Segments.MaxAge = "1h"
			cfg.Segments.MaxSize = 1 << 30
		}),
	)
	defer env.Cleanup()

	const (
		topic      = "tail-durability"
		partitions = 1
		replicas   = 3
		minISR     = 2
	)
	if err := env.Client().CreateTopicWithReplication(topic, partitions, 24*time.Hour, replicas, minISR); err != nil {
		t.Fatalf("CreateTopicWithReplication() error: %v", err)
	}
	waitForClusterReady(t, env, topic, partitions, replicas)

	acked := 0
	produce := func(n int, prefix string) {
		for i := 0; i < n; i++ {
			leaderIdx, err := leaderIndexForPartition(t, env, topic, 0)
			if err != nil {
				t.Fatalf("produce %s-%d: leader lookup: %v", prefix, i, err)
			}
			msgs := []camutest.ProduceMessage{{Key: fmt.Sprintf("%s-%d", prefix, i), Value: "v"}}
			if _, err := env.ClientFor(leaderIdx).ProduceToPartition(topic, 0, msgs); err != nil {
				t.Fatalf("produce %s-%d: %v", prefix, i, err)
			}
			acked++
		}
	}

	// Phase 1: produce a committed tail, then kill the leader while the tail is
	// still unflushed.
	produce(20, "a")
	killLeaderForPartition(t, env, topic, 0)
	waitForClusterReady(t, env, topic, partitions, replicas)

	// Phase 2: produce another committed tail, kill the new leader.
	produce(20, "b")
	killLeaderForPartition(t, env, topic, 0)
	waitForClusterReady(t, env, topic, partitions, replicas)

	// Phase 3: produce a final tail.
	produce(20, "c")

	// All instances were already restarted by killLeaderForPartition.
	time.Sleep(2 * time.Second)

	// Drain from the current leader and verify every acked record is present
	// with contiguous offsets.
	leaderIdx, err := leaderIndexForPartition(t, env, topic, 0)
	if err != nil {
		t.Fatalf("leader lookup: %v", err)
	}
	var got []camutest.ConsumedMessage
	offset := uint64(0)
	for offset < uint64(acked) {
		resp, err := env.ClientFor(leaderIdx).Consume(topic, 0, offset, 100)
		if err != nil {
			t.Fatalf("consume at %d: %v", offset, err)
		}
		if len(resp.Messages) == 0 {
			t.Fatalf("consume at offset %d returned no messages (lost tail, got %d so far)", offset, len(got))
		}
		got = append(got, resp.Messages...)
		offset += uint64(len(resp.Messages))
	}

	if len(got) != acked {
		t.Fatalf("drained %d records, want %d acked (data loss or duplicate)", len(got), acked)
	}
	for i, m := range got {
		if int(m.Offset) != i {
			t.Fatalf("offset %d, want %d: committed log has a hole", m.Offset, i)
		}
	}
}

// leaderIndexForPartition returns the instance index currently leading the
// given topic partition.
func leaderIndexForPartition(t *testing.T, env *camutest.Env, topic string, partition int) (int, error) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		routing, err := env.Client().GetRouting(topic)
		if err == nil {
			if info, ok := routing.Partitions[fmt.Sprintf("%d", partition)]; ok {
				if idx := env.InstanceIndex(info.InstanceID); idx >= 0 {
					return idx, nil
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return -1, fmt.Errorf("no leader for %s/%d", topic, partition)
}

// killLeaderForPartition kills the instance currently leading the partition,
// lets the lease expire and a new leader be elected, then restarts the killed
// instance so cluster membership is preserved.
func killLeaderForPartition(t *testing.T, env *camutest.Env, topic string, partition int) {
	t.Helper()
	idx, err := leaderIndexForPartition(t, env, topic, partition)
	if err != nil {
		t.Fatalf("kill leader: %v", err)
	}
	t.Logf("killing leader instance %d (%s)", idx, env.InstanceAddress(idx))
	env.KillInstance(idx)
	// Longer than the 6s lease TTL so the cluster elects a new leader while the
	// old leader is down, exercising the failover recovery path.
	time.Sleep(8 * time.Second)
	env.RestartInstance(idx)
}

// waitForClusterReady waits until the topic has all replicas assigned and the
// cluster reports ready.
func waitForClusterReady(t *testing.T, env *camutest.Env, topic string, partitions, replicas int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := env.Client().ClusterStatus()
		if err == nil && status.Ready && status.ActiveInstances == replicas &&
			status.InitializedPartitions == partitions && status.ExpectedPartitions == partitions {
			routing, rErr := env.Client().GetRouting(topic)
			if rErr == nil && len(routing.Partitions) == partitions {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("cluster never became ready with %d replicas and %d partitions", replicas, partitions)
}
