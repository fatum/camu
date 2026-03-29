//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	logstore "github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/pkg/camutest"
)

func TestIdempotency_RestartPreservesRecoveredStateAndS3Checkpoint(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	const (
		topic = "idem-restart"
		pid   = 0
	)

	client := env.Client()
	if err := client.CreateTopic(topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	producer, err := client.InitProducer()
	if err != nil {
		t.Fatalf("InitProducer() error: %v", err)
	}

	first, err := client.ProduceIdempotentToPartition(topic, pid, camutest.IdempotentProduceRequest{
		ProducerID: producer.ProducerID,
		Sequence:   0,
		Messages: []camutest.ProduceMessage{
			{Key: "k", Value: "v0"},
			{Key: "k", Value: "v1"},
		},
	})
	if err != nil {
		t.Fatalf("first ProduceIdempotentToPartition() error: %v", err)
	}
	if first.Duplicate {
		t.Fatal("first idempotent produce unexpectedly marked duplicate")
	}

	time.Sleep(6 * time.Second)

	checkpointKey := topic + "/0/producers.checkpoint"
	if data, err := env.S3Client().Get(context.Background(), checkpointKey); err != nil {
		t.Fatalf("Get(%q) error: %v", checkpointKey, err)
	} else if len(data) == 0 {
		t.Fatalf("Get(%q) returned empty checkpoint", checkpointKey)
	}

	stateKey := logstore.StateKey(topic, pid)
	stateData, err := env.S3Client().Get(context.Background(), stateKey)
	if err != nil {
		t.Fatalf("Get(%q) error: %v", stateKey, err)
	}
	var partState logstore.PartitionState
	if err := partState.Unmarshal(stateData); err != nil {
		t.Fatalf("PartitionState.Unmarshal() error: %v", err)
	}
	if got, want := partState.HighWatermark, uint64(2); got != want {
		t.Fatalf("state.HighWatermark = %d, want %d", got, want)
	}

	env.KillInstance(0)
	env.RestartInstance(0)
	if err := env.WaitForInstance(0, 5*time.Second); err != nil {
		t.Fatalf("instance not ready after restart: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, ok := env.Server(0).ProducerStateSnapshot(topic, pid, producer.ProducerID)
		if ok && snap.NextSeq == 2 && snap.LastOffset == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if snap, ok := env.Server(0).ProducerStateSnapshot(topic, pid, producer.ProducerID); !ok {
		t.Fatalf("ProducerStateSnapshot(%q, %d, %d) not found after restart", topic, pid, producer.ProducerID)
	} else if snap.NextSeq != 2 || snap.LastOffset != 1 {
		t.Fatalf("ProducerStateSnapshot after restart = %+v, want NextSeq=2 LastOffset=1", snap)
	}
	if ps, ok := env.Server(0).PartitionStateSnapshot(topic, pid); !ok {
		t.Fatalf("PartitionStateSnapshot(%q, %d) not found after restart", topic, pid)
	} else if ps.ReadableHighWatermark != 2 {
		t.Fatalf("PartitionStateSnapshot after restart = %+v, want ReadableHighWatermark=2", ps)
	}

	client = env.Client()
	dup, err := client.ProduceIdempotentToPartition(topic, pid, camutest.IdempotentProduceRequest{
		ProducerID: producer.ProducerID,
		Sequence:   0,
		Messages: []camutest.ProduceMessage{
			{Key: "k", Value: "v0"},
			{Key: "k", Value: "v1"},
		},
	})
	if err != nil {
		t.Fatalf("duplicate ProduceIdempotentToPartition() after restart error: %v", err)
	}
	if !dup.Duplicate {
		t.Fatalf("duplicate retry after restart = %+v, want duplicate=true", dup)
	}

	next, err := client.ProduceIdempotentToPartition(topic, pid, camutest.IdempotentProduceRequest{
		ProducerID: producer.ProducerID,
		Sequence:   2,
		Messages: []camutest.ProduceMessage{
			{Key: "k", Value: "v2"},
		},
	})
	if err != nil {
		t.Fatalf("next ProduceIdempotentToPartition() after restart error: %v", err)
	}
	if next.Duplicate {
		t.Fatal("next idempotent batch unexpectedly marked duplicate")
	}
	if got, want := len(next.Offsets), 1; got != want {
		t.Fatalf("len(next.Offsets) = %d, want %d", got, want)
	}
	if got, want := next.Offsets[0].Offset, uint64(2); got != want {
		t.Fatalf("next offset = %d, want %d", got, want)
	}
}

func TestIdempotency_FailoverPreservesRecoveredStateAndS3State_RF3(t *testing.T) {
	env := camutest.New(t,
		camutest.WithInstances(3),
		camutest.WithInstanceIDs("127.0.0.3", "127.0.0.1", "127.0.0.2"),
	)
	defer env.Cleanup()

	const (
		topic   = "idem-failover-rf3"
		partID  = 0
		partKey = "0"
	)

	if err := env.Client().CreateTopicWithReplication(topic, 1, 24*time.Hour, 3, 2); err != nil {
		t.Fatalf("CreateTopicWithReplication() error: %v", err)
	}

	time.Sleep(12 * time.Second)

	leaderIdx, leaderClient := waitForLeader(t, env, topic, partKey, map[int]bool{})
	producer, err := leaderClient.InitProducer()
	if err != nil {
		t.Fatalf("InitProducer() error: %v", err)
	}

	first, err := leaderClient.ProduceIdempotentToPartition(topic, partID, camutest.IdempotentProduceRequest{
		ProducerID: producer.ProducerID,
		Sequence:   0,
		Messages: []camutest.ProduceMessage{
			{Key: "k", Value: "v0"},
			{Key: "k", Value: "v1"},
		},
	})
	if err != nil {
		t.Fatalf("first ProduceIdempotentToPartition() error: %v", err)
	}
	if first.Duplicate {
		t.Fatal("first idempotent produce unexpectedly marked duplicate")
	}

	env.StopInstance(leaderIdx)

	excluded := map[int]bool{leaderIdx: true}
	newLeaderIdx, newLeaderClient := waitForLeader(t, env, topic, partKey, excluded)

	stateKey := logstore.StateKey(topic, partID)
	deadline := time.Now().Add(15 * time.Second)
	var partState logstore.PartitionState
	for time.Now().Before(deadline) {
		data, err := env.S3Client().Get(context.Background(), stateKey)
		if err == nil && len(data) > 0 {
			if err := partState.Unmarshal(data); err == nil && partState.HighWatermark >= 2 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got, want := partState.HighWatermark, uint64(2); got < want {
		t.Fatalf("state.HighWatermark = %d, want >= %d after failover", got, want)
	}

	checkpointKey := topic + "/0/producers.checkpoint"
	if data, err := env.S3Client().Get(context.Background(), checkpointKey); err != nil {
		t.Fatalf("Get(%q) error: %v", checkpointKey, err)
	} else if len(data) == 0 {
		t.Fatalf("Get(%q) returned empty checkpoint", checkpointKey)
	}

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snap, ok := env.Server(newLeaderIdx).ProducerStateSnapshot(topic, partID, producer.ProducerID)
		if ok && snap.NextSeq == 2 && snap.LastOffset == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if snap, ok := env.Server(newLeaderIdx).ProducerStateSnapshot(topic, partID, producer.ProducerID); !ok {
		t.Fatalf("ProducerStateSnapshot(%q, %d, %d) not found after failover", topic, partID, producer.ProducerID)
	} else if snap.NextSeq != 2 || snap.LastOffset != 1 {
		t.Fatalf("ProducerStateSnapshot after failover = %+v, want NextSeq=2 LastOffset=1", snap)
	}
	if ps, ok := env.Server(newLeaderIdx).PartitionStateSnapshot(topic, partID); !ok {
		t.Fatalf("PartitionStateSnapshot(%q, %d) not found after failover", topic, partID)
	} else if ps.ReadableHighWatermark < 2 {
		t.Fatalf("PartitionStateSnapshot after failover = %+v, want ReadableHighWatermark>=2", ps)
	}

	dup, err := newLeaderClient.ProduceIdempotentToPartition(topic, partID, camutest.IdempotentProduceRequest{
		ProducerID: producer.ProducerID,
		Sequence:   0,
		Messages: []camutest.ProduceMessage{
			{Key: "k", Value: "v0"},
			{Key: "k", Value: "v1"},
		},
	})
	if err != nil {
		t.Fatalf("duplicate ProduceIdempotentToPartition() after failover error: %v", err)
	}
	if !dup.Duplicate {
		t.Fatalf("duplicate retry after failover = %+v, want duplicate=true", dup)
	}

	next, err := newLeaderClient.ProduceIdempotentToPartition(topic, partID, camutest.IdempotentProduceRequest{
		ProducerID: producer.ProducerID,
		Sequence:   2,
		Messages: []camutest.ProduceMessage{
			{Key: "k", Value: "v2"},
		},
	})
	if err != nil {
		t.Fatalf("next ProduceIdempotentToPartition() after failover error: %v", err)
	}
	if next.Duplicate {
		t.Fatal("next idempotent batch unexpectedly marked duplicate after failover")
	}
	if got, want := next.Offsets[0].Offset, uint64(2); got != want {
		t.Fatalf("next offset after failover = %d, want %d (leader idx %d)", got, want, newLeaderIdx)
	}
}
