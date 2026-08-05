package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

// TestDiskless_KafkaProduceMultiNode verifies that a diskless topic's partitions
// are all writable via Kafka when partition leaders are spread across several
// brokers. This exercises the metadata + produce routing for diskless topics in
// a multi-node cluster (each partition's leader is a different node).
func TestDiskless_KafkaProduceMultiNode(t *testing.T) {
	const instances = 3
	const partitions = 4

	ports := make([]int, instances)
	addrByBrokerID := make(map[int32]string, instances)
	instanceIDs := make([]string, instances)
	for i := 0; i < instances; i++ {
		ports[i] = freeTCPPort(t)
		instanceIDs[i] = fmt.Sprintf("127.0.0.%d", i+1)
	}

	env := camutest.New(t,
		camutest.WithInstances(instances),
		camutest.WithInstanceIDs(instanceIDs...),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			// Use the shared S3-backed metastore so segments registered by one
			// node's writer are visible to readers on the other nodes (the same
			// layout the deployed clusters use). The in-memory default is
			// per-instance and would make cross-node reads fail spuriously.
			cfg.Diskless.MetaStore = "s3"
			for i, id := range instanceIDs {
				if cfg.Server.InstanceID == id {
					cfg.Server.KafkaPort = ports[i]
				}
			}
		}),
	)
	defer env.Cleanup()

	for i, id := range instanceIDs {
		addrByBrokerID[kafkaBrokerIDForTest(id)] = fmt.Sprintf("127.0.0.1:%d", ports[i])
		waitForKafkaAddr(t, addrByBrokerID[kafkaBrokerIDForTest(id)])
	}

	// Create a diskless topic with 4 partitions via HTTP on the first node.
	client := env.Client()
	const topic = "diskless-kafka-multinode"
	createDisklessTopic(t, client, topic, partitions)

	for i := 0; i < instances; i++ {
		routing, err := env.ClientFor(i).GetRouting(topic)
		if err != nil {
			t.Fatalf("routing via node %d: %v", i, err)
		}
		for p := 0; p < partitions; p++ {
			info, ok := routing.Partitions[strconv.Itoa(p)]
			if !ok {
				t.Fatalf("routing via node %d: partition %d missing", i, p)
			}
			t.Logf("routing node=%d partition=%d leader=%s", i, p, info.InstanceID)
		}
	}

	// Seed the kgo client with all brokers; produce to every partition with the
	// benchmark-style client options (manual partitioner + all-ISR acks).
	brokers := make([]string, 0, instances)
	for _, addr := range addrByBrokerID {
		brokers = append(brokers, addr)
	}
	kc, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordDeliveryTimeout(30*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer kc.Close()

	// Wait until metadata reports all partitions with leaders.
	var lastMeta []kmsg.MetadataResponseTopicPartition
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mreq := kmsg.NewPtrMetadataRequest()
		mreq.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topic)}}
		mresp, err := mreq.RequestWith(context.Background(), kc)
		if err == nil && len(mresp.Topics) == 1 && len(mresp.Topics[0].Partitions) == partitions {
			lastMeta = mresp.Topics[0].Partitions
			allHaveLeaders := true
			for _, p := range lastMeta {
				if p.Leader == 0 {
					allHaveLeaders = false
				}
			}
			if allHaveLeaders {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, p := range lastMeta {
		t.Logf("metadata partition=%d leader=%d replicas=%v", p.Partition, p.Leader, p.Replicas)
	}
	if len(lastMeta) != partitions {
		t.Fatalf("metadata has %d partitions, want %d", len(lastMeta), partitions)
	}
	for _, p := range lastMeta {
		if p.Leader == 0 {
			t.Fatalf("metadata partition %d has no leader", p.Partition)
		}
	}

	// Produce to every partition; each must succeed (routed to its leader).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for p := 0; p < partitions; p++ {
		rec := &kgo.Record{Topic: topic, Partition: int32(p), Value: []byte(fmt.Sprintf("probe-%d", p))}
		rec, perr := kc.ProduceSync(ctx, rec).First()
		if perr != nil {
			t.Fatalf("produce partition %d: %v", p, perr)
		}
		if rec.Offset < 0 {
			t.Fatalf("produce partition %d: negative offset %d", p, rec.Offset)
		}
	}

	// Consume back everything via the HTTP client from each partition.
	for p := 0; p < partitions; p++ {
		deadline := time.Now().Add(10 * time.Second)
		var n int
		for time.Now().Before(deadline) {
			resp, err := client.Consume(topic, p, 0, 10)
			if err != nil {
				t.Fatalf("consume partition %d: %v", p, err)
			}
			n = len(resp.Messages)
			if n >= 1 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if n != 1 {
			t.Fatalf("consume partition %d: got %d messages, want 1", p, n)
		}
	}
}
