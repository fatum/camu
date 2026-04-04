//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

func TestKafkaProduceConsumeWithFranzGo(t *testing.T) {
	env, httpClient, client := newKafkaTestEnv(t, "kafka-e2e")
	defer env.Cleanup()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := client.ProduceSync(ctx,
		&kgo.Record{Topic: "kafka-e2e", Key: []byte("k1"), Value: []byte("hello")},
		&kgo.Record{Topic: "kafka-e2e", Key: []byte("k2"), Value: []byte("world")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("ProduceSync() error: %v", err)
	}

	got := collectKafkaValues(t, ctx, client, 2)
	if len(got) != 2 {
		t.Fatalf("consumed %d records, want 2", len(got))
	}
	if string(got[0]) != "hello" {
		t.Fatalf("first record value = %q, want %q", string(got[0]), "hello")
	}
	if string(got[1]) != "world" {
		t.Fatalf("second record value = %q, want %q", string(got[1]), "world")
	}

	resp, err := httpClient.Consume("kafka-e2e", 0, 0, 10)
	if err != nil {
		t.Fatalf("HTTP Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("HTTP consumed %d records, want 2", len(resp.Messages))
	}
	if resp.Messages[0].Value != "hello" || resp.Messages[1].Value != "world" {
		t.Fatalf("HTTP consume values = [%q %q], want [hello world]", resp.Messages[0].Value, resp.Messages[1].Value)
	}
}

func TestKafkaIdempotentProduceWithFranzGo(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	const topic = "kafka-idempotent-e2e"
	if err := httpClient.CreateTopic(topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", kafkaPort)),
		kgo.MaxVersions(kversion.V1_0_0()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient() error: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := client.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Key: []byte("k1"), Value: []byte("one")},
		&kgo.Record{Topic: topic, Key: []byte("k2"), Value: []byte("two")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("first ProduceSync() error: %v", err)
	}
	results = client.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Key: []byte("k3"), Value: []byte("three")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("second ProduceSync() error: %v", err)
	}

	resp, err := httpClient.Consume(topic, 0, 0, 10)
	if err != nil {
		t.Fatalf("HTTP Consume() error: %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("HTTP consumed %d records, want 3", len(resp.Messages))
	}
	if resp.Messages[0].Value != "one" || resp.Messages[1].Value != "two" || resp.Messages[2].Value != "three" {
		t.Fatalf("HTTP consume values = [%q %q %q], want [one two three]", resp.Messages[0].Value, resp.Messages[1].Value, resp.Messages[2].Value)
	}
}

func TestKafkaCompressedProduceWithFranzGo(t *testing.T) {
	tcs := []struct {
		name string
		opt  kgo.ProducerOpt
	}{
		{name: "snappy", opt: kgo.ProducerBatchCompression(kgo.SnappyCompression(), kgo.NoCompression())},
		{name: "gzip", opt: kgo.ProducerBatchCompression(kgo.GzipCompression(), kgo.NoCompression())},
		{name: "lz4", opt: kgo.ProducerBatchCompression(kgo.Lz4Compression(), kgo.NoCompression())},
		{name: "zstd", opt: kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.NoCompression())},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			kafkaPort := freeTCPPort(t)
			env := camutest.New(t,
				camutest.WithInstances(1),
				camutest.WithConfigMutator(func(cfg *config.Config) {
					cfg.Server.KafkaPort = kafkaPort
				}),
			)
			defer env.Cleanup()

			httpClient := env.Client()
			topic := "kafka-compressed-" + tc.name
			if err := httpClient.CreateTopic(topic, 1, 24*time.Hour); err != nil {
				t.Fatalf("CreateTopic() error: %v", err)
			}

			client, err := kgo.NewClient(
				kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", kafkaPort)),
				kgo.MaxVersions(kversion.V2_1_0()),
				tc.opt,
				kgo.DisableFetchSessions(),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topic: {0: kgo.NewOffset().At(0)},
				}),
			)
			if err != nil {
				t.Fatalf("kgo.NewClient() error: %v", err)
			}
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			results := client.ProduceSync(ctx,
				&kgo.Record{Topic: topic, Value: []byte("compressed-one")},
				&kgo.Record{Topic: topic, Value: []byte("compressed-two")},
			)
			if err := results.FirstErr(); err != nil {
				t.Fatalf("ProduceSync() error: %v", err)
			}

			got := collectKafkaValues(t, ctx, client, 2)
			if len(got) != 2 {
				t.Fatalf("consumed %d records, want 2", len(got))
			}
			if string(got[0]) != "compressed-one" || string(got[1]) != "compressed-two" {
				t.Fatalf("Kafka consume values = [%q %q], want [compressed-one compressed-two]", string(got[0]), string(got[1]))
			}

			resp, err := httpClient.Consume(topic, 0, 0, 10)
			if err != nil {
				t.Fatalf("HTTP Consume() error: %v", err)
			}
			if len(resp.Messages) != 2 {
				t.Fatalf("HTTP consumed %d records, want 2", len(resp.Messages))
			}
			if resp.Messages[0].Value != "compressed-one" || resp.Messages[1].Value != "compressed-two" {
				t.Fatalf("HTTP consume values = [%q %q], want [compressed-one compressed-two]", resp.Messages[0].Value, resp.Messages[1].Value)
			}
		})
	}
}

func TestKafkaConsumeTopicsWithListOffsets(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	const topic = "kafka-list-offsets-consume"
	if err := httpClient.CreateTopic(topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", kafkaPort)),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("producer kgo.NewClient() error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Value: []byte("alpha")},
		&kgo.Record{Topic: topic, Value: []byte("beta")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("ProduceSync() error: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", kafkaPort)),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableFetchSessions(),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer kgo.NewClient() error: %v", err)
	}
	defer consumer.Close()

	got := collectKafkaValues(t, ctx, consumer, 2)
	if len(got) != 2 {
		t.Fatalf("Kafka consumed %d records, want 2", len(got))
	}
	if string(got[0]) != "alpha" || string(got[1]) != "beta" {
		t.Fatalf("Kafka consume values = [%q %q], want [alpha beta]", string(got[0]), string(got[1]))
	}
}

func TestKafkaListOffsetsByTimestamp(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	const topic = "kafka-list-offsets-timestamp"
	if err := httpClient.CreateTopic(topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	if _, err := httpClient.Produce(topic, []camutest.ProduceMessage{{Value: "early"}}); err != nil {
		t.Fatalf("first HTTP Produce() error: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	midpointMs := time.Now().UnixMilli()
	time.Sleep(10 * time.Millisecond)
	if _, err := httpClient.Produce(topic, []camutest.ProduceMessage{{Value: "late"}}); err != nil {
		t.Fatalf("second HTTP Produce() error: %v", err)
	}

	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(1)
	req.Topics = []kmsg.ListOffsetsRequestTopic{{
		Topic: topic,
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
			Partition: 0,
			Timestamp: midpointMs,
		}},
	}}
	respAny, err := sendKafkaRequest(fmt.Sprintf("127.0.0.1:%d", kafkaPort), req)
	if err != nil {
		t.Fatalf("ListOffsets Request() error: %v", err)
	}
	resp := respAny.(*kmsg.ListOffsetsResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("ListOffsets response = %+v, want one topic/partition", resp.Topics)
	}
	part := resp.Topics[0].Partitions[0]
	if part.ErrorCode != 0 {
		t.Fatalf("ListOffsets error code = %d, want 0", part.ErrorCode)
	}
	if part.Offset != 1 {
		t.Fatalf("ListOffsets offset = %d, want 1", part.Offset)
	}
}

func TestKafkaFetchReportsOffsetWatermarks(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	const topic = "kafka-fetch-watermarks"
	if err := httpClient.CreateTopic(topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
	if _, err := httpClient.Produce(topic, []camutest.ProduceMessage{{Value: "one"}, {Value: "two"}}); err != nil {
		t.Fatalf("HTTP Produce() error: %v", err)
	}

	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(4)
	req.MinBytes = 1
	req.MaxWaitMillis = 100
	req.Topics = []kmsg.FetchRequestTopic{{
		Topic: topic,
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition:         0,
			FetchOffset:       0,
			PartitionMaxBytes: 4096,
		}},
	}}
	respAny, err := sendKafkaRequest(fmt.Sprintf("127.0.0.1:%d", kafkaPort), req)
	if err != nil {
		t.Fatalf("Fetch Request() error: %v", err)
	}
	resp := respAny.(*kmsg.FetchResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("Fetch response = %+v, want one topic/partition", resp.Topics)
	}
	part := resp.Topics[0].Partitions[0]
	if part.ErrorCode != 0 {
		t.Fatalf("Fetch error code = %d, want 0", part.ErrorCode)
	}
	if part.HighWatermark != 2 {
		t.Fatalf("Fetch high watermark = %d, want 2", part.HighWatermark)
	}
	if part.LastStableOffset != 2 {
		t.Fatalf("Fetch last stable offset = %d, want 2", part.LastStableOffset)
	}
	if len(part.RecordBatches) == 0 {
		t.Fatal("Fetch returned no record batches")
	}
}

func TestKafkaACLs(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	waitForKafkaAddr(t, addr)

	createReq := kmsg.NewPtrCreateACLsRequest()
	createReq.SetVersion(1)
	createReq.Creations = []kmsg.CreateACLsRequestCreation{{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        "orders",
		ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
		Principal:           "User:alice",
		Host:                "*",
		Operation:           kmsg.ACLOperationRead,
		PermissionType:      kmsg.ACLPermissionTypeAllow,
	}}
	createRespAny, err := sendKafkaRequest(addr, createReq)
	if err != nil {
		t.Fatalf("CreateACLs Request() error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateACLsResponse)
	if len(createResp.Results) != 1 || createResp.Results[0].ErrorCode != 0 {
		t.Fatalf("CreateACLs response = %+v, want one success result", createResp.Results)
	}

	describeReq := kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeTopic
	describeReq.ResourceName = kmsg.StringPtr("orders")
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
	describeReq.Operation = kmsg.ACLOperationAny
	describeReq.PermissionType = kmsg.ACLPermissionTypeAny
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeACLsResponse)
	if describeResp.ErrorCode != 0 {
		t.Fatalf("DescribeACLs error code = %d, want 0", describeResp.ErrorCode)
	}
	if len(describeResp.Resources) != 1 || len(describeResp.Resources[0].ACLs) != 1 {
		t.Fatalf("DescribeACLs resources = %+v, want one resource with one ACL", describeResp.Resources)
	}
	if describeResp.Resources[0].ResourceName != "orders" || describeResp.Resources[0].ACLs[0].Principal != "User:alice" {
		t.Fatalf("DescribeACLs resource = %+v, want topic orders for User:alice", describeResp.Resources[0])
	}

	deleteReq := kmsg.NewPtrDeleteACLsRequest()
	deleteReq.SetVersion(1)
	deleteReq.Filters = []kmsg.DeleteACLsRequestFilter{{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        kmsg.StringPtr("orders"),
		ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
		Operation:           kmsg.ACLOperationAny,
		PermissionType:      kmsg.ACLPermissionTypeAny,
	}}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteACLs Request() error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteACLsResponse)
	if len(deleteResp.Results) != 1 || deleteResp.Results[0].ErrorCode != 0 {
		t.Fatalf("DeleteACLs response = %+v, want one success result", deleteResp.Results)
	}
	if len(deleteResp.Results[0].MatchingACLs) != 1 {
		t.Fatalf("DeleteACLs matched ACLs = %+v, want 1", deleteResp.Results[0].MatchingACLs)
	}

	describeRespAny, err = sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs after delete Request() error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeACLsResponse)
	if len(describeResp.Resources) != 0 {
		t.Fatalf("DescribeACLs resources after delete = %+v, want none", describeResp.Resources)
	}
}

func TestKafkaCreateAndDeleteTopics(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	waitForKafkaAddr(t, addr)

	createReq := kmsg.NewPtrCreateTopicsRequest()
	createReq.SetVersion(5)
	retentionMs := "3600000"
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             "kafka-admin-topic",
		NumPartitions:     2,
		ReplicationFactor: 1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "retention.ms",
			Value: &retentionMs,
		}},
	}}
	createRespAny, err := sendKafkaRequest(addr, createReq)
	if err != nil {
		t.Fatalf("CreateTopics Request() error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateTopicsResponse)
	if len(createResp.Topics) != 1 {
		t.Fatalf("CreateTopics topics = %d, want 1", len(createResp.Topics))
	}
	if createResp.Topics[0].ErrorCode != 0 {
		t.Fatalf("CreateTopics error code = %d, want 0", createResp.Topics[0].ErrorCode)
	}

	topic, err := env.Client().GetTopic("kafka-admin-topic")
	if err != nil {
		t.Fatalf("HTTP GetTopic() after Kafka create error: %v", err)
	}
	if topic.Partitions != 2 {
		t.Fatalf("topic partitions = %d, want 2", topic.Partitions)
	}
	if topic.Retention != "1h0m0s" {
		t.Fatalf("topic retention = %q, want %q", topic.Retention, "1h0m0s")
	}

	deleteReq := kmsg.NewPtrDeleteTopicsRequest()
	deleteReq.SetVersion(5)
	deleteReq.TopicNames = []string{"kafka-admin-topic"}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteTopics Request() error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteTopicsResponse)
	if len(deleteResp.Topics) != 1 {
		t.Fatalf("DeleteTopics topics = %d, want 1", len(deleteResp.Topics))
	}
	if deleteResp.Topics[0].ErrorCode != 0 {
		t.Fatalf("DeleteTopics error code = %d, want 0", deleteResp.Topics[0].ErrorCode)
	}

	if _, err := env.Client().GetTopic("kafka-admin-topic"); err == nil {
		t.Fatal("expected topic to be deleted")
	}
}

func TestKafkaCreatePartitionsAndDescribeConfigs(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	if err := httpClient.CreateTopic("kafka-expand-topic", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	waitForKafkaAddr(t, addr)

	createReq := kmsg.NewPtrCreatePartitionsRequest()
	createReq.SetVersion(1)
	createReq.Topics = []kmsg.CreatePartitionsRequestTopic{{
		Topic: "kafka-expand-topic",
		Count: 3,
	}}
	createRespAny, err := sendKafkaRequest(addr, createReq)
	if err != nil {
		t.Fatalf("CreatePartitions Request() error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreatePartitionsResponse)
	if len(createResp.Topics) != 1 || createResp.Topics[0].ErrorCode != 0 {
		t.Fatalf("CreatePartitions response = %+v, want success", createResp.Topics)
	}

	topic, err := httpClient.GetTopic("kafka-expand-topic")
	if err != nil {
		t.Fatalf("HTTP GetTopic() after CreatePartitions error: %v", err)
	}
	if topic.Partitions != 3 {
		t.Fatalf("topic partitions = %d, want 3", topic.Partitions)
	}

	describeReq := kmsg.NewPtrDescribeConfigsRequest()
	describeReq.SetVersion(1)
	describeReq.Resources = []kmsg.DescribeConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-expand-topic",
		ConfigNames:  []string{"retention.ms", "min.insync.replicas"},
	}}
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeConfigs Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeConfigsResponse)
	if len(describeResp.Resources) != 1 {
		t.Fatalf("DescribeConfigs resources = %d, want 1", len(describeResp.Resources))
	}
	resource := describeResp.Resources[0]
	if resource.ErrorCode != 0 {
		t.Fatalf("DescribeConfigs error code = %d, want 0", resource.ErrorCode)
	}
	if len(resource.Configs) != 2 {
		t.Fatalf("DescribeConfigs configs = %d, want 2", len(resource.Configs))
	}
}

func TestKafkaAlterConfigsAndIncrementalAlterConfigs(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	if err := httpClient.CreateTopic("kafka-alter-topic", 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	waitForKafkaAddr(t, addr)

	retentionMs := "1800000"
	minISR := "1"
	alterReq := kmsg.NewPtrAlterConfigsRequest()
	alterReq.SetVersion(1)
	alterReq.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-alter-topic",
		Configs: []kmsg.AlterConfigsRequestResourceConfig{
			{Name: "retention.ms", Value: &retentionMs},
			{Name: "min.insync.replicas", Value: &minISR},
		},
	}}
	alterRespAny, err := sendKafkaRequest(addr, alterReq)
	if err != nil {
		t.Fatalf("AlterConfigs Request() error: %v", err)
	}
	alterResp := alterRespAny.(*kmsg.AlterConfigsResponse)
	if len(alterResp.Resources) != 1 || alterResp.Resources[0].ErrorCode != 0 {
		t.Fatalf("AlterConfigs response = %+v, want success", alterResp.Resources)
	}

	describeReq := kmsg.NewPtrDescribeConfigsRequest()
	describeReq.SetVersion(1)
	describeReq.Resources = []kmsg.DescribeConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-alter-topic",
		ConfigNames:  []string{"retention.ms"},
	}}
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeConfigs after AlterConfigs error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeConfigsResponse)
	if got := *describeResp.Resources[0].Configs[0].Value; got != "1800000" {
		t.Fatalf("retention.ms after AlterConfigs = %q, want %q", got, "1800000")
	}

	retentionMs2 := "600000"
	incReq := kmsg.NewPtrIncrementalAlterConfigsRequest()
	incReq.SetVersion(1)
	incReq.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-alter-topic",
		Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{{
			Name:  "retention.ms",
			Op:    kmsg.IncrementalAlterConfigOpSet,
			Value: &retentionMs2,
		}},
	}}
	incRespAny, err := sendKafkaRequest(addr, incReq)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs Request() error: %v", err)
	}
	incResp := incRespAny.(*kmsg.IncrementalAlterConfigsResponse)
	if len(incResp.Resources) != 1 || incResp.Resources[0].ErrorCode != 0 {
		t.Fatalf("IncrementalAlterConfigs response = %+v, want success", incResp.Resources)
	}

	describeRespAny, err = sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeConfigs after IncrementalAlterConfigs error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeConfigsResponse)
	if got := *describeResp.Resources[0].Configs[0].Value; got != "600000" {
		t.Fatalf("retention.ms after IncrementalAlterConfigs = %q, want %q", got, "600000")
	}
}

func TestKafkaDescribeCluster(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	waitForKafkaAddr(t, addr)

	req := kmsg.NewPtrDescribeClusterRequest()
	req.SetVersion(0)
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("DescribeCluster Request() error: %v", err)
	}
	resp := respAny.(*kmsg.DescribeClusterResponse)
	if resp.ErrorCode != 0 {
		t.Fatalf("DescribeCluster error code = %d, want 0", resp.ErrorCode)
	}
	if resp.ClusterID == "" {
		t.Fatal("expected non-empty cluster ID")
	}
	if resp.ControllerID == 0 {
		t.Fatal("expected non-zero controller ID")
	}
	if len(resp.Brokers) != 1 {
		t.Fatalf("DescribeCluster brokers = %d, want 1", len(resp.Brokers))
	}
	if resp.Brokers[0].Host != "127.0.0.1" {
		t.Fatalf("DescribeCluster host = %q, want %q", resp.Brokers[0].Host, "127.0.0.1")
	}
}

func TestKafkaConsumerGroupConsumeWithFranzGo(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	httpClient := env.Client()
	const topic = "kafka-group-consume"
	if err := httpClient.CreateTopic(topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	seedBroker := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(seedBroker),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("producer kgo.NewClient() error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Value: []byte("g1")},
		&kgo.Record{Topic: topic, Value: []byte("g2")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("ProduceSync() error: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seedBroker),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableFetchSessions(),
		kgo.ConsumerGroup("group-1"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("consumer kgo.NewClient() error: %v", err)
	}
	defer consumer.Close()

	got := collectKafkaValues(t, ctx, consumer, 2)
	if len(got) != 2 {
		t.Fatalf("Kafka consumer group consumed %d records, want 2", len(got))
	}
	if string(got[0]) != "g1" || string(got[1]) != "g2" {
		t.Fatalf("Kafka consumer group values = [%q %q], want [g1 g2]", string(got[0]), string(got[1]))
	}
}

func TestKafkaProduceHTTPConsume(t *testing.T) {
	env, httpClient, client := newKafkaTestEnv(t, "kafka-http-bridge")
	defer env.Cleanup()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := client.ProduceSync(ctx,
		&kgo.Record{Topic: "kafka-http-bridge", Value: []byte("alpha")},
		&kgo.Record{Topic: "kafka-http-bridge", Value: []byte("beta")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("ProduceSync() error: %v", err)
	}

	resp, err := httpClient.Consume("kafka-http-bridge", 0, 0, 10)
	if err != nil {
		t.Fatalf("HTTP Consume() error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("HTTP consumed %d records, want 2", len(resp.Messages))
	}
	if resp.Messages[0].Value != "alpha" || resp.Messages[1].Value != "beta" {
		t.Fatalf("HTTP consume values = [%q %q], want [alpha beta]", resp.Messages[0].Value, resp.Messages[1].Value)
	}
}

func TestHTTPProduceKafkaConsume(t *testing.T) {
	env, httpClient, client := newKafkaTestEnv(t, "http-kafka-bridge")
	defer env.Cleanup()
	defer client.Close()

	_, err := httpClient.Produce("http-kafka-bridge", []camutest.ProduceMessage{
		{Value: "left"},
		{Value: "right"},
	})
	if err != nil {
		t.Fatalf("HTTP Produce() error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got := collectKafkaValues(t, ctx, client, 2)
	if len(got) != 2 {
		t.Fatalf("Kafka consumed %d records, want 2", len(got))
	}
	if string(got[0]) != "left" || string(got[1]) != "right" {
		t.Fatalf("Kafka consume values = [%q %q], want [left right]", string(got[0]), string(got[1]))
	}
}
