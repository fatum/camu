//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

const kafkaInvalidConfigCode int16 = 40

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func TestKafkaProduceConsumeWithFranzGo(t *testing.T) {
	env, httpClient, client, _ := newKafkaTopicBootstrappedEnv(t, "kafka-e2e")
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
	const topic = "kafka-idempotent-e2e"
	env, httpClient, addr := newKafkaFixtureEnv(t, topic)
	defer env.Cleanup()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
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

func TestKafkaInitProducerIDIntegration(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

	req := kmsg.NewPtrInitProducerIDRequest()
	req.SetVersion(4)
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("InitProducerID Request() error: %v", err)
	}
	resp := respAny.(*kmsg.InitProducerIDResponse)
	if resp.ErrorCode != 0 {
		t.Fatalf("InitProducerID error code = %d, want 0", resp.ErrorCode)
	}
	if resp.ProducerID < 0 {
		t.Fatalf("InitProducerID producer id = %d, want non-negative", resp.ProducerID)
	}
	if resp.ProducerEpoch != 0 {
		t.Fatalf("InitProducerID producer epoch = %d, want 0", resp.ProducerEpoch)
	}

	txnID := "txn-1"
	req = kmsg.NewPtrInitProducerIDRequest()
	req.SetVersion(4)
	req.TransactionalID = &txnID
	respAny, err = sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("InitProducerID transactional Request() error: %v", err)
	}
	resp = respAny.(*kmsg.InitProducerIDResponse)
	if resp.ErrorCode != 42 {
		t.Fatalf("InitProducerID transactional error code = %d, want 42", resp.ErrorCode)
	}
	if resp.ProducerID != -1 {
		t.Fatalf("InitProducerID transactional producer id = %d, want -1", resp.ProducerID)
	}
	if resp.ProducerEpoch != -1 {
		t.Fatalf("InitProducerID transactional producer epoch = %d, want -1", resp.ProducerEpoch)
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
			topic := "kafka-compressed-" + tc.name
			env, httpClient, addr := newKafkaFixtureEnv(t, topic)
			defer env.Cleanup()

			client, err := kgo.NewClient(
				kgo.SeedBrokers(addr),
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
	const topic = "kafka-list-offsets-consume"
	env, _, addr := newKafkaFixtureEnv(t, topic)
	defer env.Cleanup()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
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
		kgo.SeedBrokers(addr),
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
	const topic = "kafka-list-offsets-timestamp"
	env, httpClient, addr := newKafkaFixtureEnv(t, topic)
	defer env.Cleanup()

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
	respAny, err := sendKafkaRequest(addr, req)
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
	const topic = "kafka-fetch-watermarks"
	env, httpClient, addr := newKafkaFixtureEnv(t, topic)
	defer env.Cleanup()
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
	respAny, err := sendKafkaRequest(addr, req)
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
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

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

func TestKafkaACLsRejectInvalidRequests(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

	createReq := kmsg.NewPtrCreateACLsRequest()
	createReq.SetVersion(1)
	createReq.Creations = []kmsg.CreateACLsRequestCreation{{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        "orders",
		ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
		Principal:           "User:alice",
		Host:                "*",
		Operation:           kmsg.ACLOperationAny,
		PermissionType:      kmsg.ACLPermissionTypeAllow,
	}}
	createRespAny, err := sendKafkaRequest(addr, createReq)
	if err != nil {
		t.Fatalf("CreateACLs Request() error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateACLsResponse)
	if len(createResp.Results) != 1 {
		t.Fatalf("CreateACLs results = %d, want 1", len(createResp.Results))
	}
	if createResp.Results[0].ErrorCode != 42 {
		t.Fatalf("CreateACLs error code = %d, want 42", createResp.Results[0].ErrorCode)
	}

	describeReq := kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeTopic
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeUnknown
	describeReq.Operation = kmsg.ACLOperationAny
	describeReq.PermissionType = kmsg.ACLPermissionTypeAny
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeACLsResponse)
	if describeResp.ErrorCode != 42 {
		t.Fatalf("DescribeACLs error code = %d, want 42", describeResp.ErrorCode)
	}

	deleteReq := kmsg.NewPtrDeleteACLsRequest()
	deleteReq.SetVersion(1)
	deleteReq.Filters = []kmsg.DeleteACLsRequestFilter{{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        kmsg.StringPtr("orders"),
		ResourcePatternType: kmsg.ACLResourcePatternTypeUnknown,
		Operation:           kmsg.ACLOperationAny,
		PermissionType:      kmsg.ACLPermissionTypeAny,
	}}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteACLs Request() error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteACLsResponse)
	if len(deleteResp.Results) != 1 {
		t.Fatalf("DeleteACLs results = %d, want 1", len(deleteResp.Results))
	}
	if deleteResp.Results[0].ErrorCode != 42 {
		t.Fatalf("DeleteACLs error code = %d, want 42", deleteResp.Results[0].ErrorCode)
	}
}

func TestKafkaACLsFilterMatrix(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

	createReq := kmsg.NewPtrCreateACLsRequest()
	createReq.SetVersion(1)
	createReq.Creations = []kmsg.CreateACLsRequestCreation{
		{
			ResourceType:        kmsg.ACLResourceTypeTopic,
			ResourceName:        "orders",
			ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
			Principal:           "User:alice",
			Host:                "*",
			Operation:           kmsg.ACLOperationRead,
			PermissionType:      kmsg.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kmsg.ACLResourceTypeTopic,
			ResourceName:        "payments-",
			ResourcePatternType: kmsg.ACLResourcePatternTypePrefixed,
			Principal:           "User:bob",
			Host:                "*",
			Operation:           kmsg.ACLOperationWrite,
			PermissionType:      kmsg.ACLPermissionTypeDeny,
		},
	}
	createRespAny, err := sendKafkaRequest(addr, createReq)
	if err != nil {
		t.Fatalf("CreateACLs Request() error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateACLsResponse)
	if len(createResp.Results) != 2 {
		t.Fatalf("CreateACLs results = %d, want 2", len(createResp.Results))
	}
	for i, result := range createResp.Results {
		if result.ErrorCode != 0 {
			t.Fatalf("CreateACLs result %d error code = %d, want 0", i, result.ErrorCode)
		}
	}

	describeReq := kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeTopic
	describeReq.ResourceName = strPtr("payments-prod")
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeMatch
	describeReq.Operation = kmsg.ACLOperationAny
	describeReq.PermissionType = kmsg.ACLPermissionTypeAny
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs MATCH Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeACLsResponse)
	if describeResp.ErrorCode != 0 {
		t.Fatalf("DescribeACLs MATCH error code = %d, want 0", describeResp.ErrorCode)
	}
	if len(describeResp.Resources) != 1 || len(describeResp.Resources[0].ACLs) != 1 {
		t.Fatalf("DescribeACLs MATCH resources = %+v, want one prefixed ACL", describeResp.Resources)
	}
	if describeResp.Resources[0].ResourceName != "payments-" {
		t.Fatalf("DescribeACLs MATCH resource name = %q, want %q", describeResp.Resources[0].ResourceName, "payments-")
	}
	if describeResp.Resources[0].ResourcePatternType != kmsg.ACLResourcePatternTypePrefixed {
		t.Fatalf("DescribeACLs MATCH pattern type = %d, want prefixed", describeResp.Resources[0].ResourcePatternType)
	}
	if describeResp.Resources[0].ACLs[0].Principal != "User:bob" {
		t.Fatalf("DescribeACLs MATCH principal = %q, want %q", describeResp.Resources[0].ACLs[0].Principal, "User:bob")
	}

	deleteReq := kmsg.NewPtrDeleteACLsRequest()
	deleteReq.SetVersion(1)
	deleteReq.Filters = []kmsg.DeleteACLsRequestFilter{{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        strPtr("payments-prod"),
		ResourcePatternType: kmsg.ACLResourcePatternTypeMatch,
		Operation:           kmsg.ACLOperationAny,
		PermissionType:      kmsg.ACLPermissionTypeAny,
	}}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteACLs MATCH Request() error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteACLsResponse)
	if len(deleteResp.Results) != 1 || deleteResp.Results[0].ErrorCode != 0 {
		t.Fatalf("DeleteACLs MATCH response = %+v, want one success result", deleteResp.Results)
	}
	if len(deleteResp.Results[0].MatchingACLs) != 1 {
		t.Fatalf("DeleteACLs MATCH matching ACLs = %+v, want 1", deleteResp.Results[0].MatchingACLs)
	}
	if deleteResp.Results[0].MatchingACLs[0].ResourceName != "payments-" {
		t.Fatalf("DeleteACLs MATCH resource name = %q, want %q", deleteResp.Results[0].MatchingACLs[0].ResourceName, "payments-")
	}

	describeReq = kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeTopic
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	describeReq.Operation = kmsg.ACLOperationAny
	describeReq.PermissionType = kmsg.ACLPermissionTypeAny
	describeRespAny, err = sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs after delete Request() error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeACLsResponse)
	if len(describeResp.Resources) != 1 || describeResp.Resources[0].ResourceName != "orders" {
		t.Fatalf("DescribeACLs after delete resources = %+v, want only literal orders ACL", describeResp.Resources)
	}
}

func TestKafkaACLsResourceAndOperationMatrix(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

	createReq := kmsg.NewPtrCreateACLsRequest()
	createReq.SetVersion(1)
	createReq.Creations = []kmsg.CreateACLsRequestCreation{
		{
			ResourceType:        kmsg.ACLResourceTypeGroup,
			ResourceName:        "group-alpha",
			ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
			Principal:           "User:carol",
			Host:                "*",
			Operation:           kmsg.ACLOperationRead,
			PermissionType:      kmsg.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kmsg.ACLResourceTypeCluster,
			ResourceName:        "kafka-cluster",
			ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
			Principal:           "User:dave",
			Host:                "*",
			Operation:           kmsg.ACLOperationAlter,
			PermissionType:      kmsg.ACLPermissionTypeDeny,
		},
	}
	createRespAny, err := sendKafkaRequest(addr, createReq)
	if err != nil {
		t.Fatalf("CreateACLs Request() error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateACLsResponse)
	if len(createResp.Results) != 2 {
		t.Fatalf("CreateACLs results = %d, want 2", len(createResp.Results))
	}
	for i, result := range createResp.Results {
		if result.ErrorCode != 0 {
			t.Fatalf("CreateACLs result %d error code = %d, want 0", i, result.ErrorCode)
		}
	}

	describeReq := kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeGroup
	describeReq.ResourceName = strPtr("group-alpha")
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
	describeReq.Operation = kmsg.ACLOperationRead
	describeReq.PermissionType = kmsg.ACLPermissionTypeAllow
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs group Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeACLsResponse)
	if describeResp.ErrorCode != 0 {
		t.Fatalf("DescribeACLs group error code = %d, want 0", describeResp.ErrorCode)
	}
	if len(describeResp.Resources) != 1 || len(describeResp.Resources[0].ACLs) != 1 {
		t.Fatalf("DescribeACLs group resources = %+v, want one group ACL", describeResp.Resources)
	}
	if describeResp.Resources[0].ResourceType != kmsg.ACLResourceTypeGroup {
		t.Fatalf("DescribeACLs group resource type = %d, want group", describeResp.Resources[0].ResourceType)
	}
	if describeResp.Resources[0].ACLs[0].Operation != kmsg.ACLOperationRead {
		t.Fatalf("DescribeACLs group operation = %d, want read", describeResp.Resources[0].ACLs[0].Operation)
	}

	describeReq = kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeCluster
	describeReq.ResourceName = strPtr("kafka-cluster")
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
	describeReq.Operation = kmsg.ACLOperationAlter
	describeReq.PermissionType = kmsg.ACLPermissionTypeDeny
	describeRespAny, err = sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs cluster Request() error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeACLsResponse)
	if len(describeResp.Resources) != 1 || len(describeResp.Resources[0].ACLs) != 1 {
		t.Fatalf("DescribeACLs cluster resources = %+v, want one cluster ACL", describeResp.Resources)
	}
	if describeResp.Resources[0].ResourceType != kmsg.ACLResourceTypeCluster {
		t.Fatalf("DescribeACLs cluster resource type = %d, want cluster", describeResp.Resources[0].ResourceType)
	}
	if describeResp.Resources[0].ACLs[0].PermissionType != kmsg.ACLPermissionTypeDeny {
		t.Fatalf("DescribeACLs cluster permission type = %d, want deny", describeResp.Resources[0].ACLs[0].PermissionType)
	}

	deleteReq := kmsg.NewPtrDeleteACLsRequest()
	deleteReq.SetVersion(1)
	deleteReq.Filters = []kmsg.DeleteACLsRequestFilter{{
		ResourceType:        kmsg.ACLResourceTypeGroup,
		ResourceName:        strPtr("group-alpha"),
		ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
		Operation:           kmsg.ACLOperationRead,
		PermissionType:      kmsg.ACLPermissionTypeAllow,
	}}
	deleteRespAny, err := sendKafkaRequest(addr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteACLs group Request() error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteACLsResponse)
	if len(deleteResp.Results) != 1 || deleteResp.Results[0].ErrorCode != 0 {
		t.Fatalf("DeleteACLs group response = %+v, want one success result", deleteResp.Results)
	}
	if len(deleteResp.Results[0].MatchingACLs) != 1 || deleteResp.Results[0].MatchingACLs[0].ResourceType != kmsg.ACLResourceTypeGroup {
		t.Fatalf("DeleteACLs group matching ACLs = %+v, want one group ACL", deleteResp.Results[0].MatchingACLs)
	}

	describeReq = kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeAny
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	describeReq.Operation = kmsg.ACLOperationAny
	describeReq.PermissionType = kmsg.ACLPermissionTypeAny
	describeRespAny, err = sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs any Request() error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeACLsResponse)
	if len(describeResp.Resources) != 1 || describeResp.Resources[0].ResourceType != kmsg.ACLResourceTypeCluster {
		t.Fatalf("DescribeACLs any resources = %+v, want only cluster ACL to remain", describeResp.Resources)
	}
}

func TestKafkaCreateAndDeleteTopics(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

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

	meta := fetchKafkaTopicMetadata(t, addr, "kafka-admin-topic")
	if meta.ErrorCode != 0 {
		t.Fatalf("metadata error code = %d, want 0", meta.ErrorCode)
	}
	if len(meta.Partitions) != 2 {
		t.Fatalf("metadata partitions = %d, want 2", len(meta.Partitions))
	}
	configs := fetchKafkaTopicConfigValues(t, addr, "kafka-admin-topic", "retention.ms")
	if configs["retention.ms"] != "3600000" {
		t.Fatalf("retention.ms = %q, want %q", configs["retention.ms"], "3600000")
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

	meta = fetchKafkaTopicMetadata(t, addr, "kafka-admin-topic")
	if meta.ErrorCode != 3 {
		t.Fatalf("metadata error after delete = %d, want 3", meta.ErrorCode)
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

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "kafka-expand-topic", 1)

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

	// Dedicated partition tests cover no-shrink and readiness on newly added
	// partitions. This test keeps the config-read path exercised after expansion.
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

func TestKafkaDescribeConfigsFiltersUnknownConfigNames(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-describe-config-filter")
	defer env.Cleanup()

	req := kmsg.NewPtrDescribeConfigsRequest()
	req.SetVersion(1)
	req.Resources = []kmsg.DescribeConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-describe-config-filter",
		ConfigNames:  []string{"retention.ms", "unsupported.config.name", "camu.storage.mode"},
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("DescribeConfigs Request() error: %v", err)
	}
	resp := respAny.(*kmsg.DescribeConfigsResponse)
	if len(resp.Resources) != 1 {
		t.Fatalf("DescribeConfigs resources = %d, want 1", len(resp.Resources))
	}
	resource := resp.Resources[0]
	if resource.ErrorCode != 0 {
		t.Fatalf("DescribeConfigs error code = %d, want 0", resource.ErrorCode)
	}
	if len(resource.Configs) != 2 {
		t.Fatalf("DescribeConfigs configs = %d, want 2 supported configs", len(resource.Configs))
	}
	got := map[string]string{}
	for _, cfg := range resource.Configs {
		got[cfg.Name] = stringValue(cfg.Value)
	}
	if got["retention.ms"] != "604800000" {
		t.Fatalf("retention.ms = %q, want %q", got["retention.ms"], "604800000")
	}
	if got["camu.storage.mode"] != "classic" {
		t.Fatalf("camu.storage.mode = %q, want %q", got["camu.storage.mode"], "classic")
	}
	if _, ok := got["unsupported.config.name"]; ok {
		t.Fatalf("DescribeConfigs returned unsupported config unexpectedly: %+v", got)
	}
}

func TestKafkaCreatePartitionsRejectsDecreaseAndLeavesMetadataUnchanged(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, addr, "kafka-no-shrink-topic", 3)

	req := kmsg.NewPtrCreatePartitionsRequest()
	req.SetVersion(1)
	req.Topics = []kmsg.CreatePartitionsRequestTopic{{
		Topic: "kafka-no-shrink-topic",
		Count: 2,
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("CreatePartitions Request() error: %v", err)
	}
	resp := respAny.(*kmsg.CreatePartitionsResponse)
	if len(resp.Topics) != 1 {
		t.Fatalf("CreatePartitions topics = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != 37 {
		t.Fatalf("CreatePartitions error code = %d, want 37", resp.Topics[0].ErrorCode)
	}
	if !strings.Contains(stringValue(resp.Topics[0].ErrorMessage), "partition count must increase") {
		t.Fatalf("CreatePartitions error message = %q, want increase guidance", stringValue(resp.Topics[0].ErrorMessage))
	}

	meta := fetchKafkaTopicMetadata(t, addr, "kafka-no-shrink-topic")
	if meta.ErrorCode != 0 {
		t.Fatalf("metadata error code = %d, want 0", meta.ErrorCode)
	}
	if len(meta.Partitions) != 3 {
		t.Fatalf("metadata partitions after rejected decrease = %d, want 3", len(meta.Partitions))
	}
}

func TestCreatePartitionsClassicNewPartitionIsReady(t *testing.T) {
	env, client, addr := newKafkaFixtureEnv(t, "classic-partition-ready")
	defer env.Cleanup()

	req := kmsg.NewPtrCreatePartitionsRequest()
	req.SetVersion(1)
	req.Topics = []kmsg.CreatePartitionsRequestTopic{{
		Topic: "classic-partition-ready",
		Count: 3,
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("CreatePartitions Request() error: %v", err)
	}
	resp := respAny.(*kmsg.CreatePartitionsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("CreatePartitions response = %+v, want success", resp.Topics)
	}

	waitForPartitionProduceReady(t, client, "classic-partition-ready", 2)
	if _, err := client.ProduceToPartition("classic-partition-ready", 2, []camutest.ProduceMessage{{Value: "classic-ready"}}); err != nil {
		t.Fatalf("ProduceToPartition(new classic partition) error: %v", err)
	}

	consumeResp, err := client.Consume("classic-partition-ready", 2, 1, 10)
	if err != nil {
		t.Fatalf("Consume(new classic partition) error: %v", err)
	}
	if len(consumeResp.Messages) != 1 {
		t.Fatalf("consumed %d messages from new classic partition, want 1", len(consumeResp.Messages))
	}
	if consumeResp.Messages[0].Value != "classic-ready" {
		t.Fatalf("new classic partition value = %q, want %q", consumeResp.Messages[0].Value, "classic-ready")
	}
}

func TestCreatePartitionsDisklessNewPartitionIsReady(t *testing.T) {
	env, client, addr := newDisklessKafkaEnv(t, "diskless-partition-ready")
	defer env.Cleanup()

	req := kmsg.NewPtrCreatePartitionsRequest()
	req.SetVersion(1)
	req.Topics = []kmsg.CreatePartitionsRequestTopic{{
		Topic: "diskless-partition-ready",
		Count: 3,
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("CreatePartitions Request() error: %v", err)
	}
	resp := respAny.(*kmsg.CreatePartitionsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("CreatePartitions response = %+v, want success", resp.Topics)
	}

	waitForPartitionProduceReady(t, client, "diskless-partition-ready", 2)
	if _, err := client.ProduceToPartition("diskless-partition-ready", 2, []camutest.ProduceMessage{{Value: "diskless-ready"}}); err != nil {
		t.Fatalf("ProduceToPartition(new diskless partition) error: %v", err)
	}

	consumeResp, err := client.Consume("diskless-partition-ready", 2, 1, 10)
	if err != nil {
		t.Fatalf("Consume(new diskless partition) error: %v", err)
	}
	if len(consumeResp.Messages) != 1 {
		t.Fatalf("consumed %d messages from new diskless partition, want 1", len(consumeResp.Messages))
	}
	if consumeResp.Messages[0].Value != "diskless-ready" {
		t.Fatalf("new diskless partition value = %q, want %q", consumeResp.Messages[0].Value, "diskless-ready")
	}
}

func TestKafkaCreateDisklessTopicAndDescribeStorageMode(t *testing.T) {
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

	disklessMode := "diskless"
	createReq := kmsg.NewPtrCreateTopicsRequest()
	createReq.SetVersion(5)
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             "kafka-admin-diskless-topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "camu.storage.mode",
			Value: &disklessMode,
		}},
	}}
	createRespAny, err := sendKafkaRequest(addr, createReq)
	if err != nil {
		t.Fatalf("CreateTopics Request() error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateTopicsResponse)
	if len(createResp.Topics) != 1 || createResp.Topics[0].ErrorCode != 0 {
		t.Fatalf("CreateTopics response = %+v, want success", createResp.Topics)
	}

	got := fetchKafkaTopicConfigValues(t, addr, "kafka-admin-diskless-topic", "camu.storage.mode")["camu.storage.mode"]
	if got != "diskless" {
		t.Fatalf("camu.storage.mode = %q, want %q", got, "diskless")
	}
}

func TestKafkaCreateTopicsRejectsRetentionBytes(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

	retentionBytes := "1024"
	createReq := kmsg.NewPtrCreateTopicsRequest()
	createReq.SetVersion(5)
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             "kafka-retention-bytes-topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "retention.bytes",
			Value: &retentionBytes,
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
	if createResp.Topics[0].ErrorCode != kafkaInvalidConfigCode {
		t.Fatalf("CreateTopics error code = %d, want %d", createResp.Topics[0].ErrorCode, kafkaInvalidConfigCode)
	}
	if !strings.Contains(stringValue(createResp.Topics[0].ErrorMessage), "time-based retention only") {
		t.Fatalf("CreateTopics error message = %q, want time-based retention guidance", stringValue(createResp.Topics[0].ErrorMessage))
	}
}

func TestKafkaCreateTopicsValidateOnlyDoesNotCreateTopic(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

	retentionMs := "3600000"
	createReq := kmsg.NewPtrCreateTopicsRequest()
	createReq.SetVersion(5)
	createReq.ValidateOnly = true
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             "kafka-validate-only-create",
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
	if len(createResp.Topics) != 1 || createResp.Topics[0].ErrorCode != 0 {
		t.Fatalf("CreateTopics response = %+v, want success", createResp.Topics)
	}

	meta := fetchKafkaTopicMetadata(t, addr, "kafka-validate-only-create")
	if meta.ErrorCode != 3 {
		t.Fatalf("metadata error after validate-only create = %d, want 3", meta.ErrorCode)
	}
}

func TestKafkaAlterConfigsAndIncrementalAlterConfigs(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-alter-topic")
	defer env.Cleanup()

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

func TestKafkaCreatePartitionsValidateOnlyDoesNotMutate(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-validate-only-partitions")
	defer env.Cleanup()

	req := kmsg.NewPtrCreatePartitionsRequest()
	req.SetVersion(1)
	req.ValidateOnly = true
	req.Topics = []kmsg.CreatePartitionsRequestTopic{{
		Topic: "kafka-validate-only-partitions",
		Count: 3,
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("CreatePartitions Request() error: %v", err)
	}
	resp := respAny.(*kmsg.CreatePartitionsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("CreatePartitions response = %+v, want success", resp.Topics)
	}

	meta := fetchKafkaTopicMetadata(t, addr, "kafka-validate-only-partitions")
	if meta.ErrorCode != 0 {
		t.Fatalf("metadata error code = %d, want 0", meta.ErrorCode)
	}
	if len(meta.Partitions) != 1 {
		t.Fatalf("metadata partitions after validate-only create partitions = %d, want 1", len(meta.Partitions))
	}
}

func TestKafkaCreatePartitionsRejectsManualReplicaAssignment(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-manual-assignment")
	defer env.Cleanup()

	req := kmsg.NewPtrCreatePartitionsRequest()
	req.SetVersion(1)
	req.Topics = []kmsg.CreatePartitionsRequestTopic{{
		Topic: "kafka-manual-assignment",
		Count: 3,
		Assignment: []kmsg.CreatePartitionsRequestTopicAssignment{{
			Replicas: []int32{1},
		}},
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("CreatePartitions Request() error: %v", err)
	}
	resp := respAny.(*kmsg.CreatePartitionsResponse)
	if len(resp.Topics) != 1 {
		t.Fatalf("CreatePartitions topics = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != 39 {
		t.Fatalf("CreatePartitions error code = %d, want 39", resp.Topics[0].ErrorCode)
	}
	if !strings.Contains(stringValue(resp.Topics[0].ErrorMessage), "manual partition assignment") {
		t.Fatalf("CreatePartitions error message = %q, want manual partition assignment guidance", stringValue(resp.Topics[0].ErrorMessage))
	}

	meta := fetchKafkaTopicMetadata(t, addr, "kafka-manual-assignment")
	if meta.ErrorCode != 0 {
		t.Fatalf("metadata error code = %d, want 0", meta.ErrorCode)
	}
	if len(meta.Partitions) != 1 {
		t.Fatalf("metadata partitions after rejected manual assignment = %d, want 1", len(meta.Partitions))
	}
}

func TestKafkaAlterConfigsValidateOnlyDoesNotMutate(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-validate-only-alter")
	defer env.Cleanup()

	retentionMs := "1800000"
	req := kmsg.NewPtrAlterConfigsRequest()
	req.SetVersion(1)
	req.ValidateOnly = true
	req.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-validate-only-alter",
		Configs: []kmsg.AlterConfigsRequestResourceConfig{{
			Name:  "retention.ms",
			Value: &retentionMs,
		}},
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("AlterConfigs Request() error: %v", err)
	}
	resp := respAny.(*kmsg.AlterConfigsResponse)
	if len(resp.Resources) != 1 || resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("AlterConfigs response = %+v, want success", resp.Resources)
	}

	configs := fetchKafkaTopicConfigValues(t, addr, "kafka-validate-only-alter", "retention.ms")
	if configs["retention.ms"] != "604800000" {
		t.Fatalf("retention.ms after validate-only alter = %q, want %q", configs["retention.ms"], "604800000")
	}
}

func TestKafkaIncrementalAlterConfigsValidateOnlyDoesNotMutate(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-validate-only-inc-alter")
	defer env.Cleanup()

	retentionMs := "600000"
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.SetVersion(1)
	req.ValidateOnly = true
	req.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-validate-only-inc-alter",
		Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{{
			Name:  "retention.ms",
			Op:    kmsg.IncrementalAlterConfigOpSet,
			Value: &retentionMs,
		}},
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs Request() error: %v", err)
	}
	resp := respAny.(*kmsg.IncrementalAlterConfigsResponse)
	if len(resp.Resources) != 1 || resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("IncrementalAlterConfigs response = %+v, want success", resp.Resources)
	}

	configs := fetchKafkaTopicConfigValues(t, addr, "kafka-validate-only-inc-alter", "retention.ms")
	if configs["retention.ms"] != "604800000" {
		t.Fatalf("retention.ms after validate-only incremental alter = %q, want %q", configs["retention.ms"], "604800000")
	}
}

func TestKafkaSupportedTopicConfigsRoundTrip(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)

	env := camutest.New(t,
		camutest.WithInstances(2),
		camutest.WithInstanceIDs("127.0.0.1", "127.0.0.2"),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			switch cfg.Server.InstanceID {
			case "127.0.0.1":
				cfg.Server.KafkaPort = port1
			case "127.0.0.2":
				cfg.Server.KafkaPort = port2
			}
		}),
	)
	defer env.Cleanup()

	addrByBrokerID := map[int32]string{
		kafkaBrokerIDForTest("127.0.0.1"): fmt.Sprintf("127.0.0.1:%d", port1),
		kafkaBrokerIDForTest("127.0.0.2"): fmt.Sprintf("127.0.0.1:%d", port2),
	}
	controllerAddr, _ := waitForKafkaControllerAndFollowerAddrs(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID)

	retentionMs := "3600000"
	minISR := "2"
	uncleanTrue := "true"
	createKafkaFixtureTopicWithReplicationAndConfigs(t, controllerAddr, "kafka-config-roundtrip", 1, 2, []kmsg.CreateTopicsRequestTopicConfig{
		{Name: "cleanup.policy", Value: strPtr("delete")},
		{Name: "retention.ms", Value: &retentionMs},
		{Name: "min.insync.replicas", Value: &minISR},
		{Name: "unclean.leader.election.enable", Value: &uncleanTrue},
	})

	configs := fetchKafkaTopicConfigValues(t, controllerAddr, "kafka-config-roundtrip",
		"cleanup.policy",
		"retention.ms",
		"min.insync.replicas",
		"unclean.leader.election.enable",
	)
	if configs["cleanup.policy"] != "delete" {
		t.Fatalf("cleanup.policy after create = %q, want %q", configs["cleanup.policy"], "delete")
	}
	if configs["retention.ms"] != "3600000" {
		t.Fatalf("retention.ms after create = %q, want %q", configs["retention.ms"], "3600000")
	}
	if configs["min.insync.replicas"] != "2" {
		t.Fatalf("min.insync.replicas after create = %q, want %q", configs["min.insync.replicas"], "2")
	}
	if configs["unclean.leader.election.enable"] != "true" {
		t.Fatalf("unclean.leader.election.enable after create = %q, want %q", configs["unclean.leader.election.enable"], "true")
	}

	retentionMs2 := "1800000"
	minISR2 := "1"
	uncleanFalse := "false"
	alterReq := kmsg.NewPtrAlterConfigsRequest()
	alterReq.SetVersion(1)
	alterReq.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-config-roundtrip",
		Configs: []kmsg.AlterConfigsRequestResourceConfig{
			{Name: "retention.ms", Value: &retentionMs2},
			{Name: "min.insync.replicas", Value: &minISR2},
			{Name: "unclean.leader.election.enable", Value: &uncleanFalse},
		},
	}}
	alterRespAny, err := sendKafkaRequest(controllerAddr, alterReq)
	if err != nil {
		t.Fatalf("AlterConfigs Request() error: %v", err)
	}
	alterResp := alterRespAny.(*kmsg.AlterConfigsResponse)
	if len(alterResp.Resources) != 1 || alterResp.Resources[0].ErrorCode != 0 {
		t.Fatalf("AlterConfigs response = %+v, want success", alterResp.Resources)
	}

	configs = fetchKafkaTopicConfigValues(t, controllerAddr, "kafka-config-roundtrip",
		"cleanup.policy",
		"retention.ms",
		"min.insync.replicas",
		"unclean.leader.election.enable",
	)
	if configs["cleanup.policy"] != "delete" {
		t.Fatalf("cleanup.policy after alter = %q, want %q", configs["cleanup.policy"], "delete")
	}
	if configs["retention.ms"] != "1800000" {
		t.Fatalf("retention.ms after alter = %q, want %q", configs["retention.ms"], "1800000")
	}
	if configs["min.insync.replicas"] != "1" {
		t.Fatalf("min.insync.replicas after alter = %q, want %q", configs["min.insync.replicas"], "1")
	}
	if configs["unclean.leader.election.enable"] != "false" {
		t.Fatalf("unclean.leader.election.enable after alter = %q, want %q", configs["unclean.leader.election.enable"], "false")
	}
}

func TestKafkaAlterConfigsRejectsRetentionBytes(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-retention-bytes-alter-topic")
	defer env.Cleanup()

	retentionBytes := "2048"
	alterReq := kmsg.NewPtrAlterConfigsRequest()
	alterReq.SetVersion(1)
	alterReq.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-retention-bytes-alter-topic",
		Configs: []kmsg.AlterConfigsRequestResourceConfig{{
			Name:  "retention.bytes",
			Value: &retentionBytes,
		}},
	}}
	alterRespAny, err := sendKafkaRequest(addr, alterReq)
	if err != nil {
		t.Fatalf("AlterConfigs Request() error: %v", err)
	}
	alterResp := alterRespAny.(*kmsg.AlterConfigsResponse)
	if len(alterResp.Resources) != 1 {
		t.Fatalf("AlterConfigs resources = %d, want 1", len(alterResp.Resources))
	}
	if alterResp.Resources[0].ErrorCode != kafkaInvalidConfigCode {
		t.Fatalf("AlterConfigs error code = %d, want %d", alterResp.Resources[0].ErrorCode, kafkaInvalidConfigCode)
	}
	if !strings.Contains(stringValue(alterResp.Resources[0].ErrorMessage), "time-based retention only") {
		t.Fatalf("AlterConfigs error message = %q, want time-based retention guidance", stringValue(alterResp.Resources[0].ErrorMessage))
	}
}

func TestKafkaIncrementalAlterConfigsRejectsRetentionBytes(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-retention-bytes-inc-topic")
	defer env.Cleanup()

	retentionBytes := "4096"
	incReq := kmsg.NewPtrIncrementalAlterConfigsRequest()
	incReq.SetVersion(1)
	incReq.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-retention-bytes-inc-topic",
		Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{{
			Name:  "retention.bytes",
			Op:    kmsg.IncrementalAlterConfigOpSet,
			Value: &retentionBytes,
		}},
	}}
	incRespAny, err := sendKafkaRequest(addr, incReq)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs Request() error: %v", err)
	}
	incResp := incRespAny.(*kmsg.IncrementalAlterConfigsResponse)
	if len(incResp.Resources) != 1 {
		t.Fatalf("IncrementalAlterConfigs resources = %d, want 1", len(incResp.Resources))
	}
	if incResp.Resources[0].ErrorCode != kafkaInvalidConfigCode {
		t.Fatalf("IncrementalAlterConfigs error code = %d, want %d", incResp.Resources[0].ErrorCode, kafkaInvalidConfigCode)
	}
	if !strings.Contains(stringValue(incResp.Resources[0].ErrorMessage), "time-based retention only") {
		t.Fatalf("IncrementalAlterConfigs error message = %q, want time-based retention guidance", stringValue(incResp.Resources[0].ErrorMessage))
	}
}

func TestKafkaAlterConfigsRejectsStorageModeMutation(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	disklessMode := "diskless"
	createKafkaFixtureTopicWithConfigs(t, addr, "kafka-storage-mode-alter-topic", 1, []kmsg.CreateTopicsRequestTopicConfig{{
		Name:  "camu.storage.mode",
		Value: &disklessMode,
	}})

	classicMode := "classic"
	alterReq := kmsg.NewPtrAlterConfigsRequest()
	alterReq.SetVersion(1)
	alterReq.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-storage-mode-alter-topic",
		Configs: []kmsg.AlterConfigsRequestResourceConfig{{
			Name:  "camu.storage.mode",
			Value: &classicMode,
		}},
	}}
	alterRespAny, err := sendKafkaRequest(addr, alterReq)
	if err != nil {
		t.Fatalf("AlterConfigs Request() error: %v", err)
	}
	alterResp := alterRespAny.(*kmsg.AlterConfigsResponse)
	if len(alterResp.Resources) != 1 {
		t.Fatalf("AlterConfigs resources = %d, want 1", len(alterResp.Resources))
	}
	if alterResp.Resources[0].ErrorCode != kafkaInvalidConfigCode {
		t.Fatalf("AlterConfigs error code = %d, want %d", alterResp.Resources[0].ErrorCode, kafkaInvalidConfigCode)
	}
	if !strings.Contains(stringValue(alterResp.Resources[0].ErrorMessage), "immutable") {
		t.Fatalf("AlterConfigs error message = %q, want immutable guidance", stringValue(alterResp.Resources[0].ErrorMessage))
	}
}

func TestKafkaConfigAPIsRejectUnsupportedResourceTypes(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

	describeReq := kmsg.NewPtrDescribeConfigsRequest()
	describeReq.SetVersion(1)
	describeReq.Resources = []kmsg.DescribeConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeBroker,
		ResourceName: "0",
	}}
	describeRespAny, err := sendKafkaRequest(addr, describeReq)
	if err != nil {
		t.Fatalf("DescribeConfigs Request() error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeConfigsResponse)
	if len(describeResp.Resources) != 1 {
		t.Fatalf("DescribeConfigs resources = %d, want 1", len(describeResp.Resources))
	}
	if describeResp.Resources[0].ErrorCode != 42 {
		t.Fatalf("DescribeConfigs error code = %d, want 42", describeResp.Resources[0].ErrorCode)
	}
	if !strings.Contains(stringValue(describeResp.Resources[0].ErrorMessage), "only topic configs are supported") {
		t.Fatalf("DescribeConfigs error message = %q, want topic-only guidance", stringValue(describeResp.Resources[0].ErrorMessage))
	}

	alterReq := kmsg.NewPtrAlterConfigsRequest()
	alterReq.SetVersion(1)
	alterReq.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeBroker,
		ResourceName: "0",
	}}
	alterRespAny, err := sendKafkaRequest(addr, alterReq)
	if err != nil {
		t.Fatalf("AlterConfigs Request() error: %v", err)
	}
	alterResp := alterRespAny.(*kmsg.AlterConfigsResponse)
	if len(alterResp.Resources) != 1 {
		t.Fatalf("AlterConfigs resources = %d, want 1", len(alterResp.Resources))
	}
	if alterResp.Resources[0].ErrorCode != 42 {
		t.Fatalf("AlterConfigs error code = %d, want 42", alterResp.Resources[0].ErrorCode)
	}
	if !strings.Contains(stringValue(alterResp.Resources[0].ErrorMessage), "only topic config mutation is supported") {
		t.Fatalf("AlterConfigs error message = %q, want topic-only mutation guidance", stringValue(alterResp.Resources[0].ErrorMessage))
	}

	incReq := kmsg.NewPtrIncrementalAlterConfigsRequest()
	incReq.SetVersion(1)
	incReq.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeBroker,
		ResourceName: "0",
	}}
	incRespAny, err := sendKafkaRequest(addr, incReq)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs Request() error: %v", err)
	}
	incResp := incRespAny.(*kmsg.IncrementalAlterConfigsResponse)
	if len(incResp.Resources) != 1 {
		t.Fatalf("IncrementalAlterConfigs resources = %d, want 1", len(incResp.Resources))
	}
	if incResp.Resources[0].ErrorCode != 42 {
		t.Fatalf("IncrementalAlterConfigs error code = %d, want 42", incResp.Resources[0].ErrorCode)
	}
	if !strings.Contains(stringValue(incResp.Resources[0].ErrorMessage), "only topic config mutation is supported") {
		t.Fatalf("IncrementalAlterConfigs error message = %q, want topic-only mutation guidance", stringValue(incResp.Resources[0].ErrorMessage))
	}
}

func TestKafkaIncrementalAlterConfigsRejectsStorageModeDelete(t *testing.T) {
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)
	defer env.Cleanup()

	addr := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	disklessMode := "diskless"
	createKafkaFixtureTopicWithConfigs(t, addr, "kafka-storage-mode-delete-topic", 1, []kmsg.CreateTopicsRequestTopicConfig{{
		Name:  "camu.storage.mode",
		Value: &disklessMode,
	}})

	incReq := kmsg.NewPtrIncrementalAlterConfigsRequest()
	incReq.SetVersion(1)
	incReq.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-storage-mode-delete-topic",
		Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{{
			Name: "camu.storage.mode",
			Op:   kmsg.IncrementalAlterConfigOpDelete,
		}},
	}}
	incRespAny, err := sendKafkaRequest(addr, incReq)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs Request() error: %v", err)
	}
	incResp := incRespAny.(*kmsg.IncrementalAlterConfigsResponse)
	if len(incResp.Resources) != 1 {
		t.Fatalf("IncrementalAlterConfigs resources = %d, want 1", len(incResp.Resources))
	}
	if incResp.Resources[0].ErrorCode != kafkaInvalidConfigCode {
		t.Fatalf("IncrementalAlterConfigs error code = %d, want %d", incResp.Resources[0].ErrorCode, kafkaInvalidConfigCode)
	}
	if !strings.Contains(stringValue(incResp.Resources[0].ErrorMessage), "immutable") {
		t.Fatalf("IncrementalAlterConfigs error message = %q, want immutable guidance", stringValue(incResp.Resources[0].ErrorMessage))
	}
}

func TestKafkaAlterConfigsRejectsUnsupportedConfigNameWithoutMutation(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-unsupported-alter-config")
	defer env.Cleanup()

	retentionMs := "1800000"
	unsupported := "1"
	req := kmsg.NewPtrAlterConfigsRequest()
	req.SetVersion(1)
	req.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-unsupported-alter-config",
		Configs: []kmsg.AlterConfigsRequestResourceConfig{
			{Name: "retention.ms", Value: &retentionMs},
			{Name: "unsupported.config.name", Value: &unsupported},
		},
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("AlterConfigs Request() error: %v", err)
	}
	resp := respAny.(*kmsg.AlterConfigsResponse)
	if len(resp.Resources) != 1 {
		t.Fatalf("AlterConfigs resources = %d, want 1", len(resp.Resources))
	}
	if resp.Resources[0].ErrorCode != kafkaInvalidConfigCode {
		t.Fatalf("AlterConfigs error code = %d, want %d", resp.Resources[0].ErrorCode, kafkaInvalidConfigCode)
	}
	if !strings.Contains(stringValue(resp.Resources[0].ErrorMessage), "unsupported topic config") {
		t.Fatalf("AlterConfigs error message = %q, want unsupported-config guidance", stringValue(resp.Resources[0].ErrorMessage))
	}

	got := fetchKafkaTopicConfigValues(t, addr, "kafka-unsupported-alter-config", "retention.ms")
	if got["retention.ms"] != "604800000" {
		t.Fatalf("retention.ms after rejected AlterConfigs = %q, want default %q", got["retention.ms"], "604800000")
	}
}

func TestKafkaIncrementalAlterConfigsRejectsUnsupportedConfigNameWithoutMutation(t *testing.T) {
	env, _, addr := newKafkaFixtureEnv(t, "kafka-unsupported-inc-config")
	defer env.Cleanup()

	retentionMs := "1800000"
	unsupported := "1"
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.SetVersion(1)
	req.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "kafka-unsupported-inc-config",
		Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{
			{Name: "retention.ms", Op: kmsg.IncrementalAlterConfigOpSet, Value: &retentionMs},
			{Name: "unsupported.config.name", Op: kmsg.IncrementalAlterConfigOpSet, Value: &unsupported},
		},
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs Request() error: %v", err)
	}
	resp := respAny.(*kmsg.IncrementalAlterConfigsResponse)
	if len(resp.Resources) != 1 {
		t.Fatalf("IncrementalAlterConfigs resources = %d, want 1", len(resp.Resources))
	}
	if resp.Resources[0].ErrorCode != kafkaInvalidConfigCode {
		t.Fatalf("IncrementalAlterConfigs error code = %d, want %d", resp.Resources[0].ErrorCode, kafkaInvalidConfigCode)
	}
	if !strings.Contains(stringValue(resp.Resources[0].ErrorMessage), "unsupported topic config") {
		t.Fatalf("IncrementalAlterConfigs error message = %q, want unsupported-config guidance", stringValue(resp.Resources[0].ErrorMessage))
	}

	got := fetchKafkaTopicConfigValues(t, addr, "kafka-unsupported-inc-config", "retention.ms")
	if got["retention.ms"] != "604800000" {
		t.Fatalf("retention.ms after rejected IncrementalAlterConfigs = %q, want default %q", got["retention.ms"], "604800000")
	}
}

func TestKafkaDescribeCluster(t *testing.T) {
	env, _, addr := newKafkaReadyEnv(t)
	defer env.Cleanup()

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

	const topic = "kafka-group-consume"
	seedBroker := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	createKafkaFixtureTopic(t, seedBroker, topic, 1)
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
	env, httpClient, client, _ := newKafkaTopicBootstrappedEnv(t, "kafka-http-bridge")
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
	env, httpClient, client, _ := newKafkaTopicBootstrappedEnv(t, "http-kafka-bridge")
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
