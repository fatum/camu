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

func TestKafkaMetadataAdvertisesLeaderAndBroker(t *testing.T) {
	// Topic provisioning happens over Kafka admin; the assertions here are about
	// Kafka metadata once the topic exists.
	env, _, client, _ := newKafkaTopicBootstrappedEnv(t, "kafka-metadata")
	defer env.Cleanup()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: strPtr("kafka-metadata")}}

	respAny, err := client.Request(ctx, req)
	if err != nil {
		t.Fatalf("Metadata Request() error: %v", err)
	}

	resp := respAny.(*kmsg.MetadataResponse)
	if len(resp.Brokers) != 1 {
		t.Fatalf("metadata brokers = %d, want 1", len(resp.Brokers))
	}

	broker := resp.Brokers[0]
	wantBrokerID := kafkaBrokerIDForTest("127.0.0.1")
	if broker.NodeID != wantBrokerID {
		t.Fatalf("broker node id = %d, want %d", broker.NodeID, wantBrokerID)
	}
	if broker.Host != "127.0.0.1" {
		t.Fatalf("broker host = %q, want %q", broker.Host, "127.0.0.1")
	}
	if broker.Port <= 0 {
		t.Fatalf("broker port = %d, want > 0", broker.Port)
	}

	if len(resp.Topics) != 1 {
		t.Fatalf("metadata topics = %d, want 1", len(resp.Topics))
	}
	topic := resp.Topics[0]
	if topic.Topic == nil || *topic.Topic != "kafka-metadata" {
		t.Fatalf("metadata topic = %v, want kafka-metadata", topic.Topic)
	}
	if len(topic.Partitions) != 1 {
		t.Fatalf("metadata partitions = %d, want 1", len(topic.Partitions))
	}
	partition := topic.Partitions[0]
	if partition.Partition != 0 {
		t.Fatalf("metadata partition id = %d, want 0", partition.Partition)
	}
	if partition.Leader != wantBrokerID {
		t.Fatalf("metadata partition leader = %d, want %d", partition.Leader, wantBrokerID)
	}
	if len(partition.Replicas) != 1 || partition.Replicas[0] != wantBrokerID {
		t.Fatalf("metadata replicas = %v, want [%d]", partition.Replicas, wantBrokerID)
	}
	if len(partition.ISR) != 1 || partition.ISR[0] != wantBrokerID {
		t.Fatalf("metadata isr = %v, want [%d]", partition.ISR, wantBrokerID)
	}
}

func TestKafkaMetadataTranslatesReplicatedCamuState(t *testing.T) {
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
	createKafkaReplicatedFixtureTopic(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID, "kafka-metadata-rf2", 1, 2, []kmsg.CreateTopicsRequestTopicConfig{{
		Name:  "min.insync.replicas",
		Value: strPtr("1"),
	}})

	client, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", port1)),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient() error: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp *kmsg.MetadataResponse
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		req := kmsg.NewPtrMetadataRequest()
		req.Topics = []kmsg.MetadataRequestTopic{{Topic: strPtr("kafka-metadata-rf2")}}
		respAny, err := client.Request(ctx, req)
		if err != nil {
			t.Fatalf("Metadata Request() error: %v", err)
		}
		resp = respAny.(*kmsg.MetadataResponse)
		if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 && len(resp.Topics[0].Partitions[0].Replicas) == 2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if resp == nil {
		t.Fatal("metadata response was nil")
	}
	if len(resp.Brokers) != 2 {
		t.Fatalf("metadata brokers = %d, want 2", len(resp.Brokers))
	}
	wantIDs := map[int32]bool{
		kafkaBrokerIDForTest("127.0.0.1"): true,
		kafkaBrokerIDForTest("127.0.0.2"): true,
	}
	for _, broker := range resp.Brokers {
		if !wantIDs[broker.NodeID] {
			t.Fatalf("unexpected broker id %d in metadata", broker.NodeID)
		}
	}
	topic := resp.Topics[0]
	if len(topic.Partitions) != 1 {
		t.Fatalf("metadata partitions = %d, want 1", len(topic.Partitions))
	}
	partition := topic.Partitions[0]
	if len(partition.Replicas) != 2 {
		t.Fatalf("metadata replicas = %v, want 2 replicas", partition.Replicas)
	}
	if !wantIDs[partition.Leader] {
		t.Fatalf("metadata leader = %d, want one of the translated broker ids", partition.Leader)
	}
	if len(partition.ISR) < 1 || len(partition.ISR) > 2 {
		t.Fatalf("metadata isr = %v, want 1 or 2 members", partition.ISR)
	}
	for _, replicaID := range partition.Replicas {
		if !wantIDs[replicaID] {
			t.Fatalf("metadata replica id = %d, not in translated broker ids", replicaID)
		}
	}
}

func TestKafkaProduceToFollowerReturnsNotLeader(t *testing.T) {
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

	const (
		topic   = "kafka-not-leader"
		partID  = 0
		partKey = "0"
	)
	addrByBrokerID := map[int32]string{
		kafkaBrokerIDForTest("127.0.0.1"): fmt.Sprintf("127.0.0.1:%d", port1),
		kafkaBrokerIDForTest("127.0.0.2"): fmt.Sprintf("127.0.0.1:%d", port2),
	}
	createKafkaReplicatedFixtureTopic(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID, topic, 1, 2, []kmsg.CreateTopicsRequestTopicConfig{{
		Name:  "min.insync.replicas",
		Value: strPtr("1"),
	}})

	leaderIdx, _ := waitForLeader(t, env, topic, partKey, map[int]bool{})
	followerIdx := 1 - leaderIdx

	addrByIndex := map[int]string{
		0: fmt.Sprintf("127.0.0.1:%d", port1),
		1: fmt.Sprintf("127.0.0.1:%d", port2),
	}
	followerAddr := addrByIndex[followerIdx]

	metadataReq := kmsg.NewPtrMetadataRequest()
	metadataReq.SetVersion(1)
	metadataReq.Topics = []kmsg.MetadataRequestTopic{{Topic: strPtr(topic)}}
	respAny, err := sendKafkaRequest(followerAddr, metadataReq)
	if err != nil {
		t.Fatalf("Metadata via follower error: %v", err)
	}
	metadataResp := respAny.(*kmsg.MetadataResponse)
	if len(metadataResp.Topics) != 1 || len(metadataResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected metadata response: %+v", metadataResp.Topics)
	}
	wantLeaderID := kafkaBrokerIDForTest(fmt.Sprintf("127.0.0.%d", leaderIdx+1))
	if metadataResp.Topics[0].Partitions[0].Leader != wantLeaderID {
		t.Fatalf("metadata leader = %d, want %d", metadataResp.Topics[0].Partitions[0].Leader, wantLeaderID)
	}

	produceReq := kmsg.NewPtrProduceRequest()
	produceReq.SetVersion(3)
	produceReq.Acks = 1
	produceReq.Topics = []kmsg.ProduceRequestTopic{{
		Topic: topic,
		Partitions: []kmsg.ProduceRequestTopicPartition{{
			Partition: partID,
			Records:   kafkaRecordBatchForTest([]testKafkaRecord{{Value: []byte("x")}}),
		}},
	}}
	produceRespAny, err := sendKafkaRequest(followerAddr, produceReq)
	if err != nil {
		t.Fatalf("Produce via follower error: %v", err)
	}
	produceResp := produceRespAny.(*kmsg.ProduceResponse)
	if len(produceResp.Topics) != 1 || len(produceResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected produce response: %+v", produceResp.Topics)
	}
	if got := produceResp.Topics[0].Partitions[0].ErrorCode; got != 6 {
		t.Fatalf("produce error code = %d, want 6 (NOT_LEADER)", got)
	}
}

func TestKafkaFetchFromFollowerReturnsNotLeader(t *testing.T) {
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

	const (
		topic   = "kafka-fetch-not-leader"
		partID  = 0
		partKey = "0"
	)
	addrByBrokerID := map[int32]string{
		kafkaBrokerIDForTest("127.0.0.1"): fmt.Sprintf("127.0.0.1:%d", port1),
		kafkaBrokerIDForTest("127.0.0.2"): fmt.Sprintf("127.0.0.1:%d", port2),
	}
	createKafkaReplicatedFixtureTopic(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID, topic, 1, 2, []kmsg.CreateTopicsRequestTopicConfig{{
		Name:  "min.insync.replicas",
		Value: strPtr("1"),
	}})

	leaderIdx, leaderClient := waitForLeader(t, env, topic, partKey, map[int]bool{})
	waitForPartitionProduceReady(t, leaderClient, topic, partID)
	if _, err := leaderClient.ProduceToPartition(topic, partID, []camutest.ProduceMessage{{Value: "seed"}}); err != nil {
		t.Fatalf("seed produce error: %v", err)
	}
	followerIdx := 1 - leaderIdx
	addrByIndex := map[int]string{
		0: fmt.Sprintf("127.0.0.1:%d", port1),
		1: fmt.Sprintf("127.0.0.1:%d", port2),
	}
	followerAddr := addrByIndex[followerIdx]

	fetchReq := kmsg.NewPtrFetchRequest()
	fetchReq.SetVersion(4)
	fetchReq.MinBytes = 1
	fetchReq.MaxWaitMillis = 100
	fetchReq.Topics = []kmsg.FetchRequestTopic{{
		Topic: topic,
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition:         partID,
			FetchOffset:       0,
			PartitionMaxBytes: 1024,
		}},
	}}
	fetchRespAny, err := sendKafkaRequest(followerAddr, fetchReq)
	if err != nil {
		t.Fatalf("Fetch via follower error: %v", err)
	}
	fetchResp := fetchRespAny.(*kmsg.FetchResponse)
	if len(fetchResp.Topics) != 1 || len(fetchResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected fetch response: %+v", fetchResp.Topics)
	}
	if got := fetchResp.Topics[0].Partitions[0].ErrorCode; got != 6 {
		t.Fatalf("fetch error code = %d, want 6 (NOT_LEADER)", got)
	}
}

func TestKafkaAdminMutationsOnFollowerReturnNotController(t *testing.T) {
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
	waitForKafkaAddr(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")])
	waitForKafkaAddr(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.2")])
	controllerAddr, followerAddr := waitForKafkaControllerAndFollowerAddrs(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID)

	createReq := kmsg.NewPtrCreateTopicsRequest()
	createReq.SetVersion(5)
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             "kafka-admin-follower-create",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}}
	createRespAny, err := sendKafkaRequest(followerAddr, createReq)
	if err != nil {
		t.Fatalf("CreateTopics via follower error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateTopicsResponse)
	if len(createResp.Topics) != 1 {
		t.Fatalf("CreateTopics topics = %d, want 1", len(createResp.Topics))
	}
	if createResp.Topics[0].ErrorCode != kafkaErrorNotControllerForTest {
		t.Fatalf("CreateTopics error code = %d, want %d", createResp.Topics[0].ErrorCode, kafkaErrorNotControllerForTest)
	}
	meta := fetchKafkaTopicMetadata(t, controllerAddr, "kafka-admin-follower-create")
	if meta.ErrorCode != 3 {
		t.Fatalf("metadata error after follower create = %d, want 3", meta.ErrorCode)
	}

	createKafkaFixtureTopic(t, controllerAddr, "kafka-admin-follower-delete", 1)

	deleteReq := kmsg.NewPtrDeleteTopicsRequest()
	deleteReq.SetVersion(5)
	deleteReq.TopicNames = []string{"kafka-admin-follower-delete"}
	deleteRespAny, err := sendKafkaRequest(followerAddr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteTopics via follower error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteTopicsResponse)
	if len(deleteResp.Topics) != 1 {
		t.Fatalf("DeleteTopics topics = %d, want 1", len(deleteResp.Topics))
	}
	if deleteResp.Topics[0].ErrorCode != kafkaErrorNotControllerForTest {
		t.Fatalf("DeleteTopics error code = %d, want %d", deleteResp.Topics[0].ErrorCode, kafkaErrorNotControllerForTest)
	}
	meta = fetchKafkaTopicMetadata(t, controllerAddr, "kafka-admin-follower-delete")
	if meta.ErrorCode != 0 {
		t.Fatalf("metadata error after follower delete = %d, want 0", meta.ErrorCode)
	}
	if len(meta.Partitions) != 1 {
		t.Fatalf("metadata partitions after follower delete = %d, want 1", len(meta.Partitions))
	}
}

func TestKafkaACLMutationsOnFollowerReturnNotController(t *testing.T) {
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
	waitForKafkaAddr(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")])
	waitForKafkaAddr(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.2")])
	controllerAddr, followerAddr := waitForKafkaControllerAndFollowerAddrs(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID)

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
	createRespAny, err := sendKafkaRequest(followerAddr, createReq)
	if err != nil {
		t.Fatalf("CreateACLs via follower error: %v", err)
	}
	createResp := createRespAny.(*kmsg.CreateACLsResponse)
	if len(createResp.Results) != 1 {
		t.Fatalf("CreateACLs results = %d, want 1", len(createResp.Results))
	}
	if createResp.Results[0].ErrorCode != kafkaErrorNotControllerForTest {
		t.Fatalf("CreateACLs error code = %d, want %d", createResp.Results[0].ErrorCode, kafkaErrorNotControllerForTest)
	}

	describeReq := kmsg.NewPtrDescribeACLsRequest()
	describeReq.SetVersion(1)
	describeReq.ResourceType = kmsg.ACLResourceTypeTopic
	describeReq.ResourceName = kmsg.StringPtr("orders")
	describeReq.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
	describeReq.Operation = kmsg.ACLOperationAny
	describeReq.PermissionType = kmsg.ACLPermissionTypeAny
	describeRespAny, err := sendKafkaRequest(controllerAddr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs after follower create error: %v", err)
	}
	describeResp := describeRespAny.(*kmsg.DescribeACLsResponse)
	if len(describeResp.Resources) != 0 {
		t.Fatalf("DescribeACLs resources after follower create = %+v, want none", describeResp.Resources)
	}

	createRespAny, err = sendKafkaRequest(controllerAddr, createReq)
	if err != nil {
		t.Fatalf("CreateACLs via controller error: %v", err)
	}
	createResp = createRespAny.(*kmsg.CreateACLsResponse)
	if len(createResp.Results) != 1 || createResp.Results[0].ErrorCode != 0 {
		t.Fatalf("CreateACLs via controller response = %+v, want success", createResp.Results)
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
	deleteRespAny, err := sendKafkaRequest(followerAddr, deleteReq)
	if err != nil {
		t.Fatalf("DeleteACLs via follower error: %v", err)
	}
	deleteResp := deleteRespAny.(*kmsg.DeleteACLsResponse)
	if len(deleteResp.Results) != 1 {
		t.Fatalf("DeleteACLs results = %d, want 1", len(deleteResp.Results))
	}
	if deleteResp.Results[0].ErrorCode != kafkaErrorNotControllerForTest {
		t.Fatalf("DeleteACLs error code = %d, want %d", deleteResp.Results[0].ErrorCode, kafkaErrorNotControllerForTest)
	}

	describeRespAny, err = sendKafkaRequest(controllerAddr, describeReq)
	if err != nil {
		t.Fatalf("DescribeACLs after follower delete error: %v", err)
	}
	describeResp = describeRespAny.(*kmsg.DescribeACLsResponse)
	if len(describeResp.Resources) != 1 || len(describeResp.Resources[0].ACLs) != 1 {
		t.Fatalf("DescribeACLs resources after follower delete = %+v, want one ACL to remain", describeResp.Resources)
	}
}

func TestKafkaLeaderFailoverRefreshesMetadataAndRecoversIO(t *testing.T) {
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

	const (
		topic   = "kafka-failover"
		partID  = 0
		partKey = "0"
	)
	addrByBrokerID := map[int32]string{
		kafkaBrokerIDForTest("127.0.0.1"): fmt.Sprintf("127.0.0.1:%d", port1),
		kafkaBrokerIDForTest("127.0.0.2"): fmt.Sprintf("127.0.0.1:%d", port2),
	}
	createKafkaReplicatedFixtureTopic(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID, topic, 1, 2, []kmsg.CreateTopicsRequestTopicConfig{{
		Name:  "min.insync.replicas",
		Value: strPtr("1"),
	}})

	leaderIdx, leaderClient := waitForLeaderAmong(t, env, topic, partKey, map[int]bool{}, []int{0, 1})
	waitForPartitionProduceReady(t, leaderClient, topic, partID)
	if _, err := leaderClient.ProduceToPartition(topic, partID, []camutest.ProduceMessage{{Value: "before-failover"}}); err != nil {
		t.Fatalf("seed produce error: %v", err)
	}

	env.StopInstance(leaderIdx)
	survivorIdx := 1 - leaderIdx
	survivorAddr := map[int]string{
		0: fmt.Sprintf("127.0.0.1:%d", port1),
		1: fmt.Sprintf("127.0.0.1:%d", port2),
	}[survivorIdx]
	wantLeaderID := kafkaBrokerIDForTest(fmt.Sprintf("127.0.0.%d", survivorIdx+1))

	metadataResp := waitForKafkaLeaderID(t, survivorAddr, topic, wantLeaderID)
	if len(metadataResp.Topics) != 1 || len(metadataResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected metadata response after failover: %+v", metadataResp.Topics)
	}

	waitForKafkaProduceSuccess(t, survivorAddr, topic, partID, "after-failover")
	fetchResp := waitForKafkaFetchSuccess(t, survivorAddr, topic, partID)
	if len(fetchResp.Topics) != 1 || len(fetchResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected fetch response after failover: %+v", fetchResp.Topics)
	}
	if len(fetchResp.Topics[0].Partitions[0].RecordBatches) == 0 {
		t.Fatal("fetch after failover returned no record batches")
	}
}

func TestKafkaClientRecoversAcrossLeaderFailover(t *testing.T) {
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

	const (
		topic   = "kafka-client-failover"
		partID  = 0
		partKey = "0"
	)
	addrByBrokerID := map[int32]string{
		kafkaBrokerIDForTest("127.0.0.1"): fmt.Sprintf("127.0.0.1:%d", port1),
		kafkaBrokerIDForTest("127.0.0.2"): fmt.Sprintf("127.0.0.1:%d", port2),
	}
	createKafkaReplicatedFixtureTopic(t, addrByBrokerID[kafkaBrokerIDForTest("127.0.0.1")], addrByBrokerID, topic, 1, 2, []kmsg.CreateTopicsRequestTopicConfig{{
		Name:  "min.insync.replicas",
		Value: strPtr("1"),
	}})

	client, err := kgo.NewClient(
		kgo.SeedBrokers(
			fmt.Sprintf("127.0.0.1:%d", port1),
			fmt.Sprintf("127.0.0.1:%d", port2),
		),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableIdempotentWrite(),
		kgo.DisableFetchSessions(),
		kgo.MetadataMinAge(100*time.Millisecond),
		kgo.DialTimeout(2*time.Second),
		kgo.RequestTimeoutOverhead(3*time.Second),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partID: kgo.NewOffset().At(0)},
		}),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient() error: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := produceWithKgoUntilSuccess(ctx, client, topic, "before-failover"); err != nil {
		t.Fatalf("initial ProduceSync() error: %v", err)
	}
	got := collectKafkaValues(t, ctx, client, 1)
	if len(got) != 1 || string(got[0]) != "before-failover" {
		t.Fatalf("initial fetch values = %q, want [before-failover]", valuesToStrings(got))
	}

	leaderIdx, leaderClient := waitForLeaderAmong(t, env, topic, partKey, map[int]bool{}, []int{0, 1})
	waitForPartitionProduceReady(t, leaderClient, topic, partID)
	env.StopInstance(leaderIdx)

	if err := env.WaitForInstance(1-leaderIdx, 5*time.Second); err != nil {
		t.Fatalf("survivor instance not ready: %v", err)
	}

	client.ForceMetadataRefresh()
	if err := produceWithKgoUntilSuccess(ctx, client, topic, "after-failover"); err != nil {
		t.Fatalf("post-failover ProduceSync() error: %v", err)
	}

	got = collectKafkaValues(t, ctx, client, 2)
	if len(got) != 2 {
		t.Fatalf("post-failover fetched %d records, want 2", len(got))
	}
	if string(got[1]) != "after-failover" {
		t.Fatalf("post-failover last value = %q, want %q", string(got[1]), "after-failover")
	}
}
