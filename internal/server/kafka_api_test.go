package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/log"
)

func TestKafkaApiVersions(t *testing.T) {
	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{},
		TopicLister:     &mockTopicLister{},
	})

	req := &kmsg.ApiVersionsRequest{}
	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.ApiVersionsResponse)
	require.NotNil(t, apiResp)
	assert.True(t, len(apiResp.ApiKeys) > 0, "should advertise API keys")

	var hasProduce, hasFetch, hasListOffsets, hasMetadata, hasOffsetCommit, hasOffsetFetch, hasFindCoordinator, hasJoinGroup, hasHeartbeat, hasLeaveGroup, hasSyncGroup, hasDescribeGroups, hasListGroups, hasCreateTopics, hasDeleteTopics, hasDescribeConfigs, hasAlterConfigs, hasCreatePartitions, hasIncrementalAlterConfigs, hasDeleteGroups, hasOffsetDelete, hasDescribeCluster, hasCreateACLs, hasDescribeACLs, hasDeleteACLs, hasApiVersions, hasInitProducerID bool
	for _, key := range apiResp.ApiKeys {
		switch key.ApiKey {
		case 0:
			hasProduce = true
		case 1:
			hasFetch = true
		case 2:
			hasListOffsets = true
		case 3:
			hasMetadata = true
		case 8:
			hasOffsetCommit = true
		case 9:
			hasOffsetFetch = true
		case 10:
			hasFindCoordinator = true
		case 11:
			hasJoinGroup = true
		case 12:
			hasHeartbeat = true
		case 13:
			hasLeaveGroup = true
		case 14:
			hasSyncGroup = true
		case 15:
			hasDescribeGroups = true
		case 16:
			hasListGroups = true
		case 42:
			hasDeleteGroups = true
		case 47:
			hasOffsetDelete = true
		case 18:
			hasApiVersions = true
		case 19:
			hasCreateTopics = true
		case 20:
			hasDeleteTopics = true
		case 32:
			hasDescribeConfigs = true
		case 33:
			hasAlterConfigs = true
		case 37:
			hasCreatePartitions = true
		case 44:
			hasIncrementalAlterConfigs = true
		case 60:
			hasDescribeCluster = true
		case 22:
			hasInitProducerID = true
		case 29:
			hasDescribeACLs = true
		case 30:
			hasCreateACLs = true
		case 31:
			hasDeleteACLs = true
		}
	}
	assert.True(t, hasProduce, "should support Produce")
	assert.True(t, hasFetch, "should support Fetch")
	assert.True(t, hasListOffsets, "should support ListOffsets")
	assert.True(t, hasMetadata, "should support Metadata")
	assert.True(t, hasOffsetCommit, "should support OffsetCommit")
	assert.True(t, hasOffsetFetch, "should support OffsetFetch")
	assert.True(t, hasFindCoordinator, "should support FindCoordinator")
	assert.True(t, hasJoinGroup, "should support JoinGroup")
	assert.True(t, hasHeartbeat, "should support Heartbeat")
	assert.True(t, hasLeaveGroup, "should support LeaveGroup")
	assert.True(t, hasSyncGroup, "should support SyncGroup")
	assert.True(t, hasDescribeGroups, "should support DescribeGroups")
	assert.True(t, hasListGroups, "should support ListGroups")
	assert.True(t, hasCreateTopics, "should support CreateTopics")
	assert.True(t, hasDeleteTopics, "should support DeleteTopics")
	assert.True(t, hasDescribeConfigs, "should support DescribeConfigs")
	assert.True(t, hasAlterConfigs, "should support AlterConfigs")
	assert.True(t, hasCreatePartitions, "should support CreatePartitions")
	assert.True(t, hasIncrementalAlterConfigs, "should support IncrementalAlterConfigs")
	assert.True(t, hasDeleteGroups, "should support DeleteGroups")
	assert.True(t, hasOffsetDelete, "should support OffsetDelete")
	assert.True(t, hasDescribeCluster, "should support DescribeCluster")
	assert.True(t, hasCreateACLs, "should support CreateACLs")
	assert.True(t, hasDescribeACLs, "should support DescribeACLs")
	assert.True(t, hasDeleteACLs, "should support DeleteACLs")
	assert.True(t, hasApiVersions, "should support ApiVersions")
	assert.True(t, hasInitProducerID, "should support InitProducerID")
}

func TestKafkaApiVersionsAdvertisesExpectedVersionRanges(t *testing.T) {
	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{},
		TopicLister:     &mockTopicLister{},
	})

	respAny, err := ks.HandleRequest(context.Background(), &kmsg.ApiVersionsRequest{})
	require.NoError(t, err)

	resp := respAny.(*kmsg.ApiVersionsResponse)
	got := make(map[int16]kmsg.ApiVersionsResponseApiKey, len(resp.ApiKeys))
	for _, key := range resp.ApiKeys {
		got[key.ApiKey] = key
	}

	want := map[int16]struct {
		min int16
		max int16
	}{
		0:  {min: 0, max: 9},
		1:  {min: 0, max: 12},
		2:  {min: 0, max: 10},
		3:  {min: 0, max: 12},
		8:  {min: 0, max: 8},
		9:  {min: 0, max: 7},
		10: {min: 0, max: 4},
		11: {min: 0, max: 9},
		12: {min: 0, max: 4},
		13: {min: 0, max: 4},
		14: {min: 0, max: 5},
		15: {min: 0, max: 5},
		16: {min: 0, max: 5},
		18: {min: 0, max: 3},
		19: {min: 0, max: 7},
		20: {min: 0, max: 6},
		22: {min: 0, max: 5},
		29: {min: 0, max: 3},
		30: {min: 0, max: 3},
		31: {min: 0, max: 3},
		32: {min: 0, max: 4},
		33: {min: 0, max: 2},
		37: {min: 0, max: 3},
		42: {min: 0, max: 2},
		44: {min: 0, max: 1},
		47: {min: 0, max: 0},
		60: {min: 0, max: 2},
	}

	require.Len(t, got, len(want))
	for apiKey, expect := range want {
		key, ok := got[apiKey]
		require.True(t, ok, "missing api key %d", apiKey)
		assert.Equal(t, expect.min, key.MinVersion, "api key %d min version", apiKey)
		assert.Equal(t, expect.max, key.MaxVersion, "api key %d max version", apiKey)
	}
}

func TestKafkaACLs(t *testing.T) {
	var createReq *kmsg.CreateACLsRequest
	var describeReq *kmsg.DescribeACLsRequest
	var deleteReq *kmsg.DeleteACLsRequest

	ks := NewKafkaServer(&KafkaServerCfg{
		CreateACLsFunc: func(ctx context.Context, req *kmsg.CreateACLsRequest) (*kmsg.CreateACLsResponse, error) {
			createReq = req
			resp := kmsg.NewPtrCreateACLsResponse()
			resp.Results = []kmsg.CreateACLsResponseResult{{}}
			return resp, nil
		},
		DescribeACLsFunc: func(ctx context.Context, req *kmsg.DescribeACLsRequest) (*kmsg.DescribeACLsResponse, error) {
			describeReq = req
			resp := kmsg.NewPtrDescribeACLsResponse()
			resp.Resources = []kmsg.DescribeACLsResponseResource{{
				ResourceType:        kmsg.ACLResourceTypeTopic,
				ResourceName:        "orders",
				ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
			}}
			return resp, nil
		},
		DeleteACLsFunc: func(ctx context.Context, req *kmsg.DeleteACLsRequest) (*kmsg.DeleteACLsResponse, error) {
			deleteReq = req
			resp := kmsg.NewPtrDeleteACLsResponse()
			resp.Results = []kmsg.DeleteACLsResponseResult{{}}
			return resp, nil
		},
	})

	create := kmsg.NewPtrCreateACLsRequest()
	create.Creations = []kmsg.CreateACLsRequestCreation{{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        "orders",
		ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
		Principal:           "User:alice",
		Host:                "*",
		Operation:           kmsg.ACLOperationRead,
		PermissionType:      kmsg.ACLPermissionTypeAllow,
	}}
	createRespAny, err := ks.HandleRequest(context.Background(), create)
	require.NoError(t, err)
	createResp := createRespAny.(*kmsg.CreateACLsResponse)
	require.Len(t, createResp.Results, 1)
	assert.Equal(t, "orders", createReq.Creations[0].ResourceName)

	describe := kmsg.NewPtrDescribeACLsRequest()
	describe.ResourceType = kmsg.ACLResourceTypeTopic
	describe.ResourceName = kmsg.StringPtr("orders")
	describe.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
	describe.Operation = kmsg.ACLOperationAny
	describe.PermissionType = kmsg.ACLPermissionTypeAny
	describeRespAny, err := ks.HandleRequest(context.Background(), describe)
	require.NoError(t, err)
	describeResp := describeRespAny.(*kmsg.DescribeACLsResponse)
	require.Len(t, describeResp.Resources, 1)
	assert.Equal(t, "orders", *describeReq.ResourceName)

	del := kmsg.NewPtrDeleteACLsRequest()
	del.Filters = []kmsg.DeleteACLsRequestFilter{{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        kmsg.StringPtr("orders"),
		ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
		Operation:           kmsg.ACLOperationAny,
		PermissionType:      kmsg.ACLPermissionTypeAny,
	}}
	deleteRespAny, err := ks.HandleRequest(context.Background(), del)
	require.NoError(t, err)
	deleteResp := deleteRespAny.(*kmsg.DeleteACLsResponse)
	require.Len(t, deleteResp.Results, 1)
	assert.Equal(t, "orders", *deleteReq.Filters[0].ResourceName)
}

func TestKafkaCreatePartitionsAndDescribeConfigs(t *testing.T) {
	var createPartitions *kmsg.CreatePartitionsRequest
	var describeConfigs *kmsg.DescribeConfigsRequest

	ks := NewKafkaServer(&KafkaServerCfg{
		CreatePartitionsFunc: func(ctx context.Context, req *kmsg.CreatePartitionsRequest) (*kmsg.CreatePartitionsResponse, error) {
			createPartitions = req
			resp := kmsg.NewPtrCreatePartitionsResponse()
			resp.Topics = []kmsg.CreatePartitionsResponseTopic{{Topic: "test-topic"}}
			return resp, nil
		},
		DescribeConfigsFunc: func(ctx context.Context, req *kmsg.DescribeConfigsRequest) (*kmsg.DescribeConfigsResponse, error) {
			describeConfigs = req
			resp := kmsg.NewPtrDescribeConfigsResponse()
			resp.Resources = []kmsg.DescribeConfigsResponseResource{{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: "test-topic",
			}}
			return resp, nil
		},
	})

	partitionsReq := kmsg.NewPtrCreatePartitionsRequest()
	partitionsReq.Topics = []kmsg.CreatePartitionsRequestTopic{{Topic: "test-topic", Count: 4}}
	partitionsRespAny, err := ks.HandleRequest(context.Background(), partitionsReq)
	require.NoError(t, err)
	partitionsResp := partitionsRespAny.(*kmsg.CreatePartitionsResponse)
	require.Len(t, partitionsResp.Topics, 1)
	assert.Equal(t, int32(4), createPartitions.Topics[0].Count)

	describeReq := kmsg.NewPtrDescribeConfigsRequest()
	describeReq.Resources = []kmsg.DescribeConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "test-topic",
	}}
	describeRespAny, err := ks.HandleRequest(context.Background(), describeReq)
	require.NoError(t, err)
	describeResp := describeRespAny.(*kmsg.DescribeConfigsResponse)
	require.Len(t, describeResp.Resources, 1)
	assert.Equal(t, "test-topic", describeConfigs.Resources[0].ResourceName)
}

func TestKafkaAlterConfigsAndIncrementalAlterConfigs(t *testing.T) {
	var alterReq *kmsg.AlterConfigsRequest
	var incReq *kmsg.IncrementalAlterConfigsRequest

	ks := NewKafkaServer(&KafkaServerCfg{
		AlterConfigsFunc: func(ctx context.Context, req *kmsg.AlterConfigsRequest) (*kmsg.AlterConfigsResponse, error) {
			alterReq = req
			resp := kmsg.NewPtrAlterConfigsResponse()
			resp.Resources = []kmsg.AlterConfigsResponseResource{{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: "test-topic",
			}}
			return resp, nil
		},
		IncrementalAlterConfigsFunc: func(ctx context.Context, req *kmsg.IncrementalAlterConfigsRequest) (*kmsg.IncrementalAlterConfigsResponse, error) {
			incReq = req
			resp := kmsg.NewPtrIncrementalAlterConfigsResponse()
			resp.Resources = []kmsg.IncrementalAlterConfigsResponseResource{{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: "test-topic",
			}}
			return resp, nil
		},
	})

	alter := kmsg.NewPtrAlterConfigsRequest()
	alter.Resources = []kmsg.AlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "test-topic",
	}}
	alterRespAny, err := ks.HandleRequest(context.Background(), alter)
	require.NoError(t, err)
	alterResp := alterRespAny.(*kmsg.AlterConfigsResponse)
	require.Len(t, alterResp.Resources, 1)
	assert.Equal(t, "test-topic", alterReq.Resources[0].ResourceName)

	inc := kmsg.NewPtrIncrementalAlterConfigsRequest()
	inc.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic,
		ResourceName: "test-topic",
	}}
	incRespAny, err := ks.HandleRequest(context.Background(), inc)
	require.NoError(t, err)
	incResp := incRespAny.(*kmsg.IncrementalAlterConfigsResponse)
	require.Len(t, incResp.Resources, 1)
	assert.Equal(t, "test-topic", incReq.Resources[0].ResourceName)
}

func TestKafkaDescribeCluster(t *testing.T) {
	var describeReq *kmsg.DescribeClusterRequest

	ks := NewKafkaServer(&KafkaServerCfg{
		DescribeClusterFunc: func(ctx context.Context, req *kmsg.DescribeClusterRequest) (*kmsg.DescribeClusterResponse, error) {
			describeReq = req
			resp := kmsg.NewPtrDescribeClusterResponse()
			resp.ClusterID = "camu-test"
			resp.ControllerID = 1
			resp.Brokers = []kmsg.DescribeClusterResponseBroker{{NodeID: 1, Host: "127.0.0.1", Port: 9092}}
			return resp, nil
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	respAny, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	resp := respAny.(*kmsg.DescribeClusterResponse)
	require.NotNil(t, describeReq)
	assert.Equal(t, "camu-test", resp.ClusterID)
	assert.Equal(t, int32(1), resp.ControllerID)
	require.Len(t, resp.Brokers, 1)
	assert.Equal(t, int32(1), resp.Brokers[0].NodeID)
}

func TestKafkaCreateDeleteTopics(t *testing.T) {
	var created *kmsg.CreateTopicsRequest
	var deleted *kmsg.DeleteTopicsRequest

	ks := NewKafkaServer(&KafkaServerCfg{
		CreateTopicsFunc: func(ctx context.Context, req *kmsg.CreateTopicsRequest) (*kmsg.CreateTopicsResponse, error) {
			created = req
			resp := kmsg.NewPtrCreateTopicsResponse()
			resp.Topics = []kmsg.CreateTopicsResponseTopic{{Topic: "test-topic", NumPartitions: 3, ReplicationFactor: 1}}
			return resp, nil
		},
		DeleteTopicsFunc: func(ctx context.Context, req *kmsg.DeleteTopicsRequest) (*kmsg.DeleteTopicsResponse, error) {
			deleted = req
			resp := kmsg.NewPtrDeleteTopicsResponse()
			resp.Topics = []kmsg.DeleteTopicsResponseTopic{{Topic: kmsg.StringPtr("test-topic")}}
			return resp, nil
		},
	})

	createReq := kmsg.NewPtrCreateTopicsRequest()
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "test-topic", NumPartitions: 3, ReplicationFactor: 1}}
	createRespAny, err := ks.HandleRequest(context.Background(), createReq)
	require.NoError(t, err)
	createResp := createRespAny.(*kmsg.CreateTopicsResponse)
	require.Len(t, createResp.Topics, 1)
	assert.Equal(t, "test-topic", created.Topics[0].Topic)

	deleteReq := kmsg.NewPtrDeleteTopicsRequest()
	deleteReq.TopicNames = []string{"test-topic"}
	deleteRespAny, err := ks.HandleRequest(context.Background(), deleteReq)
	require.NoError(t, err)
	deleteResp := deleteRespAny.(*kmsg.DeleteTopicsResponse)
	require.Len(t, deleteResp.Topics, 1)
	require.NotNil(t, deleted)
	assert.Equal(t, "test-topic", deleted.TopicNames[0])
}

func TestKafkaInitProducerID(t *testing.T) {
	ks := NewKafkaServer(&KafkaServerCfg{
		InitProducerIDFunc: func(ctx context.Context, req *kmsg.InitProducerIDRequest) (*kmsg.InitProducerIDResponse, error) {
			resp := kmsg.NewPtrInitProducerIDResponse()
			resp.ProducerID = 123
			resp.ProducerEpoch = 0
			return resp, nil
		},
	})

	req := kmsg.NewPtrInitProducerIDRequest()
	req.SetVersion(4)
	respAny, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	resp := respAny.(*kmsg.InitProducerIDResponse)
	assert.Equal(t, int16(0), resp.ErrorCode)
	assert.Equal(t, int64(123), resp.ProducerID)
	assert.Equal(t, int16(0), resp.ProducerEpoch)
}

func TestKafkaInitProducerIDRejectsTransactionalID(t *testing.T) {
	ks := NewKafkaServer(&KafkaServerCfg{})

	req := kmsg.NewPtrInitProducerIDRequest()
	req.SetVersion(4)
	req.TransactionalID = kmsg.StringPtr("txn-1")
	respAny, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	resp := respAny.(*kmsg.InitProducerIDResponse)
	assert.Equal(t, kafkaErrorInvalidRequest, resp.ErrorCode)
	assert.Equal(t, int64(-1), resp.ProducerID)
	assert.Equal(t, int16(-1), resp.ProducerEpoch)
}

func TestKafkaListOffsets(t *testing.T) {
	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{
			partitions: map[string]map[int]*PartitionInfo{
				"test-topic": {
					0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
				},
			},
		},
		BrokerID: 1,
		ListOffsetsFunc: func(ctx context.Context, topic string, partition int, timestamp int64) (KafkaOffsetResponse, error) {
			if topic != "test-topic" || partition != 0 {
				return KafkaOffsetResponse{}, errKafkaUnknownTopicPartition
			}
			if timestamp == -2 {
				return KafkaOffsetResponse{Offset: 3, Timestamp: -1, LeaderEpoch: 7}, nil
			}
			return KafkaOffsetResponse{Offset: 11, Timestamp: -1, LeaderEpoch: 7}, nil
		},
	})

	req := &kmsg.ListOffsetsRequest{
		Topics: []kmsg.ListOffsetsRequestTopic{{
			Topic: "test-topic",
			Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
				Partition: 0,
				Timestamp: -2,
			}},
		}},
	}

	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.ListOffsetsResponse)
	require.Len(t, apiResp.Topics, 1)
	require.Len(t, apiResp.Topics[0].Partitions, 1)

	partResp := apiResp.Topics[0].Partitions[0]
	assert.Equal(t, int16(0), partResp.ErrorCode)
	assert.Equal(t, int64(3), partResp.Offset)
	assert.Equal(t, int64(-1), partResp.Timestamp)
	assert.Equal(t, int32(7), partResp.LeaderEpoch)
}

func TestKafkaFindCoordinator(t *testing.T) {
	ks := NewKafkaServer(&KafkaServerCfg{
		BrokerID:   7,
		BrokerAddr: "127.0.0.1:19092",
	})

	req := &kmsg.FindCoordinatorRequest{CoordinatorKey: "group-1"}
	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.FindCoordinatorResponse)
	assert.Equal(t, int32(7), apiResp.NodeID)
	assert.Equal(t, "127.0.0.1", apiResp.Host)
	assert.Equal(t, int32(19092), apiResp.Port)
}

func TestKafkaOffsetCommitAndFetch(t *testing.T) {
	var committed *kmsg.OffsetCommitRequest

	ks := NewKafkaServer(&KafkaServerCfg{
		OffsetCommitFunc: func(ctx context.Context, req *kmsg.OffsetCommitRequest) (*kmsg.OffsetCommitResponse, error) {
			committed = req
			resp := kmsg.NewPtrOffsetCommitResponse()
			resp.Topics = []kmsg.OffsetCommitResponseTopic{{
				Topic: "test-topic",
				Partitions: []kmsg.OffsetCommitResponseTopicPartition{{
					Partition: 0,
				}},
			}}
			return resp, nil
		},
		OffsetFetchFunc: func(ctx context.Context, req *kmsg.OffsetFetchRequest) (*kmsg.OffsetFetchResponse, error) {
			resp := kmsg.NewPtrOffsetFetchResponse()
			resp.Topics = []kmsg.OffsetFetchResponseTopic{{
				Topic: "test-topic",
				Partitions: []kmsg.OffsetFetchResponseTopicPartition{{
					Partition:   0,
					Offset:      42,
					LeaderEpoch: -1,
				}},
			}}
			return resp, nil
		},
	})

	commitReq := &kmsg.OffsetCommitRequest{
		Group: "group-1",
		Topics: []kmsg.OffsetCommitRequestTopic{{
			Topic: "test-topic",
			Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
				Partition: 0,
				Offset:    42,
			}},
		}},
	}
	commitRespAny, err := ks.HandleRequest(context.Background(), commitReq)
	require.NoError(t, err)
	commitResp := commitRespAny.(*kmsg.OffsetCommitResponse)
	require.Len(t, commitResp.Topics, 1)
	require.NotNil(t, committed)
	assert.Equal(t, "group-1", committed.Group)

	fetchReq := &kmsg.OffsetFetchRequest{
		Group: "group-1",
		Topics: []kmsg.OffsetFetchRequestTopic{{
			Topic:      "test-topic",
			Partitions: []int32{0},
		}},
	}
	fetchRespAny, err := ks.HandleRequest(context.Background(), fetchReq)
	require.NoError(t, err)
	fetchResp := fetchRespAny.(*kmsg.OffsetFetchResponse)
	require.Len(t, fetchResp.Topics, 1)
	require.Len(t, fetchResp.Topics[0].Partitions, 1)
	assert.Equal(t, int64(42), fetchResp.Topics[0].Partitions[0].Offset)
}

func TestExtractRawRecordBatches(t *testing.T) {
	now := int64(1000)
	msgs1 := []log.Message{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now},
		{Key: []byte("k2"), Value: []byte("v2"), Timestamp: now + 1},
	}
	msgs2 := []log.Message{
		{Key: []byte("k3"), Value: []byte("v3"), Timestamp: now + 2},
	}

	batch1 := log.EncodeRecordBatch(0, msgs1)
	batch2 := log.EncodeRecordBatch(0, msgs2)

	// Concatenate two batches (as Kafka protocol would send them).
	combined := append(batch1, batch2...)

	batches, err := extractRawRecordBatches(combined)
	require.NoError(t, err)
	require.Len(t, batches, 2)

	// Verify first batch header.
	h1, err := log.ReadRecordBatchHeader(batches[0])
	require.NoError(t, err)
	assert.Equal(t, int32(1), h1.LastOffsetDelta) // 2 records => delta=1
	assert.Equal(t, int32(2), h1.NumRecords)

	// Verify second batch header.
	h2, err := log.ReadRecordBatchHeader(batches[1])
	require.NoError(t, err)
	assert.Equal(t, int32(0), h2.LastOffsetDelta) // 1 record => delta=0
	assert.Equal(t, int32(1), h2.NumRecords)
}

func TestExtractRawRecordBatches_Empty(t *testing.T) {
	batches, err := extractRawRecordBatches(nil)
	require.NoError(t, err)
	assert.Len(t, batches, 0)
}

func TestExtractRawRecordBatches_InvalidSize(t *testing.T) {
	// Build a valid batch then corrupt the Length field.
	batch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k"), Value: []byte("v"), Timestamp: 1}})
	// Set Length to a huge value (bytes [8:12]).
	batch[8] = 0xFF
	batch[9] = 0xFF
	batch[10] = 0xFF
	batch[11] = 0xFF

	_, err := extractRawRecordBatches(batch)
	assert.Error(t, err)
}
