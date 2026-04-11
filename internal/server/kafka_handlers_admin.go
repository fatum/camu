package server

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func (ks *KafkaServer) handleAPIVersions(req *kmsg.ApiVersionsRequest) kmsg.Response {
	resp := kmsg.NewPtrApiVersionsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: 0, MinVersion: 0, MaxVersion: 9},
		{ApiKey: 1, MinVersion: 0, MaxVersion: 12},
		{ApiKey: 2, MinVersion: 0, MaxVersion: 10},
		{ApiKey: 3, MinVersion: 0, MaxVersion: 12},
		{ApiKey: 8, MinVersion: 0, MaxVersion: 8},
		{ApiKey: 9, MinVersion: 0, MaxVersion: 7},
		{ApiKey: 10, MinVersion: 0, MaxVersion: 4},
		{ApiKey: 11, MinVersion: 0, MaxVersion: 9},
		{ApiKey: 12, MinVersion: 0, MaxVersion: 4},
		{ApiKey: 13, MinVersion: 0, MaxVersion: 4},
		{ApiKey: 14, MinVersion: 0, MaxVersion: 5},
		{ApiKey: 15, MinVersion: 0, MaxVersion: 5},
		{ApiKey: 16, MinVersion: 0, MaxVersion: 5},
		{ApiKey: 18, MinVersion: 0, MaxVersion: 3},
		{ApiKey: 19, MinVersion: 0, MaxVersion: 7},
		{ApiKey: 20, MinVersion: 0, MaxVersion: 6},
		{ApiKey: 22, MinVersion: 0, MaxVersion: 5},
		{ApiKey: 29, MinVersion: 0, MaxVersion: 3},
		{ApiKey: 30, MinVersion: 0, MaxVersion: 3},
		{ApiKey: 31, MinVersion: 0, MaxVersion: 3},
		{ApiKey: 32, MinVersion: 0, MaxVersion: 4},
		{ApiKey: 33, MinVersion: 0, MaxVersion: 2},
		{ApiKey: 37, MinVersion: 0, MaxVersion: 3},
		{ApiKey: 42, MinVersion: 0, MaxVersion: 2},
		{ApiKey: 44, MinVersion: 0, MaxVersion: 1},
		{ApiKey: 47, MinVersion: 0, MaxVersion: 0},
		{ApiKey: 60, MinVersion: 0, MaxVersion: 2},
	}
	return resp
}

func (ks *KafkaServer) handleCreateTopics(req *kmsg.CreateTopicsRequest) (kmsg.Response, error) {
	if ks.cfg.CreateTopicsFunc != nil {
		return ks.cfg.CreateTopicsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrCreateTopicsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleDeleteTopics(req *kmsg.DeleteTopicsRequest) (kmsg.Response, error) {
	if ks.cfg.DeleteTopicsFunc != nil {
		return ks.cfg.DeleteTopicsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrDeleteTopicsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleCreatePartitions(req *kmsg.CreatePartitionsRequest) (kmsg.Response, error) {
	if ks.cfg.CreatePartitionsFunc != nil {
		return ks.cfg.CreatePartitionsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrCreatePartitionsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleDescribeConfigs(req *kmsg.DescribeConfigsRequest) (kmsg.Response, error) {
	if ks.cfg.DescribeConfigsFunc != nil {
		return ks.cfg.DescribeConfigsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrDescribeConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleAlterConfigs(req *kmsg.AlterConfigsRequest) (kmsg.Response, error) {
	if ks.cfg.AlterConfigsFunc != nil {
		return ks.cfg.AlterConfigsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrAlterConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleIncrementalAlterConfigs(req *kmsg.IncrementalAlterConfigsRequest) (kmsg.Response, error) {
	if ks.cfg.IncrementalAlterConfigsFunc != nil {
		return ks.cfg.IncrementalAlterConfigsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrIncrementalAlterConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleDescribeCluster(req *kmsg.DescribeClusterRequest) (kmsg.Response, error) {
	if ks.cfg.DescribeClusterFunc != nil {
		return ks.cfg.DescribeClusterFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrDescribeClusterResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleCreateACLs(req *kmsg.CreateACLsRequest) (kmsg.Response, error) {
	if ks.cfg.CreateACLsFunc != nil {
		return ks.cfg.CreateACLsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrCreateACLsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleDescribeACLs(req *kmsg.DescribeACLsRequest) (kmsg.Response, error) {
	if ks.cfg.DescribeACLsFunc != nil {
		return ks.cfg.DescribeACLsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrDescribeACLsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleDeleteACLs(req *kmsg.DeleteACLsRequest) (kmsg.Response, error) {
	if ks.cfg.DeleteACLsFunc != nil {
		return ks.cfg.DeleteACLsFunc(context.Background(), req)
	}
	resp := kmsg.NewPtrDeleteACLsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	return resp, nil
}

func (ks *KafkaServer) handleInitProducerID(req *kmsg.InitProducerIDRequest) (kmsg.Response, error) {
	if ks.cfg.InitProducerIDFunc != nil {
		return ks.cfg.InitProducerIDFunc(context.Background(), req)
	}

	resp := kmsg.NewPtrInitProducerIDResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	resp.ProducerID = -1
	resp.ProducerEpoch = -1
	if req.TransactionalID != nil {
		resp.ErrorCode = kafkaErrorInvalidRequest
	}
	return resp, nil
}

func (ks *KafkaServer) handleFindCoordinator(req *kmsg.FindCoordinatorRequest) (kmsg.Response, error) {
	if ks.cfg.FindCoordinatorFunc != nil {
		return ks.cfg.FindCoordinatorFunc(context.Background(), req)
	}

	resp := kmsg.NewPtrFindCoordinatorResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	host, port := splitKafkaBrokerAddr(ks.cfg.BrokerAddr)

	if req.GetVersion() >= 4 {
		keys := req.CoordinatorKeys
		if len(keys) == 0 && req.CoordinatorKey != "" {
			keys = []string{req.CoordinatorKey}
		}
		for _, key := range keys {
			resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
				Key:    key,
				NodeID: ks.cfg.BrokerID,
				Host:   host,
				Port:   port,
			})
		}
		return resp, nil
	}

	resp.NodeID = ks.cfg.BrokerID
	resp.Host = host
	resp.Port = port
	return resp, nil
}

func (ks *KafkaServer) handleDescribeGroups(req *kmsg.DescribeGroupsRequest) (kmsg.Response, error) {
	if ks.cfg.DescribeGroupsFunc != nil {
		return ks.cfg.DescribeGroupsFunc(context.Background(), req)
	}
	return kmsg.NewPtrDescribeGroupsResponse(), nil
}

func (ks *KafkaServer) handleListGroups(req *kmsg.ListGroupsRequest) (kmsg.Response, error) {
	if ks.cfg.ListGroupsFunc != nil {
		return ks.cfg.ListGroupsFunc(context.Background(), req)
	}
	return kmsg.NewPtrListGroupsResponse(), nil
}

func (ks *KafkaServer) handleDeleteGroups(req *kmsg.DeleteGroupsRequest) (kmsg.Response, error) {
	if ks.cfg.DeleteGroupsFunc != nil {
		return ks.cfg.DeleteGroupsFunc(context.Background(), req)
	}
	return kmsg.NewPtrDeleteGroupsResponse(), nil
}

func (ks *KafkaServer) handleOffsetDelete(req *kmsg.OffsetDeleteRequest) (kmsg.Response, error) {
	if ks.cfg.OffsetDeleteFunc != nil {
		return ks.cfg.OffsetDeleteFunc(context.Background(), req)
	}
	return kmsg.NewPtrOffsetDeleteResponse(), nil
}

func (ks *KafkaServer) handleJoinGroup(req *kmsg.JoinGroupRequest) (kmsg.Response, error) {
	if ks.cfg.JoinGroupFunc != nil {
		return ks.cfg.JoinGroupFunc(context.Background(), req)
	}
	return kmsg.NewPtrJoinGroupResponse(), nil
}

func (ks *KafkaServer) handleSyncGroup(req *kmsg.SyncGroupRequest) (kmsg.Response, error) {
	if ks.cfg.SyncGroupFunc != nil {
		return ks.cfg.SyncGroupFunc(context.Background(), req)
	}
	return kmsg.NewPtrSyncGroupResponse(), nil
}

func (ks *KafkaServer) handleHeartbeat(req *kmsg.HeartbeatRequest) (kmsg.Response, error) {
	if ks.cfg.HeartbeatFunc != nil {
		return ks.cfg.HeartbeatFunc(context.Background(), req)
	}
	return kmsg.NewPtrHeartbeatResponse(), nil
}

func (ks *KafkaServer) handleLeaveGroup(req *kmsg.LeaveGroupRequest) (kmsg.Response, error) {
	if ks.cfg.LeaveGroupFunc != nil {
		return ks.cfg.LeaveGroupFunc(context.Background(), req)
	}
	return kmsg.NewPtrLeaveGroupResponse(), nil
}

func (ks *KafkaServer) handleListOffsets(req *kmsg.ListOffsetsRequest) (kmsg.Response, error) {
	resp := kmsg.NewPtrListOffsetsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, topic := range req.Topics {
		topicResp := kmsg.NewListOffsetsResponseTopic()
		topicResp.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			partResp := kmsg.NewListOffsetsResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.Offset = -1
			partResp.Timestamp = -1
			partResp.LeaderEpoch = -1

			errorCode := ks.partitionError(topic.Topic, int(partition.Partition))
			if errorCode == 0 && ks.cfg.ListOffsetsFunc != nil {
				offsetResp, err := ks.cfg.ListOffsetsFunc(context.Background(), topic.Topic, int(partition.Partition), partition.Timestamp)
				if err != nil {
					errorCode = mapKafkaError(err)
				} else {
					partResp.Offset = offsetResp.Offset
					partResp.Timestamp = offsetResp.Timestamp
					partResp.LeaderEpoch = offsetResp.LeaderEpoch
					if req.GetVersion() == 0 {
						partResp.OldStyleOffsets = []int64{offsetResp.Offset}
					}
				}
			}
			partResp.ErrorCode = errorCode
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

func (ks *KafkaServer) handleOffsetCommit(req *kmsg.OffsetCommitRequest) (kmsg.Response, error) {
	if ks.cfg.OffsetCommitFunc != nil {
		return ks.cfg.OffsetCommitFunc(context.Background(), req)
	}

	resp := kmsg.NewPtrOffsetCommitResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetCommitResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetCommitResponseTopicPartition()
			partResp.Partition = partition.Partition
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (ks *KafkaServer) handleOffsetFetch(req *kmsg.OffsetFetchRequest) (kmsg.Response, error) {
	if ks.cfg.OffsetFetchFunc != nil {
		return ks.cfg.OffsetFetchFunc(context.Background(), req)
	}

	resp := kmsg.NewPtrOffsetFetchResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetFetchResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetFetchResponseTopicPartition()
			partResp.Partition = partition
			partResp.Offset = -1
			partResp.LeaderEpoch = -1
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (ks *KafkaServer) handleMetadata(req *kmsg.MetadataRequest) (kmsg.Response, error) {
	if ks.cfg.MetadataFunc != nil {
		return ks.cfg.MetadataFunc(context.Background(), req)
	}

	resp := kmsg.NewPtrMetadataResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	resp.ControllerID = ks.cfg.BrokerID

	host, port := splitKafkaBrokerAddr(ks.cfg.BrokerAddr)
	resp.Brokers = []kmsg.MetadataResponseBroker{{
		NodeID: ks.cfg.BrokerID,
		Host:   host,
		Port:   port,
	}}

	topics, err := ks.listTopics()
	if err != nil {
		return nil, err
	}

	requested := requestedMetadataTopics(req)
	seenTopics := make(map[string]struct{}, len(requested))
	for _, topic := range topics {
		if len(requested) > 0 && !requested[topic.Name] {
			continue
		}

		topicResp := kmsg.NewMetadataResponseTopic()
		topicResp.Topic = kmsg.StringPtr(topic.Name)
		for partition := 0; partition < topic.Partitions; partition++ {
			info, ok := ks.lookupPartition(topic.Name, partition)
			if !ok {
				continue
			}

			partResp := kmsg.NewMetadataResponseTopicPartition()
			partResp.Partition = int32(partition)
			partResp.Leader = info.Leader
			partResp.Replicas = append(partResp.Replicas, info.Replicas...)
			partResp.ISR = append(partResp.ISR, info.ISR...)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
		seenTopics[topic.Name] = struct{}{}
	}

	for topicName := range requested {
		if _, ok := seenTopics[topicName]; ok {
			continue
		}
		topicResp := kmsg.NewMetadataResponseTopic()
		topicResp.Topic = kmsg.StringPtr(topicName)
		topicResp.ErrorCode = kafkaErrorUnknownTopicPartition
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}
