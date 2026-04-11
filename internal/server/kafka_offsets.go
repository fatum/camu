package server

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/storage"
)

func (s *Server) handleKafkaOffsetDelete(ctx context.Context, req *kmsg.OffsetDeleteRequest) (*kmsg.OffsetDeleteResponse, error) {
	resp := kmsg.NewPtrOffsetDeleteResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	if !s.isLocalKafkaCoordinator(ctx, req.Group) {
		resp.ErrorCode = kafkaErrorNotCoordinator
		for _, topic := range req.Topics {
			topicResp := kmsg.NewOffsetDeleteResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetDeleteResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = kafkaErrorNotCoordinator
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return resp, nil
	}

	if _, err := s.s3Client.Get(ctx, kafkaGroupKey(req.Group)); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			resp.ErrorCode = kafkaErrorGroupIDNotFound
			return resp, nil
		}
		return nil, err
	}

	deletes := make(map[string][]int, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetDeleteResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetDeleteResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.ErrorCode = s.kafkaPartitionExists(ctx, topic.Topic, int(partition.Partition))
			if partResp.ErrorCode == 0 {
				deletes[topic.Topic] = append(deletes[topic.Topic], int(partition.Partition))
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	if len(deletes) > 0 {
		if err := s.offsetStore.DeleteGroupTopics(ctx, req.Group, deletes, s.currentControllerEpoch()); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (s *Server) isLocalKafkaCoordinator(ctx context.Context, groupKey string) bool {
	brokerID, _, _, err := s.kafkaControllerBroker(ctx)
	if err != nil {
		return false
	}
	return brokerID == kafkaBrokerID(s.instanceID)
}

func (s *Server) currentControllerEpoch() string {
	if s.leaderLease.ETag != "" {
		return s.leaderLease.ETag
	}
	lease, err := s.leaderElection.GetLeader(context.Background())
	if err != nil {
		return ""
	}
	return lease.ETag
}

func (s *Server) handleKafkaOffsetCommit(ctx context.Context, req *kmsg.OffsetCommitRequest) (*kmsg.OffsetCommitResponse, error) {
	resp := kmsg.NewPtrOffsetCommitResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	if !s.isLocalKafkaCoordinator(ctx, req.Group) {
		for _, topic := range req.Topics {
			topicResp := kmsg.NewOffsetCommitResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetCommitResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = kafkaErrorNotCoordinator
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return resp, nil
	}

	offsetsByTopic := make(map[string]map[int]uint64, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetCommitResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetCommitResponseTopicPartition()
			partResp.Partition = partition.Partition

			errorCode := s.kafkaPartitionExists(ctx, topic.Topic, int(partition.Partition))
			if errorCode == 0 {
				if offsetsByTopic[topic.Topic] == nil {
					offsetsByTopic[topic.Topic] = make(map[int]uint64)
				}
				offsetsByTopic[topic.Topic][int(partition.Partition)] = uint64(partition.Offset)
			}
			partResp.ErrorCode = errorCode
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	if len(offsetsByTopic) > 0 {
		if err := s.offsetStore.CommitGroupTopicsWithEpoch(ctx, req.Group, offsetsByTopic, s.currentControllerEpoch()); err != nil {
			for ti := range resp.Topics {
				for pi := range resp.Topics[ti].Partitions {
					if resp.Topics[ti].Partitions[pi].ErrorCode == 0 {
						resp.Topics[ti].Partitions[pi].ErrorCode = kafkaErrorUnknownServer
					}
				}
			}
		}
	}

	return resp, nil
}

func (s *Server) handleKafkaOffsetFetch(ctx context.Context, req *kmsg.OffsetFetchRequest) (*kmsg.OffsetFetchResponse, error) {
	resp := kmsg.NewPtrOffsetFetchResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	if !s.isLocalKafkaCoordinator(ctx, req.Group) {
		requestedTopics := req.Topics
		for _, topic := range requestedTopics {
			topicResp := kmsg.NewOffsetFetchResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetFetchResponseTopicPartition()
				partResp.Partition = partition
				partResp.Offset = -1
				partResp.LeaderEpoch = -1
				partResp.ErrorCode = kafkaErrorNotCoordinator
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return resp, nil
	}

	topics, err := s.offsetStore.GetGroupTopics(ctx, req.Group)
	if err != nil {
		return nil, err
	}

	requestedTopics := req.Topics
	if len(requestedTopics) == 0 {
		requestedTopics = make([]kmsg.OffsetFetchRequestTopic, 0, len(topics))
		for topic, partitions := range topics {
			topicReq := kmsg.NewOffsetFetchRequestTopic()
			topicReq.Topic = topic
			for partition := range partitions {
				topicReq.Partitions = append(topicReq.Partitions, int32(partition))
			}
			requestedTopics = append(requestedTopics, topicReq)
		}
	}

	for _, topic := range requestedTopics {
		topicResp := kmsg.NewOffsetFetchResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetFetchResponseTopicPartition()
			partResp.Partition = partition
			partResp.Offset = -1
			partResp.LeaderEpoch = -1

			errorCode := s.kafkaPartitionExists(ctx, topic.Topic, int(partition))
			if errorCode == 0 {
				if topicOffsets := topics[topic.Topic]; topicOffsets != nil {
					if offset, ok := topicOffsets[int(partition)]; ok {
						partResp.Offset = int64(offset)
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

func (s *Server) kafkaPartitionError(ctx context.Context, topic string, partition int) int16 {
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		return kafkaErrorUnknownTopicPartition
	}
	if partition < 0 || partition >= topicCfg.Partitions {
		return kafkaErrorUnknownTopicPartition
	}

	assignments, err := s.assignmentStore.Read(ctx, topic)
	if err != nil {
		return kafkaErrorUnknownTopicPartition
	}
	assignment, ok := assignments.Partitions[partition]
	if !ok {
		return kafkaErrorUnknownTopicPartition
	}
	if assignment.Leader != s.instanceID {
		return kafkaErrorNotLeader
	}
	return 0
}

func (s *Server) kafkaPartitionExists(ctx context.Context, topic string, partition int) int16 {
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		return kafkaErrorUnknownTopicPartition
	}
	if partition < 0 || partition >= topicCfg.Partitions {
		return kafkaErrorUnknownTopicPartition
	}
	return 0
}
