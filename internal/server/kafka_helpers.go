package server

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/idempotency"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/producer"
)

func requestedMetadataTopics(req *kmsg.MetadataRequest) map[string]bool {
	if len(req.Topics) == 0 {
		return nil
	}
	requested := make(map[string]bool, len(req.Topics))
	for _, topic := range req.Topics {
		if topic.Topic != nil {
			requested[*topic.Topic] = true
		}
	}
	return requested
}

func (ks *KafkaServer) listTopics() ([]*TopicConfig, error) {
	if ks.cfg.TopicLister == nil {
		return nil, nil
	}
	return ks.cfg.TopicLister.ListTopics()
}

func (ks *KafkaServer) lookupPartition(topic string, partition int) (*PartitionInfo, bool) {
	if ks.cfg.PartitionGetter == nil {
		return &PartitionInfo{
			Leader:   ks.cfg.BrokerID,
			Replicas: []int32{ks.cfg.BrokerID},
			ISR:      []int32{ks.cfg.BrokerID},
		}, true
	}
	info, ok := ks.cfg.PartitionGetter.GetPartitionInfo(topic, partition)
	if !ok {
		return nil, false
	}
	if len(info.Replicas) == 0 {
		info.Replicas = []int32{info.Leader}
	}
	if len(info.ISR) == 0 {
		info.ISR = append([]int32(nil), info.Replicas...)
	}
	return info, true
}

func (ks *KafkaServer) partitionError(topic string, partition int) int16 {
	if ks.cfg.PartitionErrorFunc != nil {
		return ks.cfg.PartitionErrorFunc(context.Background(), topic, partition)
	}
	info, ok := ks.lookupPartition(topic, partition)
	if !ok {
		return kafkaErrorUnknownTopicPartition
	}
	if info.Leader != ks.cfg.BrokerID {
		return kafkaErrorNotLeader
	}
	return 0
}

func mapKafkaError(err error) int16 {
	switch {
	case errors.Is(err, errKafkaUnknownTopicPartition):
		return kafkaErrorUnknownTopicPartition
	case errors.Is(err, errKafkaLeaderNotAvailable):
		return kafkaErrorLeaderNotAvailable
	case errors.Is(err, errKafkaNotLeader):
		return kafkaErrorNotLeader
	case errors.Is(err, errKafkaInvalidRequest):
		return kafkaErrorInvalidRequest
	case errors.Is(err, errKafkaSegmentNotReady):
		return kafkaErrorLeaderNotAvailable
	case errors.Is(err, errKafkaInvalidRecordBatch):
		return kafkaErrorCorruptMessage
	case errors.Is(err, producer.ErrBackpressure):
		return kafkaErrorLeaderNotAvailable
	case errors.Is(err, idempotency.ErrUnknownProducer):
		return kafkaErrorUnknownProducerID
	case errors.Is(err, idempotency.ErrSequenceGap):
		return kafkaErrorOutOfOrderSequence
	case errors.Is(err, idempotency.ErrDuplicateSequence):
		return kafkaErrorDuplicateSequence
	case errors.Is(err, diskless.ErrSequenceGap), errors.Is(err, diskless.ErrOutOfOrderSequence):
		return kafkaErrorOutOfOrderSequence
	case errors.Is(err, diskless.ErrProduceRetryable):
		// The batch was not recorded in the object-store metastore (transient
		// S3 failure or deadline). This must be retriable: an idempotent
		// client re-sends the same batch with the same producer sequence and
		// the retry either commits it or deduplicates it as an exact retry.
		// A non-retriable code here would make the client advance past the
		// unrecorded batch and permanently gap the partition.
		return kafkaErrorRequestTimedOut
	default:
		return kafkaErrorUnknownServer
	}
}

func splitKafkaBrokerAddr(addr string) (string, int32) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1", 9092
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return host, 9092
	}
	return host, int32(port)
}

func setKafkaResponseVersion(resp kmsg.Response, reqVersion int16) {
	version := reqVersion
	if version > resp.MaxVersion() {
		version = resp.MaxVersion()
	}
	if version < 0 {
		version = 0
	}
	resp.SetVersion(version)
}

type kafkaPartitionGetter struct {
	pm       *PartitionManager
	brokerID int32
}

func (p *kafkaPartitionGetter) GetPartitionInfo(topic string, partition int) (*PartitionInfo, bool) {
	p.pm.mu.RLock()
	defer p.pm.mu.RUnlock()

	tp, ok := p.pm.partitions[topic]
	if !ok {
		return nil, false
	}

	ps, ok := tp[partition]
	if !ok {
		return nil, false
	}

	info := &PartitionInfo{
		Leader:   0,
		Replicas: []int32{p.brokerID},
		ISR:      []int32{},
	}
	if ps.isLeader || ps.leaderID == "" {
		info.Leader = p.brokerID
		info.ISR = []int32{p.brokerID}
	}
	return info, true
}

type kafkaTopicLister struct {
	ts *meta.TopicStore
}

func (t *kafkaTopicLister) ListTopics() ([]*TopicConfig, error) {
	topics, err := t.ts.List(context.Background())
	if err != nil {
		return nil, err
	}

	result := make([]*TopicConfig, 0, len(topics))
	for _, topic := range topics {
		result = append(result, &TopicConfig{
			Name:       topic.Name,
			Partitions: topic.Partitions,
		})
	}
	return result, nil
}
