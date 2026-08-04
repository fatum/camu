package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/log"
)

func kafkaProduceAckServer(appendErr error, waitErr error) (*KafkaServer, *struct {
	topic     string
	partition int
	offset    uint64
	calls     int
}) {
	waited := &struct {
		topic     string
		partition int
		offset    uint64
		calls     int
	}{}

	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{
			partitions: map[string]map[int]*PartitionInfo{
				"test-topic": {0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}},
			},
		},
		TopicLister: &mockTopicLister{
			topics: map[string]*TopicConfig{"test-topic": {Name: "test-topic", Partitions: 1}},
		},
		BrokerID: 1,
		AppendRawBatchFunc: func(ctx context.Context, topic string, partition int, b []byte) (int64, error) {
			return 100, appendErr
		},
		WaitForReplicatedFunc: func(ctx context.Context, topic string, partition int, offset uint64) error {
			waited.topic = topic
			waited.partition = partition
			waited.offset = offset
			waited.calls++
			return waitErr
		},
	})
	return ks, waited
}

func produceRequest(acks int16, records []byte) *kmsg.ProduceRequest {
	return &kmsg.ProduceRequest{
		Acks: acks,
		Topics: []kmsg.ProduceRequestTopic{
			{Topic: "test-topic", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: records}}},
		},
	}
}

func TestKafkaProduceAcksAllWaitsForReplication(t *testing.T) {
	batch := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	})
	ks, waited := kafkaProduceAckServer(nil, nil)

	resp, err := ks.HandleRequest(context.Background(), produceRequest(-1, batch))
	require.NoError(t, err)

	partResp := resp.(*kmsg.ProduceResponse).Topics[0].Partitions[0]
	assert.Equal(t, int16(0), partResp.ErrorCode)
	assert.Equal(t, int64(100), partResp.BaseOffset)
	assert.Equal(t, 1, waited.calls)
	assert.Equal(t, "test-topic", waited.topic)
	assert.Equal(t, 0, waited.partition)
	// Base offset 100 + LastOffsetDelta 1 => replicated offset is the last one.
	assert.Equal(t, uint64(101), waited.offset)
}

func TestKafkaProduceAcksOneWaitsForReplication(t *testing.T) {
	batch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k1"), Value: []byte("v1")}})
	ks, waited := kafkaProduceAckServer(nil, nil)

	resp, err := ks.HandleRequest(context.Background(), produceRequest(1, batch))
	require.NoError(t, err)

	partResp := resp.(*kmsg.ProduceResponse).Topics[0].Partitions[0]
	assert.Equal(t, int16(0), partResp.ErrorCode)
	assert.Equal(t, 1, waited.calls)
	assert.Equal(t, uint64(100), waited.offset)
}

func TestKafkaProduceAcksZeroSkipsReplicationWait(t *testing.T) {
	batch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k1"), Value: []byte("v1")}})
	ks, waited := kafkaProduceAckServer(nil, nil)

	resp, err := ks.HandleRequest(context.Background(), produceRequest(0, batch))
	require.NoError(t, err)

	partResp := resp.(*kmsg.ProduceResponse).Topics[0].Partitions[0]
	assert.Equal(t, int16(0), partResp.ErrorCode)
	assert.Equal(t, int64(100), partResp.BaseOffset)
	assert.Equal(t, 0, waited.calls)
}

func TestKafkaProduceReplicationWaitErrorSetsServerError(t *testing.T) {
	batch := log.EncodeRecordBatch(0, []log.Message{{Key: []byte("k1"), Value: []byte("v1")}})
	ks, waited := kafkaProduceAckServer(nil, errors.New("replication timeout"))

	resp, err := ks.HandleRequest(context.Background(), produceRequest(-1, batch))
	require.NoError(t, err)

	partResp := resp.(*kmsg.ProduceResponse).Topics[0].Partitions[0]
	assert.Equal(t, int16(kafkaErrorUnknownServer), partResp.ErrorCode)
	assert.Equal(t, 1, waited.calls)
}
