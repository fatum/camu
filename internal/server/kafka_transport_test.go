package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/log"
)

func TestKafkaMetadata(t *testing.T) {
	pg := &mockPartitionGetter{
		partitions: map[string]map[int]*PartitionInfo{
			"test-topic": {
				0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
				1: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
				2: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			},
		},
	}
	tl := &mockTopicLister{
		topics: map[string]*TopicConfig{
			"test-topic": {Name: "test-topic", Partitions: 3},
		},
	}

	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: pg,
		TopicLister:     tl,
		BrokerID:        1,
		BrokerAddr:      "localhost:9092",
	})

	req := &kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{
			{Topic: strPtr("test-topic")},
		},
	}

	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.MetadataResponse)
	require.NotNil(t, apiResp)
	require.Len(t, apiResp.Topics, 1)

	topicResp := apiResp.Topics[0]
	assert.Equal(t, "test-topic", *topicResp.Topic)
	assert.Equal(t, int16(0), topicResp.ErrorCode)
	assert.Len(t, topicResp.Partitions, 3)
}

func TestKafkaProduce(t *testing.T) {
	var produced []struct {
		topic     string
		partition int
		msgs      [][]byte
	}

	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{
			partitions: map[string]map[int]*PartitionInfo{
				"test-topic": {
					0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
				},
			},
		},
		TopicLister: &mockTopicLister{
			topics: map[string]*TopicConfig{
				"test-topic": {Name: "test-topic", Partitions: 1},
			},
		},
		BrokerID: 1,
		AppendFunc: func(topic string, partition int, msgs []log.Message) ([]uint64, error) {
			produced = append(produced, struct {
				topic     string
				partition int
				msgs      [][]byte
			}{topic, partition, nil})
			offsets := make([]uint64, len(msgs))
			for i := range msgs {
				offsets[i] = uint64(i) + 100
			}
			return offsets, nil
		},
	})

	req := &kmsg.ProduceRequest{
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "test-topic",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   []byte("test-value"),
					},
				},
			},
		},
	}

	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.ProduceResponse)
	require.NotNil(t, apiResp)
	require.Len(t, apiResp.Topics, 1)

	topicResp := apiResp.Topics[0]
	require.Len(t, topicResp.Partitions, 1)

	partResp := topicResp.Partitions[0]
	assert.Equal(t, int32(0), partResp.Partition)
	assert.Equal(t, int16(0), partResp.ErrorCode)
	assert.Equal(t, int64(100), partResp.BaseOffset)
}

func TestKafkaFetch(t *testing.T) {
	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{
			partitions: map[string]map[int]*PartitionInfo{
				"test-topic": {
					0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
				},
			},
		},
		TopicLister: &mockTopicLister{},
		BrokerID:    1,
		FetchFunc: func(topic string, partition int, startOffset uint64, maxBytes int32) (KafkaFetchResult, error) {
			if topic == "test-topic" && partition == 0 && startOffset == 100 {
				return KafkaFetchResult{
					RecordBatches:   []byte("test-records"),
					HighWatermark:   123,
					LastStableOffset: 123,
				}, nil
			}
			return KafkaFetchResult{}, nil
		},
	})

	req := &kmsg.FetchRequest{
		Topics: []kmsg.FetchRequestTopic{
			{
				Topic: "test-topic",
				Partitions: []kmsg.FetchRequestTopicPartition{
					{
						Partition:         0,
						FetchOffset:       100,
						PartitionMaxBytes: 1024,
					},
				},
			},
		},
	}

	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.FetchResponse)
	require.NotNil(t, apiResp)
	require.Len(t, apiResp.Topics, 1)

	topicResp := apiResp.Topics[0]
	assert.Equal(t, "test-topic", topicResp.Topic)
	require.Len(t, topicResp.Partitions, 1)

	partResp := topicResp.Partitions[0]
	assert.Equal(t, int32(0), partResp.Partition)
	assert.Equal(t, int16(0), partResp.ErrorCode)
	assert.Equal(t, []byte("test-records"), partResp.RecordBatches)
	assert.Equal(t, int64(123), partResp.HighWatermark)
	assert.Equal(t, int64(123), partResp.LastStableOffset)
}

func TestDecodeKafkaProduceBatches_Compressed(t *testing.T) {
	tcs := []struct {
		name  string
		codec int16
	}{
		{name: "snappy", codec: 2},
		{name: "gzip", codec: 1},
		{name: "zstd", codec: 4},
		{name: "lz4", codec: 3},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			batchBytes := kafkaCompressedRecordBatchForTest(t, tc.codec, []testKafkaRecord{{Value: []byte("hello")}, {Value: []byte("world")}})
			batches, err := decodeKafkaProduceBatches(batchBytes)
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Len(t, batches[0].Messages, 2)
			assert.Equal(t, []byte("hello"), batches[0].Messages[0].Value)
			assert.Equal(t, []byte("world"), batches[0].Messages[1].Value)
		})
	}
}

type testKafkaRecord struct {
	Key   []byte
	Value []byte
}

var kafkaCRC32CTableForTest = crc32.MakeTable(crc32.Castagnoli)

func kafkaCompressedRecordBatchForTest(t *testing.T, codec int16, records []testKafkaRecord) []byte {
	t.Helper()

	recordBytes := make([]byte, 0, len(records)*32)
	now := int64(1_700_000_000_000)
	for i, record := range records {
		body := make([]byte, 0, len(record.Key)+len(record.Value)+16)
		body = kbin.AppendInt8(body, 0)
		body = kbin.AppendVarlong(body, 0)
		body = kbin.AppendVarint(body, int32(i))
		body = kbin.AppendVarintBytes(body, record.Key)
		body = kbin.AppendVarintBytes(body, record.Value)
		body = kbin.AppendVarint(body, 0)
		recordBytes = kbin.AppendVarint(recordBytes, int32(len(body)))
		recordBytes = append(recordBytes, body...)
	}

	compressed := kafkaCompressRecordBytesForTest(t, codec, recordBytes)
	batch := kmsg.RecordBatch{
		FirstOffset:          0,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           codec,
		LastOffsetDelta:      int32(len(records) - 1),
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(len(records)),
		Records:              compressed,
	}
	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], kafkaCRC32CTableForTest))
	return batch.AppendTo(nil)
}

func kafkaCompressRecordBytesForTest(t *testing.T, codec int16, data []byte) []byte {
	t.Helper()

	switch codec {
	case 1:
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, err := zw.Write(data)
		require.NoError(t, err)
		require.NoError(t, zw.Close())
		return buf.Bytes()
	case 2:
		return snappy.Encode(nil, data)
	case 3:
		var buf bytes.Buffer
		zw := lz4.NewWriter(&buf)
		_, err := zw.Write(data)
		require.NoError(t, err)
		require.NoError(t, zw.Close())
		return buf.Bytes()
	case 4:
		zw, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		defer zw.Close()
		return zw.EncodeAll(data, nil)
	default:
		t.Fatalf("unsupported test codec %d", codec)
		return nil
	}
}

func TestKafkaWireRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer ln.Close()

	var receivedReq kmsg.Request
	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{},
		TopicLister:     &mockTopicLister{},
		RequestHandler: func(req kmsg.Request) (kmsg.Response, error) {
			receivedReq = req
			return &kmsg.ApiVersionsResponse{
				ApiKeys: []kmsg.ApiVersionsResponseApiKey{
					{ApiKey: 18, MinVersion: 0, MaxVersion: 0},
				},
			}, nil
		},
	})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		ks.HandleConn(conn, ln)
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer client.Close()

	req := &kmsg.ApiVersionsRequest{}
	req.SetVersion(0)
	body := req.AppendTo(nil)

	var header [8]byte
	header[0] = 0
	header[1] = 18
	header[2] = 0
	header[3] = 0

	totalLen := uint32(len(header) + len(body))
	var lenBuf [4]byte
	lenBuf[0] = byte(totalLen >> 24)
	lenBuf[1] = byte(totalLen >> 16)
	lenBuf[2] = byte(totalLen >> 8)
	lenBuf[3] = byte(totalLen)

	_, err = client.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = client.Write(header[:])
	require.NoError(t, err)
	_, err = client.Write(body)
	require.NoError(t, err)

	respLenBuf := make([]byte, 4)
	_, err = io.ReadFull(client, respLenBuf)
	require.NoError(t, err)
	respLen := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])

	respBody := make([]byte, respLen)
	_, err = io.ReadFull(client, respBody)
	require.NoError(t, err)

	assert.NotNil(t, receivedReq, "server should receive request")
}

func TestKafkaHandleConn_ClosesOnDecodeError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	_, err := clientConn.Write([]byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0})
	require.NoError(t, err)

	buf := make([]byte, 1)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = clientConn.Read(buf)
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after decode error")
	}
}

func TestKafkaHandleConn_ClosesOnHandleError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{
		RequestHandler: func(req kmsg.Request) (kmsg.Response, error) {
			return nil, assert.AnError
		},
	})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	req := &kmsg.ApiVersionsRequest{}
	req.SetVersion(0)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	_, err := clientConn.Write(frame)
	require.NoError(t, err)

	buf := make([]byte, 1)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = clientConn.Read(buf)
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after handle error")
	}
}

type mockPartitionGetter struct {
	partitions map[string]map[int]*PartitionInfo
}

func (m *mockPartitionGetter) GetPartitionInfo(topic string, partition int) (*PartitionInfo, bool) {
	if m.partitions == nil {
		return nil, false
	}
	parts, ok := m.partitions[topic]
	if !ok {
		return nil, false
	}
	info, ok := parts[partition]
	return info, ok
}

type mockTopicLister struct {
	topics map[string]*TopicConfig
}

func (m *mockTopicLister) ListTopics() ([]*TopicConfig, error) {
	if m.topics == nil {
		return nil, nil
	}
	ret := make([]*TopicConfig, 0, len(m.topics))
	for _, t := range m.topics {
		ret = append(ret, t)
	}
	return ret, nil
}

func strPtr(s string) *string {
	return &s
}
