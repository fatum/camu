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

func TestKafkaMetadataIncludesUnknownRequestedTopic(t *testing.T) {
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
		BrokerID:   1,
		BrokerAddr: "localhost:9092",
	})

	req := &kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{
			{Topic: strPtr("missing-topic")},
		},
	}

	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.MetadataResponse)
	require.Len(t, apiResp.Topics, 1)
	require.NotNil(t, apiResp.Topics[0].Topic)
	assert.Equal(t, "missing-topic", *apiResp.Topics[0].Topic)
	assert.Equal(t, kafkaErrorUnknownTopicPartition, apiResp.Topics[0].ErrorCode)
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
					RecordBatches:    []byte("test-records"),
					HighWatermark:    123,
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

func TestKafkaFetchRawBatchesCapsPartitionBytesAndPropagatesContext(t *testing.T) {
	assert.Equal(t, 16<<20, maxKafkaFetchPartitionBytes)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var gotContext context.Context
	var gotMaxBytes int
	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{partitions: map[string]map[int]*PartitionInfo{
			"test-topic": {0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}},
		}},
		TopicLister: &mockTopicLister{},
		BrokerID:    1,
		FetchRawBatchesFunc: func(fetchCtx context.Context, _ string, _ int, _ int64, maxBytes int) ([]byte, int64, error) {
			gotContext = fetchCtx
			gotMaxBytes = maxBytes
			return []byte("raw"), 3, nil
		},
	})

	resp, err := ks.HandleRequest(ctx, &kmsg.FetchRequest{Topics: []kmsg.FetchRequestTopic{{
		Topic: "test-topic",
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition: 0, PartitionMaxBytes: maxKafkaFetchPartitionBytes + 1,
		}},
	}}})
	require.NoError(t, err)
	assert.Same(t, ctx, gotContext)
	assert.Equal(t, maxKafkaFetchPartitionBytes, gotMaxBytes)
	assert.Equal(t, []byte("raw"), resp.(*kmsg.FetchResponse).Topics[0].Partitions[0].RecordBatches)
}

func TestKafkaListOffsetsMapsInvalidRequestError(t *testing.T) {
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
			return KafkaOffsetResponse{}, errKafkaInvalidRequest
		},
	})

	req := kmsg.NewPtrListOffsetsRequest()
	req.Topics = []kmsg.ListOffsetsRequestTopic{{
		Topic: "test-topic",
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
			Partition: 0,
			Timestamp: 1234,
		}},
	}}

	resp, err := ks.HandleRequest(context.Background(), req)
	require.NoError(t, err)

	apiResp := resp.(*kmsg.ListOffsetsResponse)
	require.Len(t, apiResp.Topics, 1)
	require.Len(t, apiResp.Topics[0].Partitions, 1)
	assert.Equal(t, kafkaErrorInvalidRequest, apiResp.Topics[0].Partitions[0].ErrorCode)
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

	receivedReq := make(chan kmsg.Request, 1)
	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: &mockPartitionGetter{},
		TopicLister:     &mockTopicLister{},
		RequestHandler: func(req kmsg.Request) (kmsg.Response, error) {
			receivedReq <- req
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

	select {
	case req := <-receivedReq:
		assert.NotNil(t, req, "server should receive request")
	case <-time.After(time.Second):
		t.Fatal("server did not receive request")
	}
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

func TestKafkaHandleConn_ClosesOnUnsupportedAPIKey(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	frame := []byte{
		0, 0, 0, 8,
		3, 231, // api key 999
		0, 0, // version 0
		0, 0, 0, 1, // correlation id 1
	}
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
		t.Fatal("HandleConn did not exit after unsupported API key")
	}
}

func TestDecodeKafkaRequestRejectsUnsupportedAPIKey(t *testing.T) {
	_, _, err := decodeKafkaRequest([]byte{
		3, 231, // api key 999
		0, 0, // version 0
		0, 0, 0, 1, // correlation id 1
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported API key")
}

func TestWriteKafkaResponseClampsToResponseMaxVersion(t *testing.T) {
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(99)
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MinVersion: 0, MaxVersion: 3}}

	var buf bytes.Buffer
	err := writeKafkaResponse(&buf, 7, req, resp)
	require.NoError(t, err)
	assert.Equal(t, resp.MaxVersion(), resp.GetVersion())

	frame := buf.Bytes()
	require.GreaterOrEqual(t, len(frame), 8)
	reader := kbin.Reader{Src: frame[4:]}
	assert.Equal(t, int32(7), reader.Int32())
}

func TestDecodeKafkaRequestAcceptsHighKnownVersion(t *testing.T) {
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(99)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)

	correlationID, decoded, err := decodeKafkaRequest(frame[4:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), correlationID)
	assert.Equal(t, int16(99), decoded.GetVersion())
}

func TestDecodeKafkaRequestAcceptsNegativeMetadataVersion(t *testing.T) {
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(-1)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)

	correlationID, decoded, err := decodeKafkaRequest(frame[4:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), correlationID)
	assert.Equal(t, int16(-1), decoded.GetVersion())
}

func TestDecodeKafkaRequestAcceptsFutureMetadataVersion(t *testing.T) {
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(99)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)

	correlationID, decoded, err := decodeKafkaRequest(frame[4:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), correlationID)
	assert.Equal(t, int16(99), decoded.GetVersion())
}

func TestDecodeKafkaRequestRejectsTruncatedFlexibleRequest(t *testing.T) {
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	require.Greater(t, len(frame), 6)

	_, _, err := decodeKafkaRequest(frame[4 : len(frame)-1])
	require.Error(t, err)
}

func TestWriteKafkaResponse_WritesTaggedHeaderForFlexibleRequest(t *testing.T) {
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(9)
	resp := kmsg.NewPtrMetadataResponse()
	resp.Topics = nil

	var buf bytes.Buffer
	err := writeKafkaResponse(&buf, 7, req, resp)
	require.NoError(t, err)

	frame := buf.Bytes()
	require.GreaterOrEqual(t, len(frame), 9)
	expectedPayloadLen := 4 + 1 + len(resp.AppendTo(nil))
	assert.Equal(t, int32(expectedPayloadLen), int32(len(frame)-4))

	reader := kbin.Reader{Src: frame[4:]}
	assert.Equal(t, int32(7), reader.Int32())
	assert.Equal(t, int8(0), reader.Int8(), "flexible response header must include empty tagged-fields byte")
}

func TestWriteKafkaResponse_OmitsTaggedHeaderForNonFlexibleRequest(t *testing.T) {
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(0)
	resp := kmsg.NewPtrMetadataResponse()
	resp.Topics = nil

	var buf bytes.Buffer
	err := writeKafkaResponse(&buf, 7, req, resp)
	require.NoError(t, err)

	frame := buf.Bytes()
	expectedPayloadLen := 4 + len(resp.AppendTo(nil))
	assert.Equal(t, int32(expectedPayloadLen), int32(len(frame)-4))
}

func TestKafkaHandleConn_ClosesOnInvalidFrameLength(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	_, err := clientConn.Write([]byte{0, 0, 0, 0})
	require.NoError(t, err)

	buf := make([]byte, 1)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = clientConn.Read(buf)
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after invalid frame length")
	}
}

func TestKafkaHandleConn_ClosesOnOversizedFrameLength(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	tooLarge := maxKafkaRequestSize + 1
	frame := []byte{
		byte(tooLarge >> 24),
		byte(tooLarge >> 16),
		byte(tooLarge >> 8),
		byte(tooLarge),
	}
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
		t.Fatal("HandleConn did not exit after oversized frame length")
	}
}

func TestKafkaHandleConn_RejectsTruncatedFlexibleRequest(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	truncated := frame[:len(frame)-1]

	_, err := clientConn.Write(truncated)
	require.NoError(t, err)
	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after truncated flexible request")
	}
}

func TestKafkaHandleConn_ServesMultipleRequestsOnSameConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	req1 := kmsg.NewPtrApiVersionsRequest()
	req1.SetVersion(0)
	frame1 := kmsg.NewRequestFormatter().AppendRequest(nil, req1, 1)
	_, err := clientConn.Write(frame1)
	require.NoError(t, err)

	readKafkaFrame := func(t *testing.T, conn net.Conn) []byte {
		t.Helper()
		lenBuf := make([]byte, 4)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := io.ReadFull(conn, lenBuf)
		require.NoError(t, err)
		length := int(lenBuf[0])<<24 | int(lenBuf[1])<<16 | int(lenBuf[2])<<8 | int(lenBuf[3])
		body := make([]byte, length)
		_, err = io.ReadFull(conn, body)
		require.NoError(t, err)
		return body
	}

	body1 := readKafkaFrame(t, clientConn)
	reader1 := kbin.Reader{Src: body1}
	assert.Equal(t, int32(1), reader1.Int32())

	req2 := kmsg.NewPtrApiVersionsRequest()
	req2.SetVersion(0)
	frame2 := kmsg.NewRequestFormatter().AppendRequest(nil, req2, 2)
	_, err = clientConn.Write(frame2)
	require.NoError(t, err)

	body2 := readKafkaFrame(t, clientConn)
	reader2 := kbin.Reader{Src: body2}
	assert.Equal(t, int32(2), reader2.Int32())

	select {
	case <-done:
		t.Fatal("HandleConn exited after serving valid reused-connection requests")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestKafkaHandleConn_ClosesOnMalformedSecondRequestOnReusedConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(0)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	_, err := clientConn.Write(frame)
	require.NoError(t, err)

	respLenBuf := make([]byte, 4)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(clientConn, respLenBuf)
	require.NoError(t, err)
	respLen := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])
	respBody := make([]byte, respLen)
	_, err = io.ReadFull(clientConn, respBody)
	require.NoError(t, err)

	_, err = clientConn.Write([]byte{0, 0, 0, 0})
	require.NoError(t, err)

	buf := make([]byte, 1)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = clientConn.Read(buf)
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after malformed second request")
	}
}

func TestKafkaHandleConn_RespondsToHighKnownVersion(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(99)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	_, err := clientConn.Write(frame)
	require.NoError(t, err)

	respLenBuf := make([]byte, 4)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(clientConn, respLenBuf)
	require.NoError(t, err)
	respLen := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])
	assert.Greater(t, respLen, uint32(0))

	select {
	case <-done:
		t.Fatal("HandleConn exited after high known version request")
	case <-time.After(100 * time.Millisecond):
	}

	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after client close")
	}
}

func TestKafkaHandleConn_RespondsToNegativeMetadataVersion(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(-1)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	_, err := clientConn.Write(frame)
	require.NoError(t, err)

	respLenBuf := make([]byte, 4)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(clientConn, respLenBuf)
	require.NoError(t, err)
	respLen := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])
	assert.Greater(t, respLen, uint32(0))

	select {
	case <-done:
		t.Fatal("HandleConn exited after negative metadata version request")
	case <-time.After(100 * time.Millisecond):
	}

	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after client close")
	}
}

func TestKafkaHandleConn_RespondsToFutureMetadataVersion(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ks := NewKafkaServer(&KafkaServerCfg{})
	done := make(chan struct{})
	go func() {
		ks.HandleConn(serverConn, nil)
		close(done)
	}()

	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(99)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	_, err := clientConn.Write(frame)
	require.NoError(t, err)

	respLenBuf := make([]byte, 4)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(clientConn, respLenBuf)
	require.NoError(t, err)
	respLen := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])
	assert.Greater(t, respLen, uint32(0))

	select {
	case <-done:
		t.Fatal("HandleConn exited after future metadata version request")
	case <-time.After(100 * time.Millisecond):
	}

	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after client close")
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
