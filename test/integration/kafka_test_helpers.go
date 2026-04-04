//go:build integration

package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

const kafkaErrorNotCoordinatorForTest int16 = 16
const kafkaErrorNonEmptyGroupForTest int16 = 68
const kafkaErrorGroupIDNotFoundForTest int16 = 69

func newKafkaTestEnv(t *testing.T, topic string) (*camutest.Env, *camutest.Client, *kgo.Client) {
	t.Helper()

	kafkaPort := freeTCPPort(t)
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Server.KafkaPort = kafkaPort
		}),
	)

	httpClient := env.Client()
	if err := httpClient.CreateTopic(topic, 1, 24*time.Hour); err != nil {
		env.Cleanup()
		t.Fatalf("CreateTopic() error: %v", err)
	}

	seedBroker := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seedBroker),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableIdempotentWrite(),
		kgo.DisableFetchSessions(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(0)},
		}),
	)
	if err != nil {
		env.Cleanup()
		t.Fatalf("kgo.NewClient() error: %v", err)
	}

	return env, httpClient, client
}

func collectKafkaValues(t *testing.T, ctx context.Context, client *kgo.Client, want int) [][]byte {
	t.Helper()

	var got [][]byte
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(got) < want {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			t.Fatalf("PollFetches() error: %v", err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() && len(got) < want {
			record := iter.Next()
			got = append(got, append([]byte(nil), record.Value...))
		}
	}
	return got
}

func waitForPartitionProduceReady(t *testing.T, client *camutest.Client, topic string, partition int) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.ProduceToPartition(topic, partition, []camutest.ProduceMessage{{Value: "warmup"}}); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("partition %s/%d did not become writable before timeout", topic, partition)
}

func waitForLeaderAmong(t *testing.T, env *camutest.Env, topic, partition string, excluded map[int]bool, indexes []int) (int, *camutest.Client) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, idx := range indexes {
			if excluded[idx] {
				continue
			}
			client := env.ClientFor(idx)
			routing, err := client.GetRouting(topic)
			if err != nil {
				continue
			}
			info, ok := routing.Partitions[partition]
			if !ok || info.InstanceID == "" {
				continue
			}
			candidate := env.InstanceIndex(info.InstanceID)
			if candidate >= 0 && !excluded[candidate] {
				return candidate, env.ClientFor(candidate)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("leader for topic %q not found before timeout", topic)
	return -1, nil
}

func waitForKafkaLeaderID(t *testing.T, addr, topic string, wantLeaderID int32) *kmsg.MetadataResponse {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		req := kmsg.NewPtrMetadataRequest()
		req.SetVersion(1)
		req.Topics = []kmsg.MetadataRequestTopic{{Topic: strPtr(topic)}}
		respAny, err := sendKafkaRequest(addr, req)
		if err == nil {
			resp := respAny.(*kmsg.MetadataResponse)
			if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 && resp.Topics[0].Partitions[0].Leader == wantLeaderID {
				return resp
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("metadata on %s did not advertise leader %d for topic %s before timeout", addr, wantLeaderID, topic)
	return nil
}

func waitForKafkaProduceSuccess(t *testing.T, addr, topic string, partition int32, value string) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		req := kmsg.NewPtrProduceRequest()
		req.SetVersion(3)
		req.Acks = 1
		req.Topics = []kmsg.ProduceRequestTopic{{
			Topic: topic,
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: partition,
				Records:   kafkaRecordBatchForTest([]testKafkaRecord{{Value: []byte(value)}}),
			}},
		}}
		respAny, err := sendKafkaRequest(addr, req)
		if err == nil {
			resp := respAny.(*kmsg.ProduceResponse)
			if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 && resp.Topics[0].Partitions[0].ErrorCode == 0 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("kafka produce to %s for topic %s did not succeed before timeout", addr, topic)
}

func waitForKafkaFetchSuccess(t *testing.T, addr, topic string, partition int32) *kmsg.FetchResponse {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		req := kmsg.NewPtrFetchRequest()
		req.SetVersion(4)
		req.MinBytes = 1
		req.MaxWaitMillis = 100
		req.Topics = []kmsg.FetchRequestTopic{{
			Topic: topic,
			Partitions: []kmsg.FetchRequestTopicPartition{{
				Partition:         partition,
				FetchOffset:       0,
				PartitionMaxBytes: 4096,
			}},
		}}
		respAny, err := sendKafkaRequest(addr, req)
		if err == nil {
			resp := respAny.(*kmsg.FetchResponse)
			if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
				part := resp.Topics[0].Partitions[0]
				if part.ErrorCode == 0 && len(part.RecordBatches) > 0 {
					return resp
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("kafka fetch from %s for topic %s did not succeed before timeout", addr, topic)
	return nil
}

func waitForKafkaAddr(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("kafka listener %s did not become reachable before timeout", addr)
}

func produceWithKgoUntilSuccess(ctx context.Context, client *kgo.Client, topic, value string) error {
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		results := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)})
		if err := results.FirstErr(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
		client.ForceMetadataRefresh()
	}
	return lastErr
}

func valuesToStrings(values [][]byte) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = string(value)
	}
	return out
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	defer ln.Close()

	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address %T is not *net.TCPAddr", ln.Addr())
	}
	return tcpAddr.Port
}

func strPtr(s string) *string {
	return &s
}

func kafkaBrokerIDForTest(instanceID string) int32 {
	id := crc32.ChecksumIEEE([]byte(instanceID)) & 0x7fffffff
	if id == 0 {
		return 1
	}
	return int32(id)
}

func sendKafkaRequest(addr string, req kmsg.Request) (kmsg.Response, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}

	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(lenBuf[:]))
	respBuf := make([]byte, length)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return nil, err
	}
	if len(respBuf) < 4 {
		return nil, fmt.Errorf("response too short")
	}
	if got := int32(binary.BigEndian.Uint32(respBuf[:4])); got != 1 {
		return nil, fmt.Errorf("correlation id = %d, want 1", got)
	}

	body := respBuf[4:]
	if req.IsFlexible() && req.Key() != 18 {
		reader := kbin.Reader{Src: body}
		kmsg.SkipTags(&reader)
		if err := reader.Complete(); err != nil && len(reader.Src) == 0 {
			return nil, err
		}
		body = reader.Src
	}

	resp := req.ResponseKind()
	resp.SetVersion(req.GetVersion())
	if err := resp.ReadFrom(body); err != nil {
		return nil, err
	}
	return resp, nil
}

type testKafkaRecord struct {
	Key   []byte
	Value []byte
}

var kafkaCRC32CTableForTest = crc32.MakeTable(crc32.Castagnoli)

func kafkaRecordBatchForTest(records []testKafkaRecord) []byte {
	recordBytes := make([]byte, 0, len(records)*32)
	now := time.Now().UnixMilli()
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

	batch := kmsg.RecordBatch{
		FirstOffset:          0,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		LastOffsetDelta:      int32(len(records) - 1),
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(len(records)),
		Records:              recordBytes,
	}
	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], kafkaCRC32CTableForTest))
	return batch.AppendTo(nil)
}
