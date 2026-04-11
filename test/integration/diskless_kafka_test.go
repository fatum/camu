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

	"github.com/maksim/camu/pkg/camutest"
)

func TestDiskless_KafkaCompressedProduce(t *testing.T) {
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
			topic := "diskless-kafka-compressed-" + tc.name
			env, httpClient, addr := newDisklessKafkaEnv(t, topic)
			defer env.Cleanup()

			waitForPartitionProduceReady(t, httpClient, topic, 0)

			client, err := kgo.NewClient(
				kgo.SeedBrokers(addr),
				kgo.MaxVersions(kversion.V2_1_0()),
				tc.opt,
				kgo.DisableFetchSessions(),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topic: {0: kgo.NewOffset().At(1)},
				}),
			)
			if err != nil {
				t.Fatalf("kgo.NewClient() error: %v", err)
			}
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

			resp, err := httpClient.Consume(topic, 0, 1, 10)
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

func TestDiskless_KafkaProduceAndFetch(t *testing.T) {
	const topic = "diskless-kafka-e2e"
	env, httpClient, addr := newDisklessKafkaEnv(t, topic)
	defer env.Cleanup()

	waitForPartitionProduceReady(t, httpClient, topic, 0)

	// Warmup message took offset 0. Start consuming from offset 1.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableIdempotentWrite(),
		kgo.DisableFetchSessions(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(1)},
		}),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results := client.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Key: []byte("dk1"), Value: []byte("dv1")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	got := collectKafkaValues(t, ctx, client, 1)
	if len(got) != 1 {
		t.Fatalf("consumed %d records, want 1", len(got))
	}
	if string(got[0]) != "dv1" {
		t.Fatalf("record value = %q, want %q", string(got[0]), "dv1")
	}
}

func TestDiskless_KafkaMetadataIncludesUnknownRequestedTopic(t *testing.T) {
	const topic = "diskless-kafka-metadata"
	env, _, addr := newDisklessKafkaEnv(t, topic)
	defer env.Cleanup()

	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(1)
	req.Topics = []kmsg.MetadataRequestTopic{
		{Topic: strPtr(topic)},
		{Topic: strPtr("missing-diskless-topic")},
	}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("Metadata Request() error: %v", err)
	}
	resp := respAny.(*kmsg.MetadataResponse)
	if len(resp.Topics) != 2 {
		t.Fatalf("metadata topics = %d, want 2", len(resp.Topics))
	}

	foundExisting := false
	foundMissing := false
	for _, topicResp := range resp.Topics {
		if topicResp.Topic == nil {
			continue
		}
		switch *topicResp.Topic {
		case topic:
			foundExisting = true
			if topicResp.ErrorCode != 0 {
				t.Fatalf("existing topic error code = %d, want 0", topicResp.ErrorCode)
			}
			if len(topicResp.Partitions) != 1 {
				t.Fatalf("existing topic partitions = %d, want 1", len(topicResp.Partitions))
			}
		case "missing-diskless-topic":
			foundMissing = true
			if topicResp.ErrorCode != 3 {
				t.Fatalf("missing topic error code = %d, want 3", topicResp.ErrorCode)
			}
		}
	}
	if !foundExisting || !foundMissing {
		t.Fatalf("metadata response missing expected topics: %+v", resp.Topics)
	}
}

func TestDiskless_KafkaListOffsetsEarliestLatestAndTimestamp(t *testing.T) {
	const topic = "diskless-kafka-list-offsets"
	env, httpClient, addr := newDisklessKafkaEnv(t, topic)
	defer env.Cleanup()

	waitForPartitionProduceReady(t, httpClient, topic, 0)

	if _, err := httpClient.ProduceToPartition(topic, 0, []camutest.ProduceMessage{{Value: "after-warmup"}}); err != nil {
		t.Fatalf("ProduceToPartition: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	cases := []struct {
		name          string
		timestamp     int64
		wantErrorCode int16
		wantOffset    int64
	}{
		{name: "earliest", timestamp: -2, wantErrorCode: 0, wantOffset: 0},
		{name: "latest", timestamp: -1, wantErrorCode: 0, wantOffset: 2},
		{name: "max_timestamp", timestamp: -4, wantErrorCode: 0, wantOffset: 2},
		{name: "timestamp_lookup_unsupported", timestamp: 1234, wantErrorCode: 42, wantOffset: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := kmsg.NewPtrListOffsetsRequest()
			req.SetVersion(1)
			req.Topics = []kmsg.ListOffsetsRequestTopic{{
				Topic: topic,
				Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
					Partition: 0,
					Timestamp: tc.timestamp,
				}},
			}}
			respAny, err := sendKafkaRequest(addr, req)
			if err != nil {
				t.Fatalf("ListOffsets Request() error: %v", err)
			}
			resp := respAny.(*kmsg.ListOffsetsResponse)
			part := resp.Topics[0].Partitions[0]
			if part.ErrorCode != tc.wantErrorCode {
				t.Fatalf("error code = %d, want %d", part.ErrorCode, tc.wantErrorCode)
			}
			if tc.wantErrorCode == 0 && part.Offset != tc.wantOffset {
				t.Fatalf("offset = %d, want %d", part.Offset, tc.wantOffset)
			}
		})
	}
}

func TestDiskless_KafkaListOffsetsVersionCompatibility(t *testing.T) {
	const topic = "diskless-kafka-list-offsets-versions"
	env, httpClient, addr := newDisklessKafkaEnv(t, topic)
	defer env.Cleanup()

	waitForPartitionProduceReady(t, httpClient, topic, 0)
	if _, err := httpClient.ProduceToPartition(topic, 0, []camutest.ProduceMessage{{Value: "after-warmup"}}); err != nil {
		t.Fatalf("ProduceToPartition: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	t.Run("version0_earliest_uses_old_style_offsets", func(t *testing.T) {
		req := kmsg.NewPtrListOffsetsRequest()
		req.SetVersion(0)
		req.Topics = []kmsg.ListOffsetsRequestTopic{{
			Topic: topic,
			Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
				Partition: 0,
				Timestamp: -2,
			}},
		}}
		respAny, err := sendKafkaRequest(addr, req)
		if err != nil {
			t.Fatalf("ListOffsets v0 Request() error: %v", err)
		}
		resp := respAny.(*kmsg.ListOffsetsResponse)
		part := resp.Topics[0].Partitions[0]
		if part.ErrorCode != 0 {
			t.Fatalf("ListOffsets v0 earliest error code = %d, want 0", part.ErrorCode)
		}
		if part.Offset != -1 {
			t.Fatalf("ListOffsets v0 earliest offset field = %d, want legacy sentinel -1", part.Offset)
		}
		if len(part.OldStyleOffsets) != 1 || part.OldStyleOffsets[0] != 0 {
			t.Fatalf("ListOffsets v0 old style offsets = %+v, want [0]", part.OldStyleOffsets)
		}
	})

	t.Run("unsupported_timestamp_is_invalid_request_across_versions", func(t *testing.T) {
		for _, version := range []int16{0, 1, 4} {
			t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
				req := kmsg.NewPtrListOffsetsRequest()
				req.SetVersion(version)
				req.Topics = []kmsg.ListOffsetsRequestTopic{{
					Topic: topic,
					Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
						Partition: 0,
						Timestamp: 1234,
					}},
				}}
				respAny, err := sendKafkaRequest(addr, req)
				if err != nil {
					t.Fatalf("ListOffsets v%d Request() error: %v", version, err)
				}
				resp := respAny.(*kmsg.ListOffsetsResponse)
				part := resp.Topics[0].Partitions[0]
				if part.ErrorCode != 42 {
					t.Fatalf("ListOffsets v%d error code = %d, want 42", version, part.ErrorCode)
				}
				if part.Offset != -1 {
					t.Fatalf("ListOffsets v%d offset = %d, want -1 on error", version, part.Offset)
				}
				if version == 0 && len(part.OldStyleOffsets) != 0 {
					t.Fatalf("ListOffsets v0 old style offsets on error = %+v, want empty", part.OldStyleOffsets)
				}
			})
		}
	})
}

func TestDiskless_KafkaFetchEmptyReportsWatermarks(t *testing.T) {
	const topic = "diskless-kafka-fetch-empty"
	env, httpClient, addr := newDisklessKafkaEnv(t, topic)
	defer env.Cleanup()

	waitForPartitionProduceReady(t, httpClient, topic, 0)
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(4)
	req.MinBytes = 1
	req.MaxWaitMillis = 100
	req.Topics = []kmsg.FetchRequestTopic{{
		Topic: topic,
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition:         0,
			FetchOffset:       1,
			PartitionMaxBytes: 4096,
		}},
	}}
	respAny, err := sendKafkaRequest(addr, req)
	if err != nil {
		t.Fatalf("Fetch Request() error: %v", err)
	}
	resp := respAny.(*kmsg.FetchResponse)
	part := resp.Topics[0].Partitions[0]
	if part.ErrorCode != 0 {
		t.Fatalf("Fetch error code = %d, want 0", part.ErrorCode)
	}
	if part.HighWatermark != 1 {
		t.Fatalf("Fetch high watermark = %d, want 1", part.HighWatermark)
	}
	if part.LastStableOffset != 1 {
		t.Fatalf("Fetch last stable offset = %d, want 1", part.LastStableOffset)
	}
	if len(part.RecordBatches) != 0 {
		t.Fatalf("Fetch record batches = %d, want 0", len(part.RecordBatches))
	}
}
