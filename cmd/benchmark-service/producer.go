package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const batchSize = 5000

type typedRecord struct {
	RunID   string `json:"run_id"`
	Seq     int64  `json:"seq"`
	Payload string `json:"payload"`
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("json marshal: %v", err))
	}
	return b
}

func newKafkaProducer(cfg serviceConfig) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.RecordDeliveryTimeout(5 * time.Minute),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
}

func produceLoop(ctx context.Context, cfg serviceConfig, topic string, stats *statsAccumulator) {
	slog.Info("producer_starting", "topic", topic, "batch", batchSize)
	defer slog.Info("producer_stopped", "topic", topic)

	var kafkaClient *kgo.Client
	if err := retryWithBackoff(ctx, "producer_kafka_client", 5, func() error {
		var err error
		kafkaClient, err = newKafkaProducer(cfg)
		return err
	}); err != nil {
		slog.Error("producer_kafka_client_failed", "topic", topic, "error", err)
		return
	}
	defer kafkaClient.Close()

	payloadText := strings.Repeat("x", int(cfg.MessageBytes))
	lastLog := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		records := make([]*kgo.Record, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			seq := atomic.AddInt64(&stats.produceSeq, 1) - 1
			partition := int32(seq % int64(cfg.Partitions))
			value := typedRecord{
				RunID:   cfg.RunID,
				Seq:     seq,
				Payload: payloadText,
			}
			valueBytes := mustJSON(value)
			key := cfg.RunID + ":" + strconv.FormatInt(seq, 10)
			records = append(records, &kgo.Record{
				Topic:     topic,
				Partition: partition,
				Key:       []byte(key),
				Value:     valueBytes,
			})
		}

		started := time.Now()
		results := kafkaClient.ProduceSync(ctx, records...)
		latency := time.Since(started)
		for _, r := range results {
			if r.Err != nil {
				stats.recordError(topic, int(r.Record.Partition), "produce")
				slog.Warn("produce_error", "topic", topic, "partition", r.Record.Partition, "error", r.Err)
			} else {
				stats.recordProduce(topic, int(r.Record.Partition), int64(len(r.Record.Value)), latency)
			}
		}

		if time.Since(lastLog) > 10*time.Second {
			total := stats.totalProd.Load()
			slog.Info("produce_progress", "topic", topic, "records", total, "batch_latency_ms", latency.Milliseconds())
			lastLog = time.Now()
		}
	}
}
