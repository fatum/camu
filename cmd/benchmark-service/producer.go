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
		kgo.RecordDeliveryTimeout(2*time.Minute),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
}

func produceLoop(ctx context.Context, cfg serviceConfig, topic string, stats *statsAccumulator) {
	slog.Info("producer_starting", "topic", topic)
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
	interval := time.Second
	if cfg.RateLimit > 0 {
		interval = time.Second / time.Duration(cfg.RateLimit)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		seq := atomic.AddInt64(&stats.produceSeq, 1) - 1
		partition := int(seq % int64(cfg.Partitions))
		value := typedRecord{
			RunID:   cfg.RunID,
			Seq:     seq,
			Payload: payloadText,
		}
		valueBytes := mustJSON(value)
		key := cfg.RunID + ":" + strconv.FormatInt(seq, 10)

		started := time.Now()
		result := kafkaClient.ProduceSync(ctx, &kgo.Record{
			Topic:     topic,
			Partition: int32(partition),
			Key:       []byte(key),
			Value:     valueBytes,
		})
		latency := time.Since(started)
		if result.FirstErr() != nil {
			stats.recordError(topic, partition, "produce")
			slog.Warn("produce_error", "topic", topic, "partition", partition, "seq", seq, "error", result.FirstErr())
			continue
		}
		stats.recordProduce(topic, partition, int64(len(valueBytes)), latency)
	}
}
