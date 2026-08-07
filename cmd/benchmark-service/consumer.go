package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

func consumeLoop(ctx context.Context, cfg serviceConfig, topic string, stats *statsAccumulator) {
	slog.Info("consumer_starting", "topic", topic)
	defer slog.Info("consumer_stopped", "topic", topic)

	var kafkaClient *kgo.Client
	if err := retryWithBackoff(ctx, "consumer_kafka_client", 5, func() error {
		var err error
		kafkaClient, err = newKafkaConsumer(cfg, topic)
		return err
	}); err != nil {
		slog.Error("consumer_kafka_client_failed", "topic", topic, "error", err)
		return
	}
	defer kafkaClient.Close()

	expected := make(map[int32]int64)
	validators := make(map[int32]*partitionValidator)
	for p := 0; p < cfg.Partitions; p++ {
		pid := int32(p)
		validators[pid] = newPartitionValidator()
	}

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				stats.recordError(topic, int(e.Partition), "consume")
				slog.Warn("consume_error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
				if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
					return
				}
			}
			continue
		}
		fetches.EachRecord(func(record *kgo.Record) {
			var value typedRecord
			if err := json.Unmarshal(record.Value, &value); err != nil {
				stats.recordError(topic, int(record.Partition), "decode")
				slog.Warn("decode_error", "topic", topic, "partition", record.Partition, "offset", record.Offset, "error", err)
				return
			}
			v := validators[record.Partition]
			if v == nil {
				v = newPartitionValidator()
				validators[record.Partition] = v
			}
			if err := v.validate(cfg, int(record.Partition), record.Offset, value); err != nil {
				stats.recordError(topic, int(record.Partition), "validate")
				slog.Warn("validate_error", "topic", topic, "partition", record.Partition, "offset", record.Offset, "error", err)
				return
			}
			stats.recordConsume(topic, int(record.Partition), int64(len(record.Value)))
			expected[record.Partition] = record.Offset + 1
		})
	}
}

type partitionValidator struct {
	nextSeq map[string]int64
}

func newPartitionValidator() *partitionValidator {
	return &partitionValidator{nextSeq: make(map[string]int64)}
}

func (v *partitionValidator) validate(cfg serviceConfig, partition int, offset int64, rec typedRecord) error {
	if rec.RunID == "" {
		return nil
	}
	expectedOffset, ok := v.nextSeq[rec.RunID]
	if !ok {
		expectedOffset = 0
	}
	if offset != expectedOffset {
		return fmt.Errorf("offset gap: got %d, want %d", offset, expectedOffset)
	}
	v.nextSeq[rec.RunID] = offset + 1
	return nil
}
