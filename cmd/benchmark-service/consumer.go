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
			if value.RunID != cfg.RunID {
				// Records from other runs (other clients, prior processes)
				// share the partition's offset space and are not contiguous in
				// this run's sequence; count them as consumed but do not
				// validate them against this run's sequence.
				stats.recordConsume(topic, int(record.Partition), int64(len(record.Value)))
				return
			}
			if err := validators[record.Partition].validate(cfg, int(record.Partition), value); err != nil {
				stats.recordError(topic, int(record.Partition), "validate")
				slog.Warn("validate_error", "topic", topic, "partition", record.Partition, "offset", record.Offset, "error", err)
				// Validation re-baselines inside the validator, so a single lost
				// record reports one gap instead of poisoning every later record
				// of the partition.
				return
			}
			stats.recordConsume(topic, int(record.Partition), int64(len(record.Value)))
		})
	}
}

// partitionValidator checks that a single producer run's records arrive on a
// partition in monotonic sequence order: partition p carries seqs p, p+P,
// p+2P, ... This is robust to multiple producers sharing the partition (their
// Kafka offsets interleave, so offset-contiguity validation was a false
// positive) while still catching dropped or reordered records of this run.
type partitionValidator struct {
	nextSeq map[int]int64
}

func newPartitionValidator() *partitionValidator {
	return &partitionValidator{nextSeq: make(map[int]int64)}
}

func (v *partitionValidator) validate(cfg serviceConfig, partition int, rec typedRecord) error {
	expected, ok := v.nextSeq[partition]
	if !ok {
		// First record of this run on the partition: accept any starting seq
		// (the consumer may begin mid-stream) and require contiguity after it.
		expected = rec.Seq
	} else if rec.Seq != expected {
		// A gap (e.g. a produce that never landed) must not poison the rest of
		// the partition: count it once and re-baseline to the record actually
		// present so later records validate normally again.
		v.nextSeq[partition] = rec.Seq + int64(cfg.Partitions)
		return fmt.Errorf("seq gap: partition %d got %d, want %d", partition, rec.Seq, expected)
	}
	v.nextSeq[partition] = expected + int64(cfg.Partitions)
	return nil
}
