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
	offsets := newOffsetTracker()

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
			// Offset-contiguity is checked on every record regardless of which
			// producer wrote it: a partition's offsets are dense by assignment
			// (0,1,2,...), so any missing offset means a record never landed in
			// the log. This is immune to producer interleaving and client
			// restarts, unlike per-run sequence validation.
			if missing := offsets.check(record.Partition, record.Offset); missing > 0 {
				stats.recordError(topic, int(record.Partition), "offset")
				slog.Warn("offset_gap", "topic", topic, "partition", record.Partition, "offset", record.Offset, "missing", missing)
			}
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

// consumerFetchMaxBytes bounds how much data the client may hold in flight
// per broker. Large windows let the consumer prefetch far ahead of processing
// during a backlog catch-up, which balloons memory on small client droplets;
// small windows trade a little throughput for a hard ceiling on buffering.
const (
	consumerFetchMaxBytes     = 8 << 20
	consumerFetchMaxPartBytes = 2 << 20
)

// offsetTracker verifies that a partition's Kafka offsets are dense: records
// must arrive in strictly increasing, gap-free order (0,1,2,...). Because a
// partition's offset space is shared by all producers and assigned in append
// order, this check is valid even when multiple clients write the same
// partition and regardless of producer restarts. A returned value > 0 is the
// number of records that are missing from the log.
type offsetTracker struct {
	next map[int32]int64
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{next: make(map[int32]int64)}
}

func (t *offsetTracker) check(partition int32, offset int64) int64 {
	next, ok := t.next[partition]
	if !ok {
		// First record of the partition establishes the baseline at the log
		// start. The log may legitimately begin after offset 0 (segment
		// trimming, compaction), so the first record itself is never a gap;
		// contiguity is verified from the log start forward.
		t.next[partition] = offset + 1
		return 0
	}
	if offset < next {
		// Out-of-order delivery within a partition should not happen; guard
		// against negative gaps and re-baseline on the record actually present.
		t.next[partition] = offset + 1
		return 0
	}
	t.next[partition] = offset + 1
	if offset > next {
		return offset - next
	}
	return 0
}

// partitionValidator checks that a single producer run's records arrive on a
// partition in monotonic sequence order: partition p carries seqs p, p+P,
// p+2P, ... This is robust to multiple producers sharing the partition (their
// Kafka offsets interleave, so sequence-contiguity per producer is only valid
// when each partition has a single writer, as here) while still catching
// dropped or reordered records of this run. Unlike offset gaps, sequence gaps
// can be reported as false positives when a producer restarts and renumbers
// from zero, so they are tracked separately from offset gaps.
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
