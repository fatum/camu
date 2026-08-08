package server

import (
	"fmt"
	"testing"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/idempotency"
	"github.com/maksim/camu/internal/producer"
)

func TestMapKafkaError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int16
	}{
		{"unknown topic", errKafkaUnknownTopicPartition, kafkaErrorUnknownTopicPartition},
		{"unknown topic wrapped", fmt.Errorf("%w: topic x", errKafkaUnknownTopicPartition), kafkaErrorUnknownTopicPartition},
		{"leader not available", errKafkaLeaderNotAvailable, kafkaErrorLeaderNotAvailable},
		{"not leader", errKafkaNotLeader, kafkaErrorNotLeader},
		{"invalid request", errKafkaInvalidRequest, kafkaErrorInvalidRequest},
		{"segment not ready", errKafkaSegmentNotReady, kafkaErrorLeaderNotAvailable},
		{"segment not ready wrapped", fmt.Errorf("%w: active segment not initialized", errKafkaSegmentNotReady), kafkaErrorLeaderNotAvailable},
		{"invalid record batch", errKafkaInvalidRecordBatch, kafkaErrorCorruptMessage},
		{"backpressure", producer.ErrBackpressure, kafkaErrorLeaderNotAvailable},
		{"backpressure wrapped", fmt.Errorf("batcher append: %w", producer.ErrBackpressure), kafkaErrorLeaderNotAvailable},
		{"unknown producer", idempotency.ErrUnknownProducer, kafkaErrorUnknownProducerID},
		{"sequence gap", idempotency.ErrSequenceGap, kafkaErrorOutOfOrderSequence},
		{"duplicate sequence", idempotency.ErrDuplicateSequence, kafkaErrorDuplicateSequence},
		{"diskless sequence gap", diskless.ErrSequenceGap, kafkaErrorOutOfOrderSequence},
		{"diskless sequence gap wrapped", fmt.Errorf("diskless commit phase file_key=x topic=t partition=0: %w", diskless.ErrSequenceGap), kafkaErrorOutOfOrderSequence},
		{"diskless out-of-order sequence", diskless.ErrOutOfOrderSequence, kafkaErrorOutOfOrderSequence},
		{"diskless retryable produce", diskless.ErrProduceRetryable, kafkaErrorRequestTimedOut},
		{"diskless retryable produce wrapped", fmt.Errorf("%w: commit uploaded batches t/0: s3 SlowDown", diskless.ErrProduceRetryable), kafkaErrorRequestTimedOut},
		{"unmapped falls to unknown server", fmt.Errorf("some internal failure"), kafkaErrorUnknownServer},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mapKafkaError(tc.err); got != tc.want {
				t.Fatalf("mapKafkaError(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

// TestMapKafkaErrorDisklessRetryableIsRetriableByClients pins the contract that
// a failed-but-unrecorded diskless produce must map to a retriable Kafka error
// (franz-go retries these with the same producer sequence). A non-retriable
// code would make an idempotent client advance past the unrecorded batch and
// permanently gap the partition.
func TestMapKafkaErrorDisklessRetryableIsRetriableByClients(t *testing.T) {
	if got := mapKafkaError(fmt.Errorf("%w: get upload manifest t/0: s3 SlowDown", diskless.ErrProduceRetryable)); got != kafkaErrorRequestTimedOut {
		t.Fatalf("diskless transient failure = %d, want retriable REQUEST_TIMED_OUT (%d)", got, kafkaErrorRequestTimedOut)
	}
}

func TestMapKafkaErrorDoesNotReturnOffsetOutOfRange(t *testing.T) {
	// Regression: internal failures must never surface as Kafka's
	// OFFSET_OUT_OF_RANGE (code 1).
	for _, err := range []error{
		producer.ErrBackpressure,
		errKafkaSegmentNotReady,
		errKafkaInvalidRecordBatch,
		fmt.Errorf("read record batch header: boom"),
	} {
		if got := mapKafkaError(err); got == kafkaErrorOffsetOutOfRange {
			t.Fatalf("mapKafkaError(%v) = %d, must not be OFFSET_OUT_OF_RANGE", err, got)
		}
	}
}
