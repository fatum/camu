package diskless

import (
	"errors"
	"fmt"
)

// ErrProduceRetryable marks a diskless produce failure where the batch was
// NOT recorded in the metastore (upload/commit aborted by a transient
// object-store error or deadline). Kafka clients must retry such a batch with
// the same producer sequence, so the produce path maps it to a retriable
// Kafka error. Surfacing it as a non-retriable error would make an idempotent
// client advance past the unrecorded batch and create a permanent gap.
var ErrProduceRetryable = errors.New("diskless produce retryable failure")

// Idempotent-produce sequence errors. They mirror the classic path's
// ErrSequenceGap / out-of-order rejection so HTTP produce maps them to 422.
var (
	// ErrOutOfOrderSequence is returned when an idempotent producer sends a
	// first sequence at or below its last recorded batch (a stale or
	// overlapping retry).
	ErrOutOfOrderSequence = errors.New("diskless idempotent produce: out-of-order sequence")
	// ErrSequenceGap is returned when an idempotent producer skips sequences:
	// the sent first sequence is beyond the last recorded batch's end.
	ErrSequenceGap = errors.New("diskless idempotent produce: sequence gap")
)

// checkProducerSequence validates a new allocation for an idempotent producer
// against its last recorded batch. Returns nil if the allocation is an exact
// retry or the next contiguous batch; otherwise returns an error.
func checkProducerSequence(producerID int64, allocSequence int64, _ int, prevFirstSequence int64, prevCount int) error {
	next := prevFirstSequence + int64(prevCount)
	switch {
	case allocSequence == prevFirstSequence:
		return nil
	case allocSequence < prevFirstSequence:
		return fmt.Errorf("%w: producer %d sent sequence %d below recorded %d", ErrOutOfOrderSequence, producerID, allocSequence, prevFirstSequence)
	case allocSequence > next:
		return fmt.Errorf("%w: producer %d sent sequence %d, expected %d", ErrSequenceGap, producerID, allocSequence, next)
	case allocSequence < next:
		return fmt.Errorf("%w: producer %d sent sequence %d inside batch [%d,%d)", ErrOutOfOrderSequence, producerID, allocSequence, prevFirstSequence, next)
	default:
		return nil
	}
}

func checkInitialProducerSequence(producerID, sequence int64) error {
	if sequence != 0 {
		return fmt.Errorf("%w: producer %d sent initial sequence %d, expected 0", ErrSequenceGap, producerID, sequence)
	}
	return nil
}
