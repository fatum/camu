package diskless

import (
	"errors"
	"fmt"
)

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
// against its last recorded batch. exactRetry is true when the allocation is an
// exact retry of the last batch (the caller deduplicates and must verify the
// count). Otherwise the allocation must be exactly the next contiguous batch
// (prev.first + prev.count); anything lower, inside, or beyond is rejected.
func checkProducerSequence(producerID int64, allocSequence int64, allocCount int, prevFirstSequence int64, prevCount int) (exactRetry bool, err error) {
	next := prevFirstSequence + int64(prevCount)
	switch {
	case allocSequence == prevFirstSequence:
		return true, nil
	case allocSequence < prevFirstSequence:
		return false, fmt.Errorf("%w: producer %d sent sequence %d below recorded %d", ErrOutOfOrderSequence, producerID, allocSequence, prevFirstSequence)
	case allocSequence > next:
		return false, fmt.Errorf("%w: producer %d sent sequence %d, expected %d", ErrSequenceGap, producerID, allocSequence, next)
	case allocSequence < next:
		// prevFirst < allocSequence < next: inside the recorded batch.
		return false, fmt.Errorf("%w: producer %d sent sequence %d inside batch [%d,%d)", ErrOutOfOrderSequence, producerID, allocSequence, prevFirstSequence, next)
	default:
		// allocSequence == next: the exact next contiguous batch.
		return false, nil
	}
}

func checkInitialProducerSequence(producerID, sequence int64) error {
	if sequence != 0 {
		return fmt.Errorf("%w: producer %d sent initial sequence %d, expected 0", ErrSequenceGap, producerID, sequence)
	}
	return nil
}
