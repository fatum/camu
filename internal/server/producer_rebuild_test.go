package server

import (
	"github.com/maksim/camu/internal/log"
	"testing"
)

func TestRebuildProducerSeqsRestoresLastOffset(t *testing.T) {
	ps := &partitionState{producerSeqs: map[uint64]*producerPartitionState{}}
	ps.rebuildProducerSeqsFromBatches([]log.BatchMeta{{ProducerID: 99, Sequence: 0, MessageCount: 2, LastOffset: 41}})
	state := ps.producerSeqs[99]
	if state == nil || state.NextSeq != 2 || state.LastOffset != 41 {
		t.Fatalf("rebuilt producer state = %+v", state)
	}
}
