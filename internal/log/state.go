package log

import (
	"encoding/json"
	"fmt"
)

// PartitionState is the small per-partition state file (state.json) stored in S3.
// It holds only HW and epoch history — segment discovery uses S3 LIST.
type PartitionState struct {
	HighWatermark uint64       `json:"high_watermark"`
	EpochHistory  []EpochEntry `json:"epoch_history,omitempty"`
}

func (s *PartitionState) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *PartitionState) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

// StateKey returns the S3 key for a partition's state file.
func StateKey(topic string, partition int) string {
	return fmt.Sprintf("%s/%d/state.json", topic, partition)
}
