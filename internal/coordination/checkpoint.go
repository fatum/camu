package coordination

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/maksim/camu/internal/storage"
)

const checkpointKey = "_coordination/checkpoint.json"

// StorageClient is the minimal interface required by the checkpoint functions.
type StorageClient interface {
	Put(ctx context.Context, key string, data []byte, opts storage.PutOpts) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// checkpointDoc is the JSON envelope for a serialized ControllerState.
type checkpointDoc struct {
	Version uint64                        `json:"version"`
	Topics  map[string]map[string]*PartitionMeta `json:"topics"`
}

// MarshalCheckpoint serializes cs to JSON. Partition IDs (int) are stored as
// string keys because JSON only supports string object keys.
func MarshalCheckpoint(cs *ControllerState) ([]byte, error) {
	all := cs.AllPartitions()
	doc := checkpointDoc{
		Version: cs.Version(),
		Topics:  make(map[string]map[string]*PartitionMeta, len(all)),
	}
	for topic, pids := range all {
		doc.Topics[topic] = make(map[string]*PartitionMeta, len(pids))
		for pid, meta := range pids {
			doc.Topics[topic][strconv.Itoa(pid)] = meta
		}
	}
	return json.Marshal(doc)
}

// UnmarshalCheckpoint deserializes JSON produced by MarshalCheckpoint and
// populates cs via SetPartition. String partition keys are converted back to
// ints. Existing state in cs is not cleared — callers should pass a fresh
// ControllerState when a clean load is needed.
func UnmarshalCheckpoint(data []byte, cs *ControllerState) error {
	var doc checkpointDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("unmarshal checkpoint: %w", err)
	}
	for topic, pids := range doc.Topics {
		for pidStr, meta := range pids {
			pid, err := strconv.Atoi(pidStr)
			if err != nil {
				return fmt.Errorf("unmarshal checkpoint: invalid partition id %q for topic %q: %w",
					pidStr, topic, err)
			}
			cs.SetPartition(topic, pid, meta)
		}
	}
	return nil
}

// WriteCheckpoint marshals cs and writes it to _coordination/checkpoint.json
// in the given storage backend.
func WriteCheckpoint(ctx context.Context, s3 StorageClient, cs *ControllerState) error {
	data, err := MarshalCheckpoint(cs)
	if err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := s3.Put(ctx, checkpointKey, data, storage.PutOpts{ContentType: "application/json"}); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}

// ReadCheckpoint fetches _coordination/checkpoint.json and unmarshals it into
// cs. Returns storage.ErrNotFound (from the underlying client) if no checkpoint
// exists yet — callers should handle that case gracefully.
func ReadCheckpoint(ctx context.Context, s3 StorageClient, cs *ControllerState) error {
	data, err := s3.Get(ctx, checkpointKey)
	if err != nil {
		return fmt.Errorf("read checkpoint: %w", err)
	}
	if err := UnmarshalCheckpoint(data, cs); err != nil {
		return fmt.Errorf("read checkpoint: %w", err)
	}
	return nil
}
