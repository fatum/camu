// Package pipeline contains the durable, committed-record sink contract.
//
// A pipeline reads only committed source records, durably writes a sink, and
// advances its checkpoint last.  Parquet, materialized topics, and Iceberg
// can all be implemented as sinks on this contract.
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

var (
	ErrFenced   = errors.New("pipeline fenced")
	ErrConflict = errors.New("pipeline checkpoint conflict")
)

// Fence checks ownership immediately before and after a sink write.
type Fence interface {
	Fenced(context.Context, string, int, uint64) bool
}

type NoFence struct{}

func (NoFence) Fenced(context.Context, string, int, uint64) bool { return false }

// Reader reads committed records from an in-memory log index. highWatermark is
// the next readable offset (Kafka's high watermark), and epoch is the owner
// epoch captured by the caller.
type Reader struct {
	fetcher *consumer.Fetcher
	fence   Fence
}

// CommittedBatchReader is the source-read contract used by pipeline runners.
// Implementations must return only records below highWatermark and the next
// source offset after the returned batch.
type CommittedBatchReader interface {
	Read(context.Context, *log.Index, string, int, uint64, uint64, uint64, int) ([]log.Message, uint64, error)
}

var _ CommittedBatchReader = (*Reader)(nil)

func NewReader(fetcher *consumer.Fetcher, fence Fence) *Reader {
	if fence == nil {
		fence = NoFence{}
	}
	return &Reader{fetcher: fetcher, fence: fence}
}
func (r *Reader) Read(ctx context.Context, index *log.Index, topic string, partition int, start, highWatermark, epoch uint64, limit int) ([]log.Message, uint64, error) {
	if r.fetcher == nil {
		return nil, start, errors.New("pipeline reader: nil fetcher")
	}
	if r.fence.Fenced(ctx, topic, partition, epoch) {
		return nil, start, ErrFenced
	}
	if start > highWatermark {
		return nil, start, fmt.Errorf("pipeline: start offset %d exceeds committed high watermark %d", start, highWatermark)
	}
	if start == highWatermark || limit <= 0 {
		return nil, start, nil
	}
	available := highWatermark - start
	if uint64(limit) > available {
		limit = int(available)
	}
	msgs, next, err := r.fetcher.Fetch(ctx, index, topic, partition, start, limit)
	if err != nil {
		return nil, start, err
	}
	if r.fence.Fenced(ctx, topic, partition, epoch) {
		return nil, start, ErrFenced
	}
	return msgs, next, nil
}

// ValidateCommittedRange verifies that an inclusive source range is readable
// at the supplied high watermark. The high watermark is the next offset that
// may be read, so end is inclusive and must be strictly below highWatermark.
func ValidateCommittedRange(start, end, highWatermark uint64) error {
	if end < start {
		return errors.New("pipeline: invalid committed range")
	}
	if start >= highWatermark || end >= highWatermark {
		return fmt.Errorf("pipeline: range [%d,%d] exceeds committed high watermark %d", start, end, highWatermark)
	}
	return nil
}

// Checkpoint is the durable source position after a sink output is complete.
type Checkpoint struct {
	SourceTopic string `json:"source_topic"`
	Partition   int    `json:"partition"`
	NextOffset  uint64 `json:"next_offset"`
	SourceEpoch uint64 `json:"source_epoch"`
	Sink        string `json:"sink"`
	SinkVersion string `json:"sink_version"`
	OutputStart uint64 `json:"output_start,omitempty"`
	OutputEnd   uint64 `json:"output_end,omitempty"`
	Generation  uint64 `json:"generation"`
}

// SinkResult identifies the durable output range produced for a source batch.
// A sink must return this only after its output is durable.
type SinkResult struct {
	OutputStart uint64
	OutputEnd   uint64
}

type CheckpointStore struct {
	objects *storage.S3Client
	fence   Fence
}

func NewCheckpointStore(objects *storage.S3Client, fence Fence) *CheckpointStore {
	if fence == nil {
		fence = NoFence{}
	}
	return &CheckpointStore{objects: objects, fence: fence}
}

// CheckpointKey returns the durable object key for a pipeline partition.
func CheckpointKey(pipeline, topic string, partition int) string {
	return fmt.Sprintf("_meta/pipelines/%s/%s/%d.json", pipeline, topic, partition)
}
func (s *CheckpointStore) Load(ctx context.Context, pipeline, topic string, partition int) (Checkpoint, error) {
	if err := validateKeyComponent(pipeline, "pipeline"); err != nil {
		return Checkpoint{}, err
	}
	if err := validateKeyComponent(topic, "topic"); err != nil {
		return Checkpoint{}, err
	}
	data, err := s.objects.Get(ctx, CheckpointKey(pipeline, topic, partition))
	if err != nil {
		return Checkpoint{}, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("decode pipeline checkpoint: %w", err)
	}
	if cp.SourceTopic != topic || cp.Partition != partition {
		return Checkpoint{}, fmt.Errorf("pipeline checkpoint identity mismatch")
	}
	return cp, nil
}

// Publish conditionally advances generation. Re-publishing an identical
// checkpoint is idempotent, which covers a crash after sink output.
func (s *CheckpointStore) Publish(ctx context.Context, pipeline string, cp Checkpoint) error {
	if err := validateKeyComponent(pipeline, "pipeline"); err != nil {
		return err
	}
	if err := validateKeyComponent(cp.SourceTopic, "topic"); err != nil {
		return err
	}
	if err := validateKeyComponent(cp.Sink, "sink"); err != nil {
		return err
	}
	if s.fence.Fenced(ctx, cp.SourceTopic, cp.Partition, cp.SourceEpoch) {
		return ErrFenced
	}
	key := CheckpointKey(pipeline, cp.SourceTopic, cp.Partition)
	currentData, etag, err := s.objects.GetWithETag(ctx, key)
	if err == nil {
		var current Checkpoint
		if json.Unmarshal(currentData, &current) != nil {
			return errors.New("decode existing pipeline checkpoint")
		}
		if current.SourceTopic != cp.SourceTopic || current.Partition != cp.Partition || current.Sink != cp.Sink {
			return ErrConflict
		}
		if current == cp {
			return nil
		}
		if cp.Generation != current.Generation+1 {
			return ErrConflict
		}
		if cp.SourceEpoch < current.SourceEpoch {
			return ErrConflict
		}
		if cp.NextOffset < current.NextOffset || cp.OutputStart < current.OutputStart || cp.OutputEnd < current.OutputEnd {
			return ErrConflict
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	} else if cp.Generation != 1 {
		return ErrConflict
	}
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	if _, err = s.objects.ConditionalPut(ctx, key, data, etag); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return ErrConflict
		}
		return err
	}
	if s.fence.Fenced(ctx, cp.SourceTopic, cp.Partition, cp.SourceEpoch) {
		return ErrFenced
	}
	return nil
}

func validateKeyComponent(value, name string) error {
	if strings.TrimSpace(value) == "" || value == "." || value == ".." || strings.ContainsAny(value, "/\\") {
		return fmt.Errorf("invalid pipeline %s", name)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("invalid pipeline %s", name)
		}
	}
	return nil
}

// Batch is a source batch passed to a sink.
type Batch struct {
	SourceTopic string
	Partition   int
	SourceEpoch uint64
	StartOffset uint64
	EndOffset   uint64
	// SinkStartSequence is the contiguous sequence assigned by the sink. It
	// is intentionally independent of source offsets, which may have gaps.
	SinkStartSequence uint64
	Messages          []log.Message
	Error             string // optional decode/error metadata for DLQ rows
	ErrorMetadata     map[string]any
}
type Sink interface {
	Write(context.Context, Batch) (SinkResult, error)
}

// DLQAppender is the minimal server seam required by DLQSink.
type DLQAppender interface {
	Append(context.Context, string, int, uint64, uint64, []log.Message) (lastOffset uint64, duplicate bool, err error)
	// WaitDurable waits for the sink output offset and revalidates ownership of
	// the source partition before returning. The topic and epoch identify the
	// source; the implementation owns the sink destination.
	WaitDurable(context.Context, string, int, uint64, uint64) error
}

type DLQSink struct {
	appender DLQAppender
	topic    string
	source   string
	fence    Fence
}

func NewDLQSink(appender DLQAppender, sourceTopic, dlqTopic string, sourceFence Fence) *DLQSink {
	if sourceFence == nil {
		sourceFence = NoFence{}
	}
	return &DLQSink{appender: appender, source: sourceTopic, topic: dlqTopic, fence: sourceFence}
}
func (s *DLQSink) Write(ctx context.Context, b Batch) (SinkResult, error) {
	if s.appender == nil {
		return SinkResult{}, errors.New("pipeline dlq: nil appender")
	}
	if b.SourceTopic != s.source {
		return SinkResult{}, fmt.Errorf("pipeline dlq: source topic %q does not match configured topic %q", b.SourceTopic, s.source)
	}
	if len(b.Messages) == 0 {
		return SinkResult{}, nil
	}
	if s.fence.Fenced(ctx, s.source, b.Partition, b.SourceEpoch) {
		return SinkResult{}, ErrFenced
	}
	producerID := deterministicProducerID(s.source, b.Partition, s.topic)
	rows := make([]log.Message, 0, len(b.Messages))
	for _, m := range b.Messages {
		row := map[string]any{"source_topic": s.source, "source_partition": b.Partition, "source_offset": m.Offset, "original_key": m.Key, "original_value": m.Value, "headers": m.Headers, "error": b.Error}
		for key, value := range b.ErrorMetadata {
			if key != "source_topic" && key != "source_partition" && key != "source_offset" && key != "original_key" && key != "original_value" && key != "headers" && key != "error" {
				row[key] = value
			}
		}
		payload, err := json.Marshal(row)
		if err != nil {
			return SinkResult{}, err
		}
		rows = append(rows, log.Message{Key: []byte(fmt.Sprintf("%s/%d/%d", s.source, b.Partition, m.Offset)), Value: payload, Timestamp: m.Timestamp})
	}
	last, _, err := s.appender.Append(ctx, s.topic, b.Partition, producerID, b.SinkStartSequence, rows)
	if err != nil {
		return SinkResult{}, err
	}
	if err := s.appender.WaitDurable(ctx, b.SourceTopic, b.Partition, b.SourceEpoch, last); err != nil {
		return SinkResult{}, err
	}
	if s.fence.Fenced(ctx, s.source, b.Partition, b.SourceEpoch) {
		return SinkResult{}, ErrFenced
	}
	return SinkResult{OutputStart: last - uint64(len(rows)) + 1, OutputEnd: last}, nil
}

func deterministicProducerID(source string, partition int, sink string) uint64 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s", source, partition, sink)))
	return (uint64(h[0])<<56 | uint64(h[1])<<48 | uint64(h[2])<<40 | uint64(h[3])<<32 | uint64(h[4])<<24 | uint64(h[5])<<16 | uint64(h[6])<<8 | uint64(h[7])) &^ (uint64(1) << 63)
}
