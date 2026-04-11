package log

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	CompressionNone   = "none"
	CompressionSnappy = "snappy"
	CompressionZstd   = "zstd"
)

type SegmentMetadata struct {
	BaseOffset     uint64    `json:"base_offset"`
	EndOffset      uint64    `json:"end_offset"`
	MinTimestamp   int64     `json:"min_timestamp,omitempty"`
	MaxTimestamp   int64     `json:"max_timestamp,omitempty"`
	Epoch          uint64    `json:"epoch"`
	SegmentKey     string    `json:"segment_key"`
	OffsetIndexKey string    `json:"offset_index_key"`
	CreatedAt      time.Time `json:"created_at"`
	RecordCount    int       `json:"record_count"`
	SizeBytes      int64     `json:"size_bytes"`
	Compression    string    `json:"compression"`
}

func SegmentOffsetIndexKey(segmentKey string) string {
	if strings.HasSuffix(segmentKey, ".segment") {
		return strings.TrimSuffix(segmentKey, ".segment") + ".offset.idx"
	}
	return segmentKey + ".offset.idx"
}

func SegmentMetadataKey(segmentKey string) string {
	if strings.HasSuffix(segmentKey, ".segment") {
		return strings.TrimSuffix(segmentKey, ".segment") + ".meta.json"
	}
	return segmentKey + ".meta.json"
}

func BuildSegmentMetadata(ref SegmentRef, recordCount int, sizeBytes int64, compression string) ([]byte, error) {
	if compression == "" {
		compression = CompressionNone
	}
	meta := SegmentMetadata{
		BaseOffset:     ref.BaseOffset,
		EndOffset:      ref.EndOffset,
		MinTimestamp:   ref.MinTimestamp,
		MaxTimestamp:   ref.MaxTimestamp,
		Epoch:          ref.Epoch,
		SegmentKey:     ref.Key,
		OffsetIndexKey: ref.OffsetIndexObjectKey(),
		CreatedAt:      ref.CreatedAt.UTC(),
		RecordCount:    recordCount,
		SizeBytes:      sizeBytes,
		Compression:    compression,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal segment metadata: %w", err)
	}
	return data, nil
}

// ReadSegmentBatches reads raw RecordBatch byte slices from segment data.
// Each batch is self-framing via its Length field at offset 8.
// Returns raw batch byte slices that overlap startOffset..startOffset+limit.
func ReadSegmentBatches(data []byte, startOffset uint64, limit int) ([][]byte, error) {
	return ReadSegmentBatchesFromPosition(data, 0, startOffset, limit)
}

// ReadSegmentBatchesFromPosition is like ReadSegmentBatches but begins scanning
// at bytePos instead of the start of data. Use this with a sidecar index to
// skip directly to the approximate position of startOffset.
func ReadSegmentBatchesFromPosition(data []byte, bytePos int, startOffset uint64, limit int) ([][]byte, error) {
	if bytePos < 0 || bytePos > len(data) {
		bytePos = 0
	}
	var batches [][]byte
	pos := bytePos
	for pos < len(data) && (limit <= 0 || len(batches) < limit) {
		if len(data)-pos < RecordBatchHeaderSize {
			break
		}
		h, err := ReadRecordBatchHeader(data[pos:])
		if err != nil {
			break
		}
		batchSize := int(h.RecordBatchSize())
		if batchSize < RecordBatchHeaderSize || pos+batchSize > len(data) {
			break
		}
		if uint64(h.LastOffset()) >= startOffset {
			batches = append(batches, data[pos:pos+batchSize])
		}
		pos += batchSize
	}
	return batches, nil
}

// ReadSegmentBatchesAsMessages decodes bare RecordBatch segment data into
// messages. It is a convenience wrapper around ReadSegmentBatches used by the
// HTTP consume path.
func ReadSegmentBatchesAsMessages(data []byte, startOffset uint64, limit int) ([]Message, error) {
	rawBatches, err := ReadSegmentBatches(data, startOffset, limit)
	if err != nil {
		return nil, err
	}
	var msgs []Message
	for _, raw := range rawBatches {
		decoded, err := DecodeRecordBatch(raw)
		if err != nil {
			return nil, err
		}
		for _, m := range decoded {
			if m.Offset >= startOffset && (limit <= 0 || len(msgs) < limit) {
				msgs = append(msgs, m)
			}
		}
	}
	return msgs, nil
}
