package server

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/log"
)

// Package-level zstd decoder reused across calls. DecodeAll is safe for
// concurrent use so no synchronization is needed.
var zstdDecoder, _ = zstd.NewReader(nil)

// extractRawRecordBatches slices raw Kafka protocol bytes into individual
// RecordBatch byte slices without decoding individual records.
func extractRawRecordBatches(records []byte) ([][]byte, error) {
	var batches [][]byte
	for len(records) >= log.RecordBatchHeaderSize {
		h, err := log.ReadRecordBatchHeader(records)
		if err != nil {
			return batches, err
		}
		batchSize := int(h.RecordBatchSize())
		if batchSize < log.RecordBatchHeaderSize || batchSize > len(records) {
			return batches, fmt.Errorf("invalid batch size: %d", batchSize)
		}
		batches = append(batches, records[:batchSize])
		records = records[batchSize:]
	}
	return batches, nil
}

func decodeKafkaProduceMessages(data []byte) ([]log.Message, error) {
	batches, err := decodeKafkaProduceBatches(data)
	if err != nil {
		return nil, err
	}
	var msgs []log.Message
	for _, batch := range batches {
		msgs = append(msgs, batch.Messages...)
	}
	return msgs, nil
}

func decodeKafkaProduceBatches(data []byte) ([]log.Batch, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var batches []log.Batch
	for len(data) > 0 {
		if len(data) < 12 {
			return nil, fmt.Errorf("record batch too short")
		}
		batchLen := int(binary.BigEndian.Uint32(data[8:12]))
		totalLen := 12 + batchLen
		if batchLen < 49 || totalLen > len(data) {
			return nil, fmt.Errorf("record batch length invalid")
		}

		var batch kmsg.RecordBatch
		if err := batch.ReadFrom(data[:totalLen]); err != nil {
			return nil, err
		}

		msgs := make([]log.Message, 0, int(batch.NumRecords))
		recordData, err := decodeKafkaBatchRecords(batch.Attributes&0x07, batch.Records)
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(batch.NumRecords); i++ {
			record, n, err := decodeKafkaRecord(recordData)
			if err != nil {
				return nil, err
			}
			recordData = recordData[n:]
			headers := make(map[string]string, len(record.Headers))
			for _, header := range record.Headers {
				headers[header.Key] = string(header.Value)
			}
			timestampDelta := int64(record.TimestampDelta)
			if record.TimestampDelta64 != 0 {
				timestampDelta = record.TimestampDelta64
			}
			msgs = append(msgs, log.Message{
				Timestamp: batch.FirstTimestamp + timestampDelta,
				Key:       append([]byte(nil), record.Key...),
				Value:     append([]byte(nil), record.Value...),
				Headers:   headers,
			})
		}
		producerID := uint64(0)
		if batch.ProducerID >= 0 {
			producerID = uint64(batch.ProducerID)
		}
		sequence := uint64(0)
		if batch.FirstSequence >= 0 {
			sequence = uint64(batch.FirstSequence)
		}
		batches = append(batches, log.Batch{
			ProducerID: producerID,
			Sequence:   sequence,
			Messages:   msgs,
		})
		data = data[totalLen:]
	}

	return batches, nil
}

func decodeKafkaBatchRecords(codec int16, data []byte) ([]byte, error) {
	switch codec {
	case 0:
		return data, nil
	case 1:
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip record batch: %w", err)
		}
		defer func() { _ = reader.Close() }()
		decoded, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("gzip record batch: %w", err)
		}
		return decoded, nil
	case 2:
		decoded, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("snappy record batch: %w", err)
		}
		return decoded, nil
	case 3:
		reader := lz4.NewReader(bytes.NewReader(data))
		decoded, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("lz4 record batch: %w", err)
		}
		return decoded, nil
	case 4:
		decoded, err := zstdDecoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd record batch: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported record batch compression codec: %d", codec)
	}
}

func decodeKafkaRecord(data []byte) (kmsg.Record, int, error) {
	length, n := kbin.Varint(data)
	if n <= 0 {
		return kmsg.Record{}, 0, fmt.Errorf("record length invalid")
	}
	totalLen := n + int(length)
	if int(length) < 0 || totalLen > len(data) {
		return kmsg.Record{}, 0, fmt.Errorf("record body truncated")
	}

	var record kmsg.Record
	if err := record.ReadFrom(data[:totalLen]); err != nil {
		return kmsg.Record{}, 0, err
	}
	return record, totalLen, nil
}

func encodeKafkaRecordBatch(msgs []log.Message) []byte {
	if len(msgs) == 0 {
		return nil
	}

	firstTimestamp := msgs[0].Timestamp
	if firstTimestamp == 0 {
		firstTimestamp = time.Now().UnixMilli()
	}
	maxTimestamp := firstTimestamp

	recordBytes := make([]byte, 0, len(msgs)*32)
	firstOffset := int64(msgs[0].Offset)
	for i, msg := range msgs {
		ts := msg.Timestamp
		if ts == 0 {
			ts = firstTimestamp
		}
		if ts > maxTimestamp {
			maxTimestamp = ts
		}
		recordBytes = appendKafkaRecord(recordBytes, int64(i), ts-firstTimestamp, msg)
	}

	batch := kmsg.RecordBatch{
		FirstOffset:          firstOffset,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		LastOffsetDelta:      int32(len(msgs) - 1),
		FirstTimestamp:       firstTimestamp,
		MaxTimestamp:         maxTimestamp,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(len(msgs)),
		Records:              recordBytes,
	}

	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], kafkaCRC32CTable))
	return batch.AppendTo(nil)
}

func appendKafkaRecord(dst []byte, offsetDelta, timestampDelta int64, msg log.Message) []byte {
	body := make([]byte, 0, len(msg.Key)+len(msg.Value)+16)
	body = kbin.AppendInt8(body, 0)
	body = kbin.AppendVarlong(body, timestampDelta)
	body = kbin.AppendVarint(body, int32(offsetDelta))
	body = kbin.AppendVarintBytes(body, msg.Key)
	body = kbin.AppendVarintBytes(body, msg.Value)
	body = kbin.AppendVarint(body, int32(len(msg.Headers)))
	for key, value := range msg.Headers {
		body = kbin.AppendVarintString(body, key)
		body = kbin.AppendVarintBytes(body, []byte(value))
	}

	dst = kbin.AppendVarint(dst, int32(len(body)))
	dst = append(dst, body...)
	return dst
}
