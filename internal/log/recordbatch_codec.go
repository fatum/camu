package log

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kbin"
)

const (
	codecNone   = 0
	codecGzip   = 1
	codecSnappy = 2
	codecLZ4    = 3
	codecZstd   = 4

	// decompressLimit is the maximum number of bytes allowed when decompressing
	// a records payload (memory bomb protection).
	decompressLimit = 256 << 20 // 256 MiB
)

// zstdDecoder is a package-level zstd decoder reused across calls.
var zstdDecoder, _ = zstd.NewReader(nil)

// CompressionCodec reads bits 0–2 of the Attributes field at wire offset [21:23]
// and returns the codec identifier: 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd.
// Returns 0 if batch is shorter than 23 bytes.
func CompressionCodec(batch []byte) int {
	if len(batch) < 23 {
		return 0
	}
	attrs := int16(batch[21])<<8 | int16(batch[22])
	return int(attrs & 0x07)
}

// EncodeRecordBatch encodes msgs into an uncompressed Kafka v2 RecordBatch.
// baseOffset is written into the FirstOffset field. If msgs is nil or empty,
// an empty batch (NumRecords=0) is returned with a valid CRC.
func EncodeRecordBatch(baseOffset int64, msgs []Message) []byte {
	return EncodeRecordBatchWithMeta(baseOffset, Batch{Messages: msgs})
}

// EncodeRecordBatchWithMeta encodes a batch into an uncompressed Kafka v2
// RecordBatch including optional producer metadata.
//
// Header fields:
//   - Magic = 2
//   - ProducerID / FirstSequence from batch when ProducerID != 0
//   - ProducerEpoch = 0 for idempotent batches produced through Camu
//   - ProducerID = -1, ProducerEpoch = -1, FirstSequence = -1 otherwise
//   - PartitionLeaderEpoch = -1
//   - Attributes = 0 (no compression)
//
// CRC32-C is computed over bytes [21..end] and stored at [17:21].
func EncodeRecordBatchWithMeta(baseOffset int64, batch Batch) []byte {
	msgs := batch.Messages
	// 1. Encode individual records.
	// The HTTP and Kafka produce paths already have all message sizes available.
	// Reserve the aggregate payload once and reuse one scratch record buffer;
	// allocating a separate growing []byte per record used to dominate produce
	// allocation volume for medium and large batches.
	recordsBuf := make([]byte, 0, estimateRecordBatchPayloadSize(msgs))
	var recordScratch []byte
	var lastOffsetDelta int32
	var firstTimestamp, maxTimestamp int64

	for i, msg := range msgs {
		offsetDelta := int32(i)
		lastOffsetDelta = offsetDelta

		tsDelta := int64(0)
		if i == 0 {
			firstTimestamp = msg.Timestamp
			maxTimestamp = msg.Timestamp
		} else {
			tsDelta = msg.Timestamp - firstTimestamp
			if msg.Timestamp > maxTimestamp {
				maxTimestamp = msg.Timestamp
			}
		}

		recordScratch = appendKafkaRecord(recordScratch[:0], offsetDelta, tsDelta, msg.Key, msg.Value, msg.Headers)
		recordsBuf = kbin.AppendVarint(recordsBuf, int32(len(recordScratch)))
		recordsBuf = append(recordsBuf, recordScratch...)
	}

	numRecords := int32(len(msgs))
	if numRecords == 0 {
		lastOffsetDelta = 0
	}

	// 2. Build the 61-byte header (placeholders for Length and CRC).
	//
	// [0:8]   FirstOffset
	// [8:12]  Length           (filled after we know total size)
	// [12:16] PartitionLeaderEpoch = -1
	// [16]    Magic = 2
	// [17:21] CRC              (filled after header+records are assembled)
	// [21:23] Attributes = 0
	// [23:27] LastOffsetDelta
	// [27:35] FirstTimestamp
	// [35:43] MaxTimestamp
	// [43:51] ProducerID
	// [51:53] ProducerEpoch
	// [53:57] FirstSequence
	// [57:61] NumRecords

	buf := make([]byte, RecordBatchHeaderSize, RecordBatchHeaderSize+len(recordsBuf))

	binary.BigEndian.PutUint64(buf[0:8], uint64(baseOffset))
	// buf[8:12] Length — filled below
	binary.BigEndian.PutUint32(buf[12:16], ^uint32(0)) // PartitionLeaderEpoch = -1
	buf[16] = 2                                        // Magic
	// buf[17:21] CRC — filled below
	binary.BigEndian.PutUint16(buf[21:23], 0) // Attributes
	binary.BigEndian.PutUint32(buf[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint64(buf[27:35], uint64(firstTimestamp))
	binary.BigEndian.PutUint64(buf[35:43], uint64(maxTimestamp))
	if batch.ProducerID != 0 {
		binary.BigEndian.PutUint64(buf[43:51], batch.ProducerID)
		binary.BigEndian.PutUint16(buf[51:53], 0)
		binary.BigEndian.PutUint32(buf[53:57], uint32(batch.Sequence))
	} else {
		binary.BigEndian.PutUint64(buf[43:51], ^uint64(0)) // ProducerID = -1
		binary.BigEndian.PutUint16(buf[51:53], ^uint16(0)) // ProducerEpoch = -1
		binary.BigEndian.PutUint32(buf[53:57], ^uint32(0)) // FirstSequence = -1
	}
	binary.BigEndian.PutUint32(buf[57:61], uint32(numRecords))

	buf = append(buf, recordsBuf...)

	// 3. Fill Length = total_size - 12.
	length := int32(len(buf) - 12)
	binary.BigEndian.PutUint32(buf[8:12], uint32(length))

	// 4. Compute and store CRC32-C over [21..end].
	crc := computeCRC32C(buf[21:])
	binary.BigEndian.PutUint32(buf[17:21], crc)

	return buf
}

// estimateRecordBatchPayloadSize deliberately overestimates the Kafka record
// framing. It avoids repeated aggregate-buffer growth without affecting the
// encoded bytes on the wire.
func estimateRecordBatchPayloadSize(msgs []Message) int {
	size := 0
	for _, msg := range msgs {
		size += len(msg.Key) + len(msg.Value) + 32
		for key, value := range msg.Headers {
			size += len(key) + len(value) + 10
		}
	}
	return size
}

// DecodeRecordBatch decodes a complete Kafka v2 RecordBatch and returns the
// contained messages with absolute Offset and Timestamp values.
//
// Supports compression codecs: none (0), gzip (1), snappy (2), lz4 (3), zstd (4).
func DecodeRecordBatch(batch []byte) ([]Message, error) {
	hdr, err := ReadRecordBatchHeader(batch)
	if err != nil {
		return nil, fmt.Errorf("recordbatch_codec: %w", err)
	}

	if int(hdr.NumRecords) < 0 {
		return nil, fmt.Errorf("recordbatch_codec: negative NumRecords: %d", hdr.NumRecords)
	}

	if hdr.NumRecords == 0 {
		return nil, nil
	}

	rawRecords := batch[RecordBatchHeaderSize:]

	codec := CompressionCodec(batch)
	if codec != codecNone {
		rawRecords, err = decompressRecords(codec, rawRecords)
		if err != nil {
			return nil, fmt.Errorf("recordbatch_codec: decompress: %w", err)
		}
	}

	msgs := make([]Message, 0, hdr.NumRecords)
	r := kbin.Reader{Src: rawRecords}

	for i := int32(0); i < hdr.NumRecords; i++ {
		recLen := r.Varint()
		if r.Complete() != nil {
			return nil, fmt.Errorf("recordbatch_codec: reading record %d length: %w", i, r.Complete())
		}
		recData := r.Span(int(recLen))
		if r.Complete() != nil {
			return nil, fmt.Errorf("recordbatch_codec: reading record %d body: %w", i, r.Complete())
		}

		msg, err := decodeRecord(recData, hdr.FirstOffset, hdr.FirstTimestamp)
		if err != nil {
			return nil, fmt.Errorf("recordbatch_codec: record %d: %w", i, err)
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// appendKafkaRecord encodes a single Kafka record body (without the leading
// varint length prefix). The caller is responsible for prepending the length.
//
// Format:
//
//	[attributes int8][timestampDelta varint64][offsetDelta varint32]
//	[keyLen varint][key bytes][valueLen varint][value bytes]
//	[headerCount varint][headers...]
func appendKafkaRecord(dst []byte, offsetDelta int32, timestampDelta int64, key, value []byte, headers map[string]string) []byte {
	dst = kbin.AppendInt8(dst, 0) // attributes (always 0 at record level)
	dst = kbin.AppendVarlong(dst, timestampDelta)
	dst = kbin.AppendVarint(dst, offsetDelta)
	dst = kbin.AppendVarintBytes(dst, key)
	dst = kbin.AppendVarintBytes(dst, value)

	// Sort header keys for deterministic output.
	if len(headers) == 0 {
		dst = kbin.AppendVarint(dst, 0)
	} else {
		keys := make([]string, 0, len(headers))
		for k := range headers {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		dst = kbin.AppendVarint(dst, int32(len(keys)))
		for _, k := range keys {
			dst = kbin.AppendVarintString(dst, k)
			dst = kbin.AppendVarintBytes(dst, []byte(headers[k]))
		}
	}

	return dst
}

// decodeRecord parses a single record body (without the leading length varint)
// and returns a Message with absolute offset and timestamp.
func decodeRecord(data []byte, firstOffset int64, firstTimestamp int64) (Message, error) {
	r := kbin.Reader{Src: data}

	_ = r.Int8()              // attributes (ignored at record level)
	tsDelta := r.Varlong()    // timestamp delta
	offsetDelta := r.Varint() // offset delta

	key := r.VarintBytes()
	value := r.VarintBytes()
	numHeaders := r.Varint()

	var headers map[string]string
	if numHeaders > 0 {
		headers = make(map[string]string, numHeaders)
		for i := int32(0); i < numHeaders; i++ {
			hk := r.VarintString()
			hv := r.VarintBytes()
			headers[hk] = string(hv)
		}
	}

	if err := r.Complete(); err != nil {
		return Message{}, fmt.Errorf("reading record: %w", err)
	}

	return Message{
		Offset:    uint64(firstOffset + int64(offsetDelta)),
		Timestamp: firstTimestamp + tsDelta,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, nil
}

// decompressRecords decompresses compressed record bytes using the given codec.
// A 256 MiB limit is enforced on the decompressed output to prevent memory bombs.
func decompressRecords(codec int, compressed []byte) ([]byte, error) {
	switch codec {
	case codecGzip:
		r, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		defer func() { _ = r.Close() }()
		return io.ReadAll(io.LimitReader(r, decompressLimit))

	case codecSnappy:
		// Kafka uses the snappy block format, not the framing format.
		return snappy.Decode(nil, compressed)

	case codecLZ4:
		// Kafka uses the LZ4 stream format.
		r := lz4.NewReader(bytes.NewReader(compressed))
		return io.ReadAll(io.LimitReader(r, decompressLimit))

	case codecZstd:
		out, err := zstdDecoder.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		if int64(len(out)) > decompressLimit {
			return nil, fmt.Errorf("zstd: decompressed size %d exceeds limit %d", len(out), decompressLimit)
		}
		return out, nil

	default:
		return nil, fmt.Errorf("unknown compression codec: %d", codec)
	}
}

// computeCRC32C returns the CRC32-C (Castagnoli) checksum of data.
func computeCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}
