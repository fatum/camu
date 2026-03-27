package log

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

const (
	segmentMagic   uint32 = 0x43414D55 // "CAMU"
	segmentVersion byte   = 2

	CompressionNone   = "none"
	CompressionSnappy = "snappy"
	CompressionZstd   = "zstd"

	compressionFlagNone   byte = 0
	compressionFlagSnappy byte = 1
	compressionFlagZstd   byte = 2

	minSegmentRecordBatchTargetSize int64 = 1024
)

var (
	zstdEncoderPool = sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil)
			if err != nil {
				panic(fmt.Sprintf("create zstd encoder: %v", err))
			}
			return enc
		},
	}
	zstdDecoderPool = sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil)
			if err != nil {
				panic(fmt.Sprintf("create zstd decoder: %v", err))
			}
			return dec
		},
	}
)

// WriteSegment writes a segment header followed by independently readable
// message batches. Header format: [4B magic][1B version][1B compression].
// Each batch is:
// [4B stored batch length][4B message count][8B producer id][8B sequence][batch bytes]
// where batch bytes are either the raw concatenated message frames or a
// compressed batch payload, depending on the segment compression flag.
func WriteSegment(w io.Writer, msgs []Message, compression string, batchTargetSize int64) error {
	var compressionFlag byte
	switch compression {
	case CompressionNone, "":
		compressionFlag = compressionFlagNone
	case CompressionSnappy:
		compressionFlag = compressionFlagSnappy
	case CompressionZstd:
		compressionFlag = compressionFlagZstd
	default:
		return fmt.Errorf("compression %q not supported", compression)
	}

	// Write header: [4-byte magic][1-byte version][1-byte compression]
	if err := binary.Write(w, binary.BigEndian, segmentMagic); err != nil {
		return fmt.Errorf("write magic: %w", err)
	}
	if _, err := w.Write([]byte{segmentVersion, compressionFlag}); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	batches, err := buildSegmentBatches(msgs, batchTargetSize)
	if err != nil {
		return err
	}
	for i, batch := range batches {
		storedBatch, err := encodeSegmentFrame(batch.payload, compressionFlag)
		if err != nil {
			return fmt.Errorf("encode batch %d: %w", i, err)
		}

		if err := binary.Write(w, binary.BigEndian, uint32(len(storedBatch))); err != nil {
			return fmt.Errorf("write batch length %d: %w", i, err)
		}
		if err := binary.Write(w, binary.BigEndian, uint32(batch.count)); err != nil {
			return fmt.Errorf("write batch count %d: %w", i, err)
		}
		if err := binary.Write(w, binary.BigEndian, batch.producerID); err != nil {
			return fmt.Errorf("write batch producer id %d: %w", i, err)
		}
		if err := binary.Write(w, binary.BigEndian, batch.sequence); err != nil {
			return fmt.Errorf("write batch sequence %d: %w", i, err)
		}
		if _, err := w.Write(storedBatch); err != nil {
			return fmt.Errorf("write batch %d: %w", i, err)
		}
	}
	return nil
}

type segmentBatch struct {
	payload    []byte
	count      int
	producerID uint64
	sequence   uint64
}

func buildSegmentBatches(msgs []Message, batchTargetSize int64) ([]segmentBatch, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if batchTargetSize <= 0 {
		batchTargetSize = 16 * 1024
	}
	if batchTargetSize < minSegmentRecordBatchTargetSize {
		batchTargetSize = minSegmentRecordBatchTargetSize
	}

	batches := make([]segmentBatch, 0, (len(msgs)+31)/32)
	var payload bytes.Buffer
	count := 0

	flush := func() {
		if count == 0 {
			return
		}
		batches = append(batches, segmentBatch{
			payload: append([]byte(nil), payload.Bytes()...),
			count:   count,
		})
		payload.Reset()
		count = 0
	}

	for i, msg := range msgs {
		var frame bytes.Buffer
		if err := writeMessageFrame(&frame, msg); err != nil {
			return nil, fmt.Errorf("write message frame %d: %w", i, err)
		}
		frameBytes := frame.Bytes()

		if count > 0 && int64(payload.Len()+len(frameBytes)) > batchTargetSize {
			flush()
		}
		if _, err := payload.Write(frameBytes); err != nil {
			return nil, fmt.Errorf("buffer message frame %d: %w", i, err)
		}
		count++
	}
	flush()

	return batches, nil
}

func encodeSegmentFrame(frame []byte, compressionFlag byte) ([]byte, error) {
	switch compressionFlag {
	case compressionFlagNone:
		return frame, nil
	case compressionFlagSnappy:
		return snappy.Encode(nil, frame), nil
	case compressionFlagZstd:
		enc := zstdEncoderPool.Get().(*zstd.Encoder)
		defer zstdEncoderPool.Put(enc)
		return enc.EncodeAll(frame, nil), nil
	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", compressionFlag)
	}
}

func decodeSegmentFrame(frame []byte, compressionFlag byte) ([]byte, error) {
	switch compressionFlag {
	case compressionFlagNone:
		return frame, nil
	case compressionFlagSnappy:
		decoded, err := snappy.Decode(nil, frame)
		if err != nil {
			return nil, fmt.Errorf("snappy decompress: %w", err)
		}
		return decoded, nil
	case compressionFlagZstd:
		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		defer zstdDecoderPool.Put(dec)
		decoded, err := dec.DecodeAll(frame, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", compressionFlag)
	}
}

// writeMessageFrame writes a single message frame:
// [8-byte offset][8-byte timestamp][4-byte key len][key bytes]
// [4-byte value len][value bytes][4-byte headers count]
// [for each header: 4-byte key len, key bytes, 4-byte val len, val bytes]
func writeMessageFrame(w io.Writer, msg Message) error {
	// offset
	if err := binary.Write(w, binary.BigEndian, msg.Offset); err != nil {
		return err
	}
	// timestamp
	if err := binary.Write(w, binary.BigEndian, msg.Timestamp); err != nil {
		return err
	}
	// key
	if err := writeBytes(w, msg.Key); err != nil {
		return err
	}
	// value
	if err := writeBytes(w, msg.Value); err != nil {
		return err
	}
	// headers count
	headerCount := uint32(len(msg.Headers))
	if err := binary.Write(w, binary.BigEndian, headerCount); err != nil {
		return err
	}
	keys := make([]string, 0, len(msg.Headers))
	for k := range msg.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := msg.Headers[k]
		if err := writeBytes(w, []byte(k)); err != nil {
			return err
		}
		if err := writeBytes(w, []byte(v)); err != nil {
			return err
		}
	}
	return nil
}

// writeBytes writes a 4-byte length prefix followed by the byte slice.
// A nil slice is written as length 0.
func writeBytes(w io.Writer, b []byte) error {
	length := uint32(len(b))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}
	if length > 0 {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

// ReadSegment reads all message frames from r after validating the segment header.
func ReadSegment(r io.ReaderAt, size int64) ([]Message, error) {
	return readSegmentFromOffset(r, size, nil, 0, 0, 0)
}

// ReadSegmentFromOffset reads frames from r, skipping messages with offset < startOffset,
// and returning up to limit messages (0 means no limit).
func ReadSegmentFromOffset(r io.ReaderAt, size int64, startOffset uint64, limit int) ([]Message, error) {
	return readSegmentFromOffset(r, size, nil, 0, startOffset, limit)
}

// ReadSegmentFromOffsetWithIndex seeks into the segment using a per-segment
// offset index before scanning batches. baseOffset must match the segment's
// base offset used when the index was built.
func ReadSegmentFromOffsetWithIndex(r io.ReaderAt, size int64, offsetIndex []byte, baseOffset uint64, startOffset uint64, limit int) ([]Message, error) {
	return readSegmentFromOffset(r, size, offsetIndex, baseOffset, startOffset, limit)
}

func readSegmentFromOffset(r io.ReaderAt, size int64, offsetIndex []byte, baseOffset uint64, startOffset uint64, limit int) ([]Message, error) {
	sr := io.NewSectionReader(r, 0, size)

	// Validate magic number
	var magic uint32
	if err := binary.Read(sr, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if magic != segmentMagic {
		return nil, fmt.Errorf("invalid magic number: got 0x%08X, want 0x%08X", magic, segmentMagic)
	}

	// Read version
	var version [1]byte
	if _, err := io.ReadFull(sr, version[:]); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	if version[0] != segmentVersion {
		return nil, fmt.Errorf("unsupported segment version: %d", version[0])
	}

	// Read compression flag
	var compressionFlag [1]byte
	if _, err := io.ReadFull(sr, compressionFlag[:]); err != nil {
		return nil, fmt.Errorf("read compression flag: %w", err)
	}

	startPos := int64(6) // 4B magic + 1B version + 1B compression
	if len(offsetIndex) > 0 && startOffset > baseOffset {
		if pos, ok, err := lookupSegmentOffsetIndex(offsetIndex, baseOffset, startOffset); err != nil {
			return nil, err
		} else if ok {
			startPos = int64(pos)
		}
	}
	sr = io.NewSectionReader(r, startPos, size-startPos)

	var msgs []Message
	for {
		var batchLen uint32
		if err := binary.Read(sr, binary.BigEndian, &batchLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read batch length: %w", err)
		}
		var batchCount uint32
		if err := binary.Read(sr, binary.BigEndian, &batchCount); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read batch count: %w", err)
		}

		// Read producer metadata (informational, not used on the read path yet).
		var producerID, sequence uint64
		_ = producerID
		_ = sequence
		if err := binary.Read(sr, binary.BigEndian, &producerID); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read batch producer id: %w", err)
		}
		if err := binary.Read(sr, binary.BigEndian, &sequence); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read batch sequence: %w", err)
		}

		batch := make([]byte, batchLen)
		if _, err := io.ReadFull(sr, batch); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read batch payload: %w", err)
		}

		payload, err := decodeSegmentFrame(batch, compressionFlag[0])
		if err != nil {
			return nil, fmt.Errorf("decode batch: %w", err)
		}

		pr := bytes.NewReader(payload)
		for i := uint32(0); i < batchCount; i++ {
			msg, err := readMessageFrame(pr)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("read message frame: %w", err)
			}

			if msg.Offset < startOffset {
				continue
			}

			msgs = append(msgs, msg)

			if limit > 0 && len(msgs) >= limit {
				return msgs, nil
			}
		}
	}
	return msgs, nil
}

type segmentOffsetIndexEntry struct {
	RelativeOffset uint32
	Position       uint32
}

type SegmentMetadata struct {
	BaseOffset     uint64    `json:"base_offset"`
	EndOffset      uint64    `json:"end_offset"`
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

func BuildSegmentOffsetIndex(segment []byte, baseOffset uint64, intervalBytes int) ([]byte, error) {
	if intervalBytes <= 0 {
		intervalBytes = 4096
	}
	reader := bytes.NewReader(segment)

	var magic uint32
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if magic != segmentMagic {
		return nil, fmt.Errorf("invalid magic number: got 0x%08X, want 0x%08X", magic, segmentMagic)
	}

	var version [1]byte
	if _, err := io.ReadFull(reader, version[:]); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	if version[0] != segmentVersion {
		return nil, fmt.Errorf("unsupported segment version: %d", version[0])
	}

	var compressionFlag [1]byte
	if _, err := io.ReadFull(reader, compressionFlag[:]); err != nil {
		return nil, fmt.Errorf("read compression flag: %w", err)
	}

	const headerSize = 6
	lastIndexedPos := -intervalBytes
	entries := make([]segmentOffsetIndexEntry, 0, 16)

	for {
		batchStart, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("seek batch start: %w", err)
		}

		var batchLen uint32
		if err := binary.Read(reader, binary.BigEndian, &batchLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read batch length: %w", err)
		}
		var batchCount uint32
		if err := binary.Read(reader, binary.BigEndian, &batchCount); err != nil {
			return nil, fmt.Errorf("read batch count: %w", err)
		}

		// Skip producer metadata (8B producer id + 8B sequence).
		var producerID, sequence uint64
		if err := binary.Read(reader, binary.BigEndian, &producerID); err != nil {
			return nil, fmt.Errorf("read batch producer id: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &sequence); err != nil {
			return nil, fmt.Errorf("read batch sequence: %w", err)
		}

		payload := make([]byte, batchLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("read batch payload: %w", err)
		}
		if batchCount == 0 {
			continue
		}

		// We need the first message offset in each stored batch to build a
		// Kafka-style sparse offset index. With the current batch-compressed
		// format, that requires decoding the batch payload here. This is flush-
		// time work only, so the extra CPU is acceptable on the write path.
		decoded, err := decodeSegmentFrame(payload, compressionFlag[0])
		if err != nil {
			return nil, fmt.Errorf("decode batch: %w", err)
		}
		firstMsg, err := readMessageFrame(bytes.NewReader(decoded))
		if err != nil {
			return nil, fmt.Errorf("read first message in batch: %w", err)
		}

		if batchStart < headerSize {
			return nil, fmt.Errorf("invalid batch start %d", batchStart)
		}
		if batchStart > math.MaxUint32 {
			return nil, fmt.Errorf("batch position %d exceeds uint32", batchStart)
		}
		if firstMsg.Offset < baseOffset {
			return nil, fmt.Errorf("batch offset %d before base offset %d", firstMsg.Offset, baseOffset)
		}
		rel := firstMsg.Offset - baseOffset
		if rel > math.MaxUint32 {
			return nil, fmt.Errorf("relative offset %d exceeds uint32", rel)
		}

		if len(entries) == 0 || int(batchStart)-lastIndexedPos >= intervalBytes {
			entries = append(entries, segmentOffsetIndexEntry{
				RelativeOffset: uint32(rel),
				Position:       uint32(batchStart),
			})
			lastIndexedPos = int(batchStart)
		}
	}

	var out bytes.Buffer
	for _, entry := range entries {
		if err := binary.Write(&out, binary.BigEndian, entry.RelativeOffset); err != nil {
			return nil, fmt.Errorf("write relative offset: %w", err)
		}
		if err := binary.Write(&out, binary.BigEndian, entry.Position); err != nil {
			return nil, fmt.Errorf("write position: %w", err)
		}
	}
	return out.Bytes(), nil
}

func lookupSegmentOffsetIndex(index []byte, baseOffset uint64, targetOffset uint64) (uint32, bool, error) {
	if len(index)%8 != 0 {
		return 0, false, fmt.Errorf("invalid segment offset index size %d", len(index))
	}
	if len(index) == 0 || targetOffset < baseOffset {
		return 0, false, nil
	}

	targetRel := targetOffset - baseOffset
	if targetRel > math.MaxUint32 {
		targetRel = math.MaxUint32
	}
	n := len(index) / 8
	pos := sort.Search(n, func(i int) bool {
		rel := binary.BigEndian.Uint32(index[i*8 : i*8+4])
		return rel > uint32(targetRel)
	})
	if pos == 0 {
		return 0, false, nil
	}
	entryPos := (pos - 1) * 8
	return binary.BigEndian.Uint32(index[entryPos+4 : entryPos+8]), true, nil
}

// readMessageFrame reads one message frame from r.
func readMessageFrame(r io.Reader) (Message, error) {
	var msg Message

	// offset
	if err := binary.Read(r, binary.BigEndian, &msg.Offset); err != nil {
		return msg, err
	}
	// timestamp
	if err := binary.Read(r, binary.BigEndian, &msg.Timestamp); err != nil {
		return msg, err
	}
	// key
	key, err := readBytes(r)
	if err != nil {
		return msg, err
	}
	msg.Key = key

	// value
	value, err := readBytes(r)
	if err != nil {
		return msg, err
	}
	msg.Value = value

	// headers count
	var headerCount uint32
	if err := binary.Read(r, binary.BigEndian, &headerCount); err != nil {
		return msg, err
	}
	if headerCount > 0 {
		msg.Headers = make(map[string]string, headerCount)
		for i := uint32(0); i < headerCount; i++ {
			hKey, err := readBytes(r)
			if err != nil {
				return msg, err
			}
			hVal, err := readBytes(r)
			if err != nil {
				return msg, err
			}
			msg.Headers[string(hKey)] = string(hVal)
		}
	}
	return msg, nil
}

func readMessageFrameOffset(r io.Reader) (uint64, error) {
	var offset uint64
	if err := binary.Read(r, binary.BigEndian, &offset); err != nil {
		return 0, err
	}

	var timestamp int64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return 0, err
	}
	if err := skipBytes(r); err != nil {
		return 0, err
	}
	if err := skipBytes(r); err != nil {
		return 0, err
	}

	var headerCount uint32
	if err := binary.Read(r, binary.BigEndian, &headerCount); err != nil {
		return 0, err
	}
	for i := uint32(0); i < headerCount; i++ {
		if err := skipBytes(r); err != nil {
			return 0, err
		}
		if err := skipBytes(r); err != nil {
			return 0, err
		}
	}
	return offset, nil
}

// readBytes reads a 4-byte length-prefixed byte slice.
// Returns nil (not an error) when length is 0.
func readBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func skipBytes(r io.Reader) error {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, r, int64(length))
	return err
}
