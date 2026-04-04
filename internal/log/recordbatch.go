package log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// RecordBatchHeaderSize is the fixed size in bytes of the Kafka v2 RecordBatch
// header, covering all fields from FirstOffset through NumRecords.
const RecordBatchHeaderSize = 61

// castagnoliTable is the CRC32 table for the Castagnoli polynomial used by
// Kafka to protect the RecordBatch payload (bytes [21..end]).
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// RecordBatchHeader holds the fixed-offset fields of a Kafka v2 RecordBatch.
//
// Wire layout (all big-endian):
//
//	[0:8]   FirstOffset            int64
//	[8:12]  Length                 int32   (covers [12..end])
//	[12:16] PartitionLeaderEpoch   int32
//	[16]    Magic                  byte    = 2
//	[17:21] CRC                    uint32  (CRC32-C over [21..end])
//	[21:23] Attributes             int16
//	[23:27] LastOffsetDelta        int32
//	[27:35] FirstTimestamp         int64
//	[35:43] MaxTimestamp           int64
//	[43:51] ProducerID             int64
//	[51:53] ProducerEpoch          int16
//	[53:57] FirstSequence          int32
//	[57:61] NumRecords             int32
type RecordBatchHeader struct {
	FirstOffset          int64
	Length               int32
	PartitionLeaderEpoch int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	FirstSequence        int32
	NumRecords           int32
}

// RecordBatchSize returns the total byte length of the batch on the wire,
// including the 8-byte BaseOffset + 4-byte Length prefix (i.e. Length + 12).
func (h RecordBatchHeader) RecordBatchSize() int32 {
	return h.Length + 12
}

// LastOffset returns the absolute offset of the last record in the batch.
func (h RecordBatchHeader) LastOffset() int64 {
	return h.FirstOffset + int64(h.LastOffsetDelta)
}

// ReadRecordBatchHeader parses the 61-byte fixed header from buf.
// Returns an error if buf is shorter than RecordBatchHeaderSize.
func ReadRecordBatchHeader(buf []byte) (RecordBatchHeader, error) {
	if len(buf) < RecordBatchHeaderSize {
		return RecordBatchHeader{}, fmt.Errorf(
			"recordbatch: buffer too short: need %d bytes, got %d",
			RecordBatchHeaderSize, len(buf),
		)
	}

	return RecordBatchHeader{
		FirstOffset:          int64(binary.BigEndian.Uint64(buf[0:8])),
		Length:               int32(binary.BigEndian.Uint32(buf[8:12])),
		PartitionLeaderEpoch: int32(binary.BigEndian.Uint32(buf[12:16])),
		Attributes:           int16(binary.BigEndian.Uint16(buf[21:23])),
		LastOffsetDelta:      int32(binary.BigEndian.Uint32(buf[23:27])),
		FirstTimestamp:       int64(binary.BigEndian.Uint64(buf[27:35])),
		MaxTimestamp:         int64(binary.BigEndian.Uint64(buf[35:43])),
		ProducerID:           int64(binary.BigEndian.Uint64(buf[43:51])),
		ProducerEpoch:        int16(binary.BigEndian.Uint16(buf[51:53])),
		FirstSequence:        int32(binary.BigEndian.Uint32(buf[53:57])),
		NumRecords:           int32(binary.BigEndian.Uint32(buf[57:61])),
	}, nil
}

// PatchRecordBatchFirstOffset overwrites bytes [0:8] with firstOffset.
// This field lies outside the CRC-protected region ([21..end]), so the
// stored CRC remains valid after this call.
func PatchRecordBatchFirstOffset(batch []byte, firstOffset int64) error {
	if len(batch) < 8 {
		return fmt.Errorf("recordbatch: buffer too short to patch FirstOffset: need 8, got %d", len(batch))
	}
	binary.BigEndian.PutUint64(batch[0:8], uint64(firstOffset))
	return nil
}

// PatchRecordBatchLeaderEpoch overwrites bytes [12:16] with leaderEpoch.
// This field lies outside the CRC-protected region ([21..end]), so the
// stored CRC remains valid after this call.
func PatchRecordBatchLeaderEpoch(batch []byte, leaderEpoch int32) error {
	if len(batch) < 16 {
		return fmt.Errorf("recordbatch: buffer too short to patch LeaderEpoch: need 16, got %d", len(batch))
	}
	binary.BigEndian.PutUint32(batch[12:16], uint32(leaderEpoch))
	return nil
}

// ValidateRecordBatchCRC verifies that the CRC32-C stored in bytes [17:21]
// matches the checksum of bytes [21..end].
// Returns an error if buf is too short or the checksum does not match.
func ValidateRecordBatchCRC(batch []byte) error {
	// We need at least bytes [17:21] for the stored CRC and byte 21 to start
	// the protected region; 22 bytes is the minimum meaningful check, but
	// require 21 so that an empty protected region is still accepted.
	if len(batch) < 21 {
		return fmt.Errorf(
			"recordbatch: buffer too short for CRC validation: need at least 21 bytes, got %d",
			len(batch),
		)
	}

	stored := binary.BigEndian.Uint32(batch[17:21])
	computed := crc32.Checksum(batch[21:], castagnoliTable)
	if stored != computed {
		return fmt.Errorf(
			"recordbatch: CRC mismatch: stored 0x%08x, computed 0x%08x",
			stored, computed,
		)
	}
	return nil
}
