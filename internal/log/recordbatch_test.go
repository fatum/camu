package log

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// buildTestRecordBatch constructs a valid Kafka v2 RecordBatch byte slice
// with the given parameters and a correct CRC32-C over bytes [21..end].
//
// Layout (61-byte fixed header + optional records payload):
//
//	[0:8]   FirstOffset       int64
//	[8:12]  Length            int32  (covers [12..end], i.e. len(batch)-12)
//	[12:16] PartitionLeaderEpoch int32
//	[16]    Magic             byte   = 2
//	[17:21] CRC               uint32 (CRC32-C over [21..end])
//	[21:23] Attributes        int16
//	[23:27] LastOffsetDelta   int32
//	[27:35] FirstTimestamp    int64
//	[35:43] MaxTimestamp      int64
//	[43:51] ProducerID        int64
//	[51:53] ProducerEpoch     int16
//	[53:57] FirstSequence     int32
//	[57:61] NumRecords        int32
//	[61:]   records payload
func buildTestRecordBatch(
	firstOffset int64,
	partitionLeaderEpoch int32,
	lastOffsetDelta int32,
	firstTimestamp int64,
	maxTimestamp int64,
	producerID int64,
	producerEpoch int16,
	firstSequence int32,
	numRecords int32,
	records []byte,
) []byte {
	totalSize := RecordBatchHeaderSize + len(records)
	buf := make([]byte, totalSize)

	// Length field covers from byte 12 to end, so = totalSize - 12
	length := int32(totalSize - 12)

	binary.BigEndian.PutUint64(buf[0:8], uint64(firstOffset))
	binary.BigEndian.PutUint32(buf[8:12], uint32(length))
	binary.BigEndian.PutUint32(buf[12:16], uint32(partitionLeaderEpoch))
	buf[16] = 2 // magic
	// [17:21] CRC — filled in below
	binary.BigEndian.PutUint16(buf[21:23], 0)
	binary.BigEndian.PutUint32(buf[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint64(buf[27:35], uint64(firstTimestamp))
	binary.BigEndian.PutUint64(buf[35:43], uint64(maxTimestamp))
	binary.BigEndian.PutUint64(buf[43:51], uint64(producerID))
	binary.BigEndian.PutUint16(buf[51:53], uint16(producerEpoch))
	binary.BigEndian.PutUint32(buf[53:57], uint32(firstSequence))
	binary.BigEndian.PutUint32(buf[57:61], uint32(numRecords))
	copy(buf[61:], records)

	// Compute CRC32-C over [21..end]
	checksum := crc32.Checksum(buf[21:], castagnoliTable)
	binary.BigEndian.PutUint32(buf[17:21], checksum)

	return buf
}

func TestReadRecordBatchHeader_HappyPath(t *testing.T) {
	batch := buildTestRecordBatch(
		42,   // firstOffset
		7,    // partitionLeaderEpoch
		5,    // lastOffsetDelta
		1000, // firstTimestamp
		2000, // maxTimestamp
		99,   // producerID
		3,    // producerEpoch
		10,   // firstSequence
		6,    // numRecords
		[]byte("fake-records-payload"),
	)

	hdr, err := ReadRecordBatchHeader(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hdr.FirstOffset != 42 {
		t.Errorf("FirstOffset: got %d, want 42", hdr.FirstOffset)
	}
	// Length = totalSize - 12 = (61 + 20) - 12 = 69
	wantLength := int32(len(batch) - 12)
	if hdr.Length != wantLength {
		t.Errorf("Length: got %d, want %d", hdr.Length, wantLength)
	}
	if hdr.PartitionLeaderEpoch != 7 {
		t.Errorf("PartitionLeaderEpoch: got %d, want 7", hdr.PartitionLeaderEpoch)
	}
	if hdr.Attributes != 0 {
		t.Errorf("Attributes: got %d, want 0", hdr.Attributes)
	}
	if hdr.LastOffsetDelta != 5 {
		t.Errorf("LastOffsetDelta: got %d, want 5", hdr.LastOffsetDelta)
	}
	if hdr.FirstTimestamp != 1000 {
		t.Errorf("FirstTimestamp: got %d, want 1000", hdr.FirstTimestamp)
	}
	if hdr.MaxTimestamp != 2000 {
		t.Errorf("MaxTimestamp: got %d, want 2000", hdr.MaxTimestamp)
	}
	if hdr.ProducerID != 99 {
		t.Errorf("ProducerID: got %d, want 99", hdr.ProducerID)
	}
	if hdr.ProducerEpoch != 3 {
		t.Errorf("ProducerEpoch: got %d, want 3", hdr.ProducerEpoch)
	}
	if hdr.FirstSequence != 10 {
		t.Errorf("FirstSequence: got %d, want 10", hdr.FirstSequence)
	}
	if hdr.NumRecords != 6 {
		t.Errorf("NumRecords: got %d, want 6", hdr.NumRecords)
	}
}

func TestReadRecordBatchHeader_ShortBuffer(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"one byte", []byte{0x01}},
		{"60 bytes (one short)", make([]byte, 60)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadRecordBatchHeader(tc.buf)
			if err == nil {
				t.Error("expected error for short buffer, got nil")
			}
		})
	}
}

func TestReadRecordBatchHeader_ExactSize(t *testing.T) {
	// A header-only batch (no records payload) should work fine.
	batch := buildTestRecordBatch(0, 0, 0, 0, 0, 0, 0, 0, 0, nil)
	if len(batch) != RecordBatchHeaderSize {
		t.Fatalf("expected %d bytes, got %d", RecordBatchHeaderSize, len(batch))
	}
	_, err := ReadRecordBatchHeader(batch)
	if err != nil {
		t.Fatalf("unexpected error on exact-size buffer: %v", err)
	}
}

func TestRecordBatchHeader_HelperMethods(t *testing.T) {
	batch := buildTestRecordBatch(10, 0, 4, 0, 0, 0, 0, 0, 5, nil)
	hdr, err := ReadRecordBatchHeader(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// RecordBatchSize = Length + 12
	wantSize := hdr.Length + 12
	if got := hdr.RecordBatchSize(); got != wantSize {
		t.Errorf("RecordBatchSize: got %d, want %d", got, wantSize)
	}

	// LastOffset = FirstOffset + LastOffsetDelta
	wantLast := int64(10 + 4)
	if got := hdr.LastOffset(); got != wantLast {
		t.Errorf("LastOffset: got %d, want %d", got, wantLast)
	}
}

func TestPatchRecordBatchFirstOffset_PreservesCRC(t *testing.T) {
	batch := buildTestRecordBatch(100, 1, 0, 0, 0, 0, 0, 0, 1, nil)

	// Verify CRC is valid before patching.
	if err := ValidateRecordBatchCRC(batch); err != nil {
		t.Fatalf("CRC invalid before patch: %v", err)
	}

	if err := PatchRecordBatchFirstOffset(batch, 999); err != nil {
		t.Fatalf("PatchRecordBatchFirstOffset: %v", err)
	}

	// CRC must still be valid after patching FirstOffset (bytes [0:8]).
	if err := ValidateRecordBatchCRC(batch); err != nil {
		t.Errorf("CRC invalid after PatchFirstOffset: %v", err)
	}

	// Verify the new value is readable.
	hdr, err := ReadRecordBatchHeader(batch)
	if err != nil {
		t.Fatalf("ReadRecordBatchHeader after patch: %v", err)
	}
	if hdr.FirstOffset != 999 {
		t.Errorf("FirstOffset after patch: got %d, want 999", hdr.FirstOffset)
	}
}

func TestPatchRecordBatchLeaderEpoch_PreservesCRC(t *testing.T) {
	batch := buildTestRecordBatch(0, 1, 0, 0, 0, 0, 0, 0, 1, nil)

	if err := ValidateRecordBatchCRC(batch); err != nil {
		t.Fatalf("CRC invalid before patch: %v", err)
	}

	if err := PatchRecordBatchLeaderEpoch(batch, 42); err != nil {
		t.Fatalf("PatchRecordBatchLeaderEpoch: %v", err)
	}

	if err := ValidateRecordBatchCRC(batch); err != nil {
		t.Errorf("CRC invalid after PatchLeaderEpoch: %v", err)
	}

	hdr, err := ReadRecordBatchHeader(batch)
	if err != nil {
		t.Fatalf("ReadRecordBatchHeader after patch: %v", err)
	}
	if hdr.PartitionLeaderEpoch != 42 {
		t.Errorf("PartitionLeaderEpoch after patch: got %d, want 42", hdr.PartitionLeaderEpoch)
	}
}

func TestValidateRecordBatchCRC_DetectsCorruption(t *testing.T) {
	batch := buildTestRecordBatch(0, 0, 0, 0, 0, 0, 0, 0, 1, []byte("payload"))

	if err := ValidateRecordBatchCRC(batch); err != nil {
		t.Fatalf("CRC should be valid initially: %v", err)
	}

	// Corrupt a byte in the CRC-protected region (byte 21 is Attributes MSB).
	batch[21] ^= 0xFF
	if err := ValidateRecordBatchCRC(batch); err == nil {
		t.Error("expected CRC error after corruption, got nil")
	}
}

func TestValidateRecordBatchCRC_ShortBuffer(t *testing.T) {
	// Buffer shorter than 21 bytes cannot even have a CRC region.
	if err := ValidateRecordBatchCRC(make([]byte, 20)); err == nil {
		t.Error("expected error for buffer shorter than 21 bytes")
	}
}

func TestPatchFirstOffset_RoundTrip(t *testing.T) {
	offsets := []int64{0, 1, -1, 1<<62 - 1, -1 << 62}
	for _, off := range offsets {
		batch := buildTestRecordBatch(0, 0, 0, 0, 0, 0, 0, 0, 1, nil)
		if err := PatchRecordBatchFirstOffset(batch, off); err != nil {
			t.Fatalf("offset %d: patch error: %v", off, err)
		}
		hdr, err := ReadRecordBatchHeader(batch)
		if err != nil {
			t.Fatalf("offset %d: read error: %v", off, err)
		}
		if hdr.FirstOffset != off {
			t.Errorf("offset %d: got %d", off, hdr.FirstOffset)
		}
	}
}
