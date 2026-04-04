package log

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompressionCodec verifies the attribute bits are read correctly.
func TestCompressionCodec(t *testing.T) {
	makeHeader := func(attrs int16) []byte {
		b := make([]byte, 61)
		// Attributes at offset [21:23]
		b[21] = byte(attrs >> 8)
		b[22] = byte(attrs)
		return b
	}

	tests := []struct {
		name  string
		attrs int16
		want  int
	}{
		{"none", 0, 0},
		{"gzip", 1, 1},
		{"snappy", 2, 2},
		{"lz4", 3, 3},
		{"zstd", 4, 4},
		{"mask other bits", 0b1111_1000, 0}, // upper bits set, codec bits = 0
		{"lz4 with other bits", 0b0000_1011, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CompressionCodec(makeHeader(tc.attrs))
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCompressionCodecTooShort(t *testing.T) {
	// Buffer shorter than 23 bytes — codec should return 0 safely.
	assert.Equal(t, 0, CompressionCodec(make([]byte, 10)))
	assert.Equal(t, 0, CompressionCodec(nil))
}

// TestEncodeDecodeRoundTrip is the primary round-trip test.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	msgs := []Message{
		{
			Offset:    0,
			Timestamp: 1_700_000_000_000,
			Key:       []byte("key-1"),
			Value:     []byte("hello world"),
			Headers:   map[string]string{"content-type": "text/plain", "x-id": "42"},
		},
		{
			Offset:    1,
			Timestamp: 1_700_000_001_000,
			Key:       []byte("key-2"),
			Value:     []byte("second message"),
			Headers:   nil,
		},
		{
			Offset:    2,
			Timestamp: 1_700_000_002_000,
			Key:       nil,
			Value:     []byte("nil key message"),
			Headers:   map[string]string{"a": "1"},
		},
	}

	batch := EncodeRecordBatch(0, msgs)
	require.NotEmpty(t, batch)

	// CRC must be valid.
	require.NoError(t, ValidateRecordBatchCRC(batch))

	got, err := DecodeRecordBatch(batch)
	require.NoError(t, err)
	require.Len(t, got, len(msgs))

	for i, want := range msgs {
		t.Run("msg"+string(rune('0'+i)), func(t *testing.T) {
			assert.Equal(t, uint64(i), got[i].Offset, "offset")
			assert.Equal(t, want.Timestamp, got[i].Timestamp, "timestamp")
			assert.Equal(t, want.Key, got[i].Key, "key")
			assert.Equal(t, want.Value, got[i].Value, "value")
			assert.Equal(t, want.Headers, got[i].Headers, "headers")
		})
	}
}

func TestEncodeDecodeEmptyBatch(t *testing.T) {
	batch := EncodeRecordBatch(0, nil)
	require.NotEmpty(t, batch)
	require.NoError(t, ValidateRecordBatchCRC(batch))

	got, err := DecodeRecordBatch(batch)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestEncodeDecodeEmptyMsgSlice(t *testing.T) {
	batch := EncodeRecordBatch(0, []Message{})
	require.NoError(t, ValidateRecordBatchCRC(batch))
	got, err := DecodeRecordBatch(batch)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestEncodeNilKeyValue(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 1000, Key: nil, Value: nil},
	}
	batch := EncodeRecordBatch(0, msgs)
	require.NoError(t, ValidateRecordBatchCRC(batch))

	got, err := DecodeRecordBatch(batch)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Nil(t, got[0].Key)
	assert.Nil(t, got[0].Value)
}

func TestEncodeDecodeHeadersSorted(t *testing.T) {
	// Encode twice with different header-map iteration order (Go maps are random),
	// and verify both produce the identical bytes — determinism guaranteed by sorting.
	msgs := []Message{
		{
			Offset:    0,
			Timestamp: 999,
			Value:     []byte("v"),
			Headers:   map[string]string{"z": "last", "a": "first", "m": "middle"},
		},
	}

	b1 := EncodeRecordBatch(0, msgs)
	b2 := EncodeRecordBatch(0, msgs)
	assert.True(t, bytes.Equal(b1, b2), "encoding is not deterministic")
}

func TestEncodeBaseOffset(t *testing.T) {
	msgs := []Message{
		{Offset: 100, Timestamp: 1000, Value: []byte("a")},
		{Offset: 101, Timestamp: 1001, Value: []byte("b")},
	}
	batch := EncodeRecordBatch(100, msgs)
	require.NoError(t, ValidateRecordBatchCRC(batch))

	hdr, err := ReadRecordBatchHeader(batch)
	require.NoError(t, err)
	assert.Equal(t, int64(100), hdr.FirstOffset)
	assert.Equal(t, int32(1), hdr.LastOffsetDelta)
	assert.Equal(t, int32(2), hdr.NumRecords)

	got, err := DecodeRecordBatch(batch)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, uint64(100), got[0].Offset)
	assert.Equal(t, uint64(101), got[1].Offset)
}

func TestDecodeRecordBatchTooShort(t *testing.T) {
	_, err := DecodeRecordBatch(make([]byte, 10))
	assert.Error(t, err)
}

func TestDecodeRecordBatchGzip(t *testing.T) {
	// Build a batch manually with gzip-compressed records.
	msgs := []Message{
		{Offset: 0, Timestamp: 5000, Key: []byte("k"), Value: []byte("gzip-value")},
	}

	// Encode uncompressed first, then replace records section with gzip.
	uncompressed := EncodeRecordBatch(0, msgs)
	recordsPayload := uncompressed[RecordBatchHeaderSize:]

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(recordsPayload)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	batch := buildCompressedBatch(uncompressed, buf.Bytes(), 1 /*gzip*/)

	got, err := DecodeRecordBatch(batch)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []byte("k"), got[0].Key)
	assert.Equal(t, []byte("gzip-value"), got[0].Value)
}

func TestDecodeRecordBatchZstd(t *testing.T) {
	msgs := []Message{
		{Offset: 0, Timestamp: 7000, Key: []byte("zk"), Value: []byte("zstd-value")},
	}

	uncompressed := EncodeRecordBatch(0, msgs)
	recordsPayload := uncompressed[RecordBatchHeaderSize:]

	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := enc.EncodeAll(recordsPayload, nil)
	enc.Close()

	batch := buildCompressedBatch(uncompressed, compressed, 4 /*zstd*/)

	got, err := DecodeRecordBatch(batch)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []byte("zk"), got[0].Key)
	assert.Equal(t, []byte("zstd-value"), got[0].Value)
}

// buildCompressedBatch replaces the records section with compressedRecords and
// patches Attributes + CRC accordingly.
func buildCompressedBatch(original, compressedRecords []byte, codec int) []byte {
	batch := make([]byte, RecordBatchHeaderSize+len(compressedRecords))
	copy(batch, original[:RecordBatchHeaderSize])
	copy(batch[RecordBatchHeaderSize:], compressedRecords)

	// Patch Length field [8:12] = len(batch) - 12
	length := int32(len(batch) - 12)
	batch[8] = byte(length >> 24)
	batch[9] = byte(length >> 16)
	batch[10] = byte(length >> 8)
	batch[11] = byte(length)

	// Patch Attributes [21:23] — set codec bits 0-2.
	attrs := int16(batch[21])<<8 | int16(batch[22])
	attrs = (attrs &^ 0x07) | int16(codec)
	batch[21] = byte(attrs >> 8)
	batch[22] = byte(attrs)

	// Recompute CRC.
	crc := computeCRC32C(batch[21:])
	batch[17] = byte(crc >> 24)
	batch[18] = byte(crc >> 16)
	batch[19] = byte(crc >> 8)
	batch[20] = byte(crc)

	return batch
}

func TestDecodeRecordBatchRecordBatchHeaderFields(t *testing.T) {
	// Verify magic, producer fields, etc. are set correctly in encode output.
	msgs := []Message{
		{Offset: 0, Timestamp: 12345, Value: []byte("x")},
	}
	batch := EncodeRecordBatch(0, msgs)
	hdr, err := ReadRecordBatchHeader(batch)
	require.NoError(t, err)
	assert.Equal(t, int16(0), hdr.Attributes, "no compression")
	assert.Equal(t, int64(-1), hdr.ProducerID)
	assert.Equal(t, int16(-1), hdr.ProducerEpoch)
	assert.Equal(t, int32(-1), hdr.FirstSequence)
	assert.Equal(t, int32(-1), hdr.PartitionLeaderEpoch)
	assert.Equal(t, byte(2), batch[16], "magic byte = 2")
}
