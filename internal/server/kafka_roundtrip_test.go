package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
)

// TestKafkaRecordBatchRoundTrip verifies the full zero-copy produce/fetch path:
//
//  1. EncodeRecordBatch → AppendRawBatch (3 batches with varied content)
//  2. ReadRawBatches → DecodeRecordBatch (verify content matches)
//  3. On-disk format is bare RecordBatch (no envelope), CRC valid
//  4. Offsets assigned sequentially, HW returned correctly
//  5. Seal the segment, read sealed file — identical bytes, sidecar parseable
func TestKafkaRecordBatchRoundTrip(t *testing.T) {
	pm := newTestPartitionManagerWithSegmentMaxSize(t, 1<<20)

	tc := meta.TopicConfig{
		Name:              "rt-topic",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := pm.InitTopic(context.Background(), tc, map[int]uint64{}); err != nil {
		t.Fatalf("InitTopic() error = %v", err)
	}

	ps := pm.GetPartitionState("rt-topic", 0)
	if ps == nil {
		t.Fatal("expected partition state")
	}

	segDir := filepath.Join(t.TempDir(), "seg")
	as, err := log.OpenActiveSegment(segDir, 0)
	if err != nil {
		t.Fatalf("OpenActiveSegment() error = %v", err)
	}
	ps.mu.Lock()
	ps.activeSegment = as
	ps.isLeader = true
	ps.mu.Unlock()

	now := time.Now().UnixMilli()

	// --- Batch 1: 3 messages with keys, values, and headers ---
	batch1Msgs := []log.Message{
		{Key: []byte("fruit"), Value: []byte("apple"), Timestamp: now, Headers: map[string]string{"color": "red", "origin": "farm"}},
		{Key: []byte("veggie"), Value: []byte("carrot"), Timestamp: now + 1, Headers: map[string]string{"color": "orange"}},
		{Key: nil, Value: []byte("no-key"), Timestamp: now + 2},
	}
	raw1 := log.EncodeRecordBatch(0, batch1Msgs)

	base1, err := pm.AppendRawBatch(context.Background(), "rt-topic", 0, raw1)
	if err != nil {
		t.Fatalf("AppendRawBatch(batch1) error = %v", err)
	}
	if base1 != 0 {
		t.Fatalf("batch1 baseOffset = %d, want 0", base1)
	}

	// --- Batch 2: 2 messages ---
	batch2Msgs := []log.Message{
		{Key: []byte("k4"), Value: []byte("hello world"), Timestamp: now + 10},
		{Key: []byte("k5"), Value: []byte("foo bar baz"), Timestamp: now + 11},
	}
	raw2 := log.EncodeRecordBatch(0, batch2Msgs)

	base2, err := pm.AppendRawBatch(context.Background(), "rt-topic", 0, raw2)
	if err != nil {
		t.Fatalf("AppendRawBatch(batch2) error = %v", err)
	}
	if base2 != 3 {
		t.Fatalf("batch2 baseOffset = %d, want 3", base2)
	}

	// --- Batch 3: 1 large message ---
	batch3Msgs := []log.Message{
		{Key: []byte("big-key"), Value: bytes.Repeat([]byte("X"), 1024), Timestamp: now + 20, Headers: map[string]string{"batch": "three"}},
	}
	raw3 := log.EncodeRecordBatch(0, batch3Msgs)

	base3, err := pm.AppendRawBatch(context.Background(), "rt-topic", 0, raw3)
	if err != nil {
		t.Fatalf("AppendRawBatch(batch3) error = %v", err)
	}
	if base3 != 5 {
		t.Fatalf("batch3 baseOffset = %d, want 5", base3)
	}

	// === Fetch all from offset 0 ===
	data, hw, err := pm.ReadRawBatches(context.Background(), "rt-topic", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches(offset=0) error = %v", err)
	}
	if hw != 6 {
		t.Fatalf("high watermark = %d, want 6", hw)
	}
	if len(data) == 0 {
		t.Fatal("ReadRawBatches returned empty data")
	}

	// Parse and verify all three batches from returned bytes.
	pos := 0
	expectedBases := []int64{0, 3, 5}
	allMsgs := [][]log.Message{batch1Msgs, batch2Msgs, batch3Msgs}

	for i, wantBase := range expectedBases {
		if pos >= len(data) {
			t.Fatalf("ran out of data at batch %d (pos=%d, len=%d)", i+1, pos, len(data))
		}
		hdr, err := log.ReadRecordBatchHeader(data[pos:])
		if err != nil {
			t.Fatalf("batch %d: ReadRecordBatchHeader error = %v", i+1, err)
		}
		if hdr.FirstOffset != wantBase {
			t.Fatalf("batch %d: FirstOffset = %d, want %d", i+1, hdr.FirstOffset, wantBase)
		}
		batchSize := int(hdr.RecordBatchSize())
		if pos+batchSize > len(data) {
			t.Fatalf("batch %d: batch extends past data boundary (pos=%d size=%d len=%d)", i+1, pos, batchSize, len(data))
		}
		batchBytes := data[pos : pos+batchSize]

		// Validate CRC.
		if err := log.ValidateRecordBatchCRC(batchBytes); err != nil {
			t.Fatalf("batch %d: CRC validation failed: %v", i+1, err)
		}

		// Decode and verify message content.
		decoded, err := log.DecodeRecordBatch(batchBytes)
		if err != nil {
			t.Fatalf("batch %d: DecodeRecordBatch error = %v", i+1, err)
		}
		want := allMsgs[i]
		if len(decoded) != len(want) {
			t.Fatalf("batch %d: decoded %d messages, want %d", i+1, len(decoded), len(want))
		}
		for j, msg := range decoded {
			if !bytes.Equal(msg.Key, want[j].Key) {
				t.Errorf("batch %d msg %d: key = %q, want %q", i+1, j, msg.Key, want[j].Key)
			}
			if !bytes.Equal(msg.Value, want[j].Value) {
				t.Errorf("batch %d msg %d: value = %q, want %q", i+1, j, msg.Value, want[j].Value)
			}
			for hk, hv := range want[j].Headers {
				if got := msg.Headers[hk]; got != hv {
					t.Errorf("batch %d msg %d: header[%q] = %q, want %q", i+1, j, hk, got, hv)
				}
			}
		}

		pos += batchSize
	}
	if pos != len(data) {
		t.Errorf("trailing bytes after all batches: consumed %d of %d", pos, len(data))
	}

	// === Partial fetch from offset 3 ===
	data2, hw2, err := pm.ReadRawBatches(context.Background(), "rt-topic", 0, 3, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches(offset=3) error = %v", err)
	}
	if hw2 != 6 {
		t.Fatalf("partial fetch hw = %d, want 6", hw2)
	}
	hdr2, err := log.ReadRecordBatchHeader(data2)
	if err != nil {
		t.Fatalf("partial fetch header error = %v", err)
	}
	if hdr2.FirstOffset != 3 {
		t.Fatalf("partial fetch FirstOffset = %d, want 3", hdr2.FirstOffset)
	}

	// === Fetch beyond HW returns empty ===
	data3, hw3, err := pm.ReadRawBatches(context.Background(), "rt-topic", 0, 6, 1<<20)
	if err != nil {
		t.Fatalf("ReadRawBatches(offset=6) error = %v", err)
	}
	if hw3 != 6 {
		t.Fatalf("beyond-HW hw = %d, want 6", hw3)
	}
	if len(data3) != 0 {
		t.Fatalf("beyond-HW data = %d bytes, want 0", len(data3))
	}

	// === On-disk format: bare RecordBatches, no envelope ===
	// The active segment file should be named "00000000000000000000.log".
	diskPath := filepath.Join(segDir, "00000000000000000000.log")
	diskData, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("reading segment file %s: %v", diskPath, err)
	}

	// Scan through file verifying every batch: header parseable, CRC valid.
	diskPos := 0
	diskBatchCount := 0
	diskBases := []int64{}
	for diskPos < len(diskData) {
		hdr, err := log.ReadRecordBatchHeader(diskData[diskPos:])
		if err != nil {
			t.Fatalf("on-disk batch %d at pos %d: ReadRecordBatchHeader error = %v", diskBatchCount+1, diskPos, err)
		}
		sz := int(hdr.RecordBatchSize())
		if diskPos+sz > len(diskData) {
			t.Fatalf("on-disk batch %d at pos %d: extends past EOF (size=%d fileSize=%d)", diskBatchCount+1, diskPos, sz, len(diskData))
		}
		if err := log.ValidateRecordBatchCRC(diskData[diskPos : diskPos+sz]); err != nil {
			t.Fatalf("on-disk batch %d: CRC invalid: %v", diskBatchCount+1, err)
		}
		diskBases = append(diskBases, hdr.FirstOffset)
		diskPos += sz
		diskBatchCount++
	}
	if diskBatchCount != 3 {
		t.Fatalf("on-disk batch count = %d, want 3", diskBatchCount)
	}
	for i, b := range expectedBases {
		if diskBases[i] != b {
			t.Errorf("on-disk batch %d: FirstOffset = %d, want %d", i+1, diskBases[i], b)
		}
	}

	// On-disk bytes should match what ReadRawBatches returned (same contiguous data).
	if !bytes.Equal(diskData, data) {
		t.Errorf("on-disk bytes differ from ReadRawBatches output (disk=%d bytes, fetch=%d bytes)", len(diskData), len(data))
	}

	// === Seal and verify sidecar ===
	segPath, sidecarPath, err := as.Seal()
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}

	// Sealed segment file should contain identical bytes.
	sealedData, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("reading sealed segment %s: %v", segPath, err)
	}
	if !bytes.Equal(diskData, sealedData) {
		t.Errorf("sealed segment differs from active (active=%d bytes, sealed=%d bytes)", len(diskData), len(sealedData))
	}

	// Sidecar must parse and contain 3 index entries with correct base offsets.
	sidecarData, err := os.ReadFile(sidecarPath)
	if err != nil {
		t.Fatalf("reading sidecar %s: %v", sidecarPath, err)
	}
	entries, _, err := log.ReadSidecar(sidecarData)
	if err != nil {
		t.Fatalf("ReadSidecar() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("sidecar entry count = %d, want 3", len(entries))
	}
	for i, e := range entries {
		if e.BaseOffset != expectedBases[i] {
			t.Errorf("sidecar entry %d: BaseOffset = %d, want %d", i, e.BaseOffset, expectedBases[i])
		}
	}
}
