package replication

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestEncodeDecodeRequestRoundtrip(t *testing.T) {
	original := &ReplicaFetchRequest{
		CorrelationID: 42,
		Topic:         "test-topic",
		PartitionID:   3,
		FromOffset:    1000,
		ReplicaID:     "node-5",
		ReplicaOffset: 999,
		ReplicaEpoch:  7,
		MaxBytes:      1 << 20,
	}

	encoded := EncodeRequest(original)
	decoded, err := DecodeRequest(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
	if decoded.CorrelationID != original.CorrelationID {
		t.Errorf("CorrelationID = %d, want %d", decoded.CorrelationID, original.CorrelationID)
	}
	if decoded.Topic != original.Topic {
		t.Errorf("Topic = %q, want %q", decoded.Topic, original.Topic)
	}
	if decoded.PartitionID != original.PartitionID {
		t.Errorf("PartitionID = %d, want %d", decoded.PartitionID, original.PartitionID)
	}
	if decoded.FromOffset != original.FromOffset {
		t.Errorf("FromOffset = %d, want %d", decoded.FromOffset, original.FromOffset)
	}
	if decoded.ReplicaID != original.ReplicaID {
		t.Errorf("ReplicaID = %q, want %q", decoded.ReplicaID, original.ReplicaID)
	}
	if decoded.ReplicaOffset != original.ReplicaOffset {
		t.Errorf("ReplicaOffset = %d, want %d", decoded.ReplicaOffset, original.ReplicaOffset)
	}
	if decoded.ReplicaEpoch != original.ReplicaEpoch {
		t.Errorf("ReplicaEpoch = %d, want %d", decoded.ReplicaEpoch, original.ReplicaEpoch)
	}
	if decoded.MaxBytes != original.MaxBytes {
		t.Errorf("MaxBytes = %d, want %d", decoded.MaxBytes, original.MaxBytes)
	}
}

func TestEncodeDecodeResponseRoundtrip(t *testing.T) {
	batchData := []byte("some-raw-batch-data-here")
	original := &ReplicaFetchResponse{
		CorrelationID: 99,
		ErrorCode:     ReplicaErrOK,
		TruncateTo:    0,
		LeaderEpoch:   3,
		HighWatermark: 500,
		FlushedOffset: 400,
		ActiveBase:    100,
		BatchData:     batchData,
	}

	header := EncodeResponseHeader(original)
	totalPayload := len(header) + len(original.BatchData)
	frame := make([]byte, 4+totalPayload)
	binary.BigEndian.PutUint32(frame[0:4], uint32(totalPayload))
	copy(frame[4:], header)
	copy(frame[4+len(header):], original.BatchData)

	decoded, err := ReadResponse(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("ReadResponse() error = %v", err)
	}
	if decoded.CorrelationID != original.CorrelationID {
		t.Errorf("CorrelationID = %d, want %d", decoded.CorrelationID, original.CorrelationID)
	}
	if decoded.ErrorCode != original.ErrorCode {
		t.Errorf("ErrorCode = %d, want %d", decoded.ErrorCode, original.ErrorCode)
	}
	if decoded.TruncateTo != original.TruncateTo {
		t.Errorf("TruncateTo = %d, want %d", decoded.TruncateTo, original.TruncateTo)
	}
	if decoded.LeaderEpoch != original.LeaderEpoch {
		t.Errorf("LeaderEpoch = %d, want %d", decoded.LeaderEpoch, original.LeaderEpoch)
	}
	if decoded.HighWatermark != original.HighWatermark {
		t.Errorf("HighWatermark = %d, want %d", decoded.HighWatermark, original.HighWatermark)
	}
	if decoded.FlushedOffset != original.FlushedOffset {
		t.Errorf("FlushedOffset = %d, want %d", decoded.FlushedOffset, original.FlushedOffset)
	}
	if decoded.ActiveBase != original.ActiveBase {
		t.Errorf("ActiveBase = %d, want %d", decoded.ActiveBase, original.ActiveBase)
	}
	if !bytes.Equal(decoded.BatchData, original.BatchData) {
		t.Errorf("BatchData mismatch: got %d bytes, want %d bytes", len(decoded.BatchData), len(original.BatchData))
	}
}

func TestEncodeDecodeResponseEmptyBatchData(t *testing.T) {
	original := &ReplicaFetchResponse{
		CorrelationID: 1,
		ErrorCode:     ReplicaErrOK,
		HighWatermark: 100,
		LeaderEpoch:   1,
	}

	header := EncodeResponseHeader(original)
	totalPayload := len(header)
	frame := make([]byte, 4+totalPayload)
	binary.BigEndian.PutUint32(frame[0:4], uint32(totalPayload))
	copy(frame[4:], header)

	decoded, err := ReadResponse(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("ReadResponse() error = %v", err)
	}
	if decoded.HighWatermark != 100 {
		t.Errorf("HighWatermark = %d, want 100", decoded.HighWatermark)
	}
	if len(decoded.BatchData) != 0 {
		t.Errorf("BatchData length = %d, want 0", len(decoded.BatchData))
	}
}

func TestDecodeRequestRejectsWrongAPIKey(t *testing.T) {
	req := &ReplicaFetchRequest{
		CorrelationID: 1,
		Topic:         "t",
		PartitionID:   0,
		FromOffset:    0,
		ReplicaID:     "n",
		ReplicaOffset: 0,
		ReplicaEpoch:  0,
		MaxBytes:      1024,
	}
	encoded := EncodeRequest(req)
	// Corrupt the API key.
	encoded[4] = 0
	encoded[5] = 0
	_, err := DecodeRequest(bytes.NewReader(encoded))
	if err == nil {
		t.Fatal("expected error for wrong API key")
	}
}

func TestReadResponseEmptyReader(t *testing.T) {
	_, err := ReadResponse(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}
