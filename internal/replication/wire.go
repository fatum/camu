package replication

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	replicaFetchAPIKey         = int16(1000)
	replicaFetchAPIVer         = int16(0)
	replicaFetchHeaderSize     = 2 + 2 + 4                     // apiKey + apiVersion + correlationID
	replicaFetchRespHeaderSize = 4 + 2 + 8 + 8 + 8 + 8 + 8 + 4 // correlationID + errorCode + 5×uint64 + batchLen
	maxReplicaRequestSize      = 1 << 20
)

const (
	ReplicaErrOK       = int16(0)
	ReplicaErrNotFound = int16(1)
	ReplicaErrInternal = int16(2)
	ReplicaErrTruncate = int16(3) // epoch divergence: truncate to TruncateTo with LeaderEpoch
)

// ReplicaFetchRequest is the follower→leader replication fetch request.
type ReplicaFetchRequest struct {
	CorrelationID int32
	Topic         string
	PartitionID   int32
	FromOffset    uint64
	ReplicaID     string
	ReplicaOffset uint64
	ReplicaEpoch  uint64
	MaxBytes      int32
}

// ReplicaFetchResponse is the leader→follower replication fetch response.
// When ErrorCode is ReplicaErrTruncate, the follower must truncate and
// resume from TruncateTo with the given LeaderEpoch. BatchData contains raw
// concatenated Kafka v2 RecordBatch bytes (may be nil/empty for streaming).
type ReplicaFetchResponse struct {
	CorrelationID int32
	ErrorCode     int16
	TruncateTo    uint64
	LeaderEpoch   uint64
	HighWatermark uint64
	FlushedOffset uint64
	ActiveBase    uint64
	BatchData     []byte // nil when using streaming (BatchDataLen > 0 with io.Reader)
	BatchDataLen  int32  // length of batch data that follows the header on the wire
}

// EncodeRequest encodes a ReplicaFetchRequest into a length-prefixed frame
// suitable for writing to a TCP connection.
func EncodeRequest(req *ReplicaFetchRequest) []byte {
	topicLen := len(req.Topic)
	replicaIDLen := len(req.ReplicaID)
	payloadLen := replicaFetchHeaderSize + 2 + topicLen + 4 + 8 + 2 + replicaIDLen + 8 + 8 + 4

	buf := make([]byte, 4+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))

	off := 4
	binary.BigEndian.PutUint16(buf[off:], uint16(replicaFetchAPIKey))
	off += 2
	binary.BigEndian.PutUint16(buf[off:], uint16(replicaFetchAPIVer))
	off += 2
	binary.BigEndian.PutUint32(buf[off:], uint32(req.CorrelationID))
	off += 4

	binary.BigEndian.PutUint16(buf[off:], uint16(topicLen))
	off += 2
	copy(buf[off:], req.Topic)
	off += topicLen

	binary.BigEndian.PutUint32(buf[off:], uint32(req.PartitionID))
	off += 4

	binary.BigEndian.PutUint64(buf[off:], req.FromOffset)
	off += 8

	binary.BigEndian.PutUint16(buf[off:], uint16(replicaIDLen))
	off += 2
	copy(buf[off:], req.ReplicaID)
	off += replicaIDLen

	binary.BigEndian.PutUint64(buf[off:], req.ReplicaOffset)
	off += 8

	binary.BigEndian.PutUint64(buf[off:], req.ReplicaEpoch)
	off += 8

	binary.BigEndian.PutUint32(buf[off:], uint32(req.MaxBytes))

	return buf
}

// DecodeRequest reads a single length-prefixed ReplicaFetchRequest from r.
func DecodeRequest(r io.Reader) (*ReplicaFetchRequest, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := int32(binary.BigEndian.Uint32(lenBuf[:]))
	if length <= 0 || length > maxReplicaRequestSize {
		return nil, fmt.Errorf("replication: invalid request length %d", length)
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	return parseRequest(body)
}

func parseRequest(body []byte) (*ReplicaFetchRequest, error) {
	if len(body) < replicaFetchHeaderSize {
		return nil, fmt.Errorf("replication: request body too short: %d", len(body))
	}
	apiKey := int16(binary.BigEndian.Uint16(body[0:2]))
	if apiKey != replicaFetchAPIKey {
		return nil, fmt.Errorf("replication: unexpected API key %d", apiKey)
	}
	apiVersion := int16(binary.BigEndian.Uint16(body[2:4]))
	if apiVersion != replicaFetchAPIVer {
		return nil, fmt.Errorf("replication: unsupported API version %d", apiVersion)
	}

	req := &ReplicaFetchRequest{
		CorrelationID: int32(binary.BigEndian.Uint32(body[4:8])),
	}

	off := 8
	topicLen, off2, err := readString(body, off)
	if err != nil {
		return nil, fmt.Errorf("replication: decode topic: %w", err)
	}
	req.Topic = topicLen
	off = off2

	if off+4 > len(body) {
		return nil, fmt.Errorf("replication: truncated partitionID")
	}
	req.PartitionID = int32(binary.BigEndian.Uint32(body[off:]))
	off += 4

	if off+8 > len(body) {
		return nil, fmt.Errorf("replication: truncated fromOffset")
	}
	req.FromOffset = binary.BigEndian.Uint64(body[off:])
	off += 8

	replicaID, off2, err := readString(body, off)
	if err != nil {
		return nil, fmt.Errorf("replication: decode replicaID: %w", err)
	}
	req.ReplicaID = replicaID
	off = off2

	if off+8 > len(body) {
		return nil, fmt.Errorf("replication: truncated replicaOffset")
	}
	req.ReplicaOffset = binary.BigEndian.Uint64(body[off:])
	off += 8

	if off+8 > len(body) {
		return nil, fmt.Errorf("replication: truncated replicaEpoch")
	}
	req.ReplicaEpoch = binary.BigEndian.Uint64(body[off:])
	off += 8

	if off+4 > len(body) {
		return nil, fmt.Errorf("replication: truncated maxBytes")
	}
	req.MaxBytes = int32(binary.BigEndian.Uint32(body[off:]))

	return req, nil
}

// EncodeResponseHeader encodes the fixed-size response header (everything
// except the batch data) into a standalone buffer. The caller should write
// the 4-byte frame length, this header, and then the batch data using
// net.Buffers for zero-copy writev.
func EncodeResponseHeader(resp *ReplicaFetchResponse) []byte {
	buf := make([]byte, replicaFetchRespHeaderSize)
	off := 0
	binary.BigEndian.PutUint32(buf[off:], uint32(resp.CorrelationID))
	off += 4
	binary.BigEndian.PutUint16(buf[off:], uint16(resp.ErrorCode))
	off += 2
	binary.BigEndian.PutUint64(buf[off:], resp.TruncateTo)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], resp.LeaderEpoch)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], resp.HighWatermark)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], resp.FlushedOffset)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], resp.ActiveBase)
	off += 8
	dataLen := resp.BatchDataLen
	if dataLen == 0 && len(resp.BatchData) > 0 {
		dataLen = int32(len(resp.BatchData))
	}
	binary.BigEndian.PutUint32(buf[off:], uint32(dataLen))
	return buf
}

// ReadResponse reads a single ReplicaFetchResponse from r, including the
// batch data payload materialized into BatchData. Use ReadResponseHeader for
// zero-copy streaming of batch data.
func ReadResponse(r io.Reader) (*ReplicaFetchResponse, error) {
	resp, batchLen, err := ReadResponseHeader(r)
	if err != nil {
		return resp, err
	}
	if batchLen > 0 {
		resp.BatchData = make([]byte, batchLen)
		if _, err := io.ReadFull(r, resp.BatchData); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

// ReadResponseHeader reads the fixed-size response header and returns the
// response metadata along with the number of batch-data bytes that follow on
// the wire. The caller is responsible for reading exactly batchLen bytes from
// r before decoding the next response.
func ReadResponseHeader(r io.Reader) (resp *ReplicaFetchResponse, batchLen int32, err error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, 0, err
	}
	frameLen := int32(binary.BigEndian.Uint32(lenBuf[:]))
	if frameLen < replicaFetchRespHeaderSize || frameLen > (1<<30) {
		return nil, 0, fmt.Errorf("replication: invalid response frame length %d", frameLen)
	}

	var header [replicaFetchRespHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}

	resp = &ReplicaFetchResponse{
		CorrelationID: int32(binary.BigEndian.Uint32(header[0:4])),
		ErrorCode:     int16(binary.BigEndian.Uint16(header[4:6])),
		TruncateTo:    binary.BigEndian.Uint64(header[6:14]),
		LeaderEpoch:   binary.BigEndian.Uint64(header[14:22]),
		HighWatermark: binary.BigEndian.Uint64(header[22:30]),
		FlushedOffset: binary.BigEndian.Uint64(header[30:38]),
		ActiveBase:    binary.BigEndian.Uint64(header[38:46]),
	}

	batchLen = int32(binary.BigEndian.Uint32(header[46:50]))
	if batchLen < 0 || batchLen > (1<<28) {
		return resp, batchLen, fmt.Errorf("replication: invalid batch data length %d", batchLen)
	}
	resp.BatchDataLen = batchLen
	return resp, batchLen, nil
}

func readString(buf []byte, off int) (string, int, error) {
	if off+2 > len(buf) {
		return "", off, fmt.Errorf("string length truncated")
	}
	strLen := int(binary.BigEndian.Uint16(buf[off:]))
	off += 2
	if off+strLen > len(buf) {
		return "", off, fmt.Errorf("string body truncated")
	}
	s := string(buf[off : off+strLen])
	off += strLen
	return s, off, nil
}
