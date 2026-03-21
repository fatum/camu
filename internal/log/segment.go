package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

const (
	segmentMagic   uint32 = 0x43414D55 // "CAMU"
	segmentVersion byte   = 1

	CompressionNone   = "none"
	CompressionSnappy = "snappy"
	CompressionZstd   = "zstd"

	compressionFlagNone   byte = 0
	compressionFlagSnappy byte = 1
	compressionFlagZstd   byte = 2
)

// WriteSegment writes a segment header followed by message frames to w.
// Header format: [4B magic][1B version][1B compression]
func WriteSegment(w io.Writer, msgs []Message, compression string) error {
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

	// Serialize all message frames to a buffer first.
	var payload bytes.Buffer
	for i, msg := range msgs {
		if err := writeMessageFrame(&payload, msg); err != nil {
			return fmt.Errorf("write message frame %d: %w", i, err)
		}
	}

	// Compress the payload if needed.
	var compressedPayload []byte
	switch compressionFlag {
	case compressionFlagNone:
		compressedPayload = payload.Bytes()
	case compressionFlagSnappy:
		compressedPayload = snappy.Encode(nil, payload.Bytes())
	case compressionFlagZstd:
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			return fmt.Errorf("create zstd encoder: %w", err)
		}
		compressedPayload = enc.EncodeAll(payload.Bytes(), nil)
		enc.Close()
	}

	// Write header: [4-byte magic][1-byte version][1-byte compression]
	if err := binary.Write(w, binary.BigEndian, segmentMagic); err != nil {
		return fmt.Errorf("write magic: %w", err)
	}
	if _, err := w.Write([]byte{segmentVersion, compressionFlag}); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write compressed (or plain) payload.
	if _, err := w.Write(compressedPayload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
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
	for k, v := range msg.Headers {
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
	return ReadSegmentFromOffset(r, size, 0, 0)
}

// ReadSegmentFromOffset reads frames from r, skipping messages with offset < startOffset,
// and returning up to limit messages (0 means no limit).
func ReadSegmentFromOffset(r io.ReaderAt, size int64, startOffset uint64, limit int) ([]Message, error) {
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

	// Read remaining payload bytes
	rawPayload, err := io.ReadAll(sr)
	if err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	// Decompress if needed
	var decompressed []byte
	switch compressionFlag[0] {
	case compressionFlagNone:
		decompressed = rawPayload
	case compressionFlagSnappy:
		decompressed, err = snappy.Decode(nil, rawPayload)
		if err != nil {
			return nil, fmt.Errorf("snappy decompress: %w", err)
		}
	case compressionFlagZstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("create zstd decoder: %w", err)
		}
		decompressed, err = dec.DecodeAll(rawPayload, nil)
		dec.Close()
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", compressionFlag[0])
	}

	pr := bytes.NewReader(decompressed)
	var msgs []Message
	for {
		msg, err := readMessageFrame(pr)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read message frame: %w", err)
		}

		// Skip messages before startOffset
		if msg.Offset < startOffset {
			continue
		}

		msgs = append(msgs, msg)

		if limit > 0 && len(msgs) >= limit {
			break
		}
	}
	return msgs, nil
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
