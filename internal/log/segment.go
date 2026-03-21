package log

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	segmentMagic   uint32 = 0x43414D55 // "CAMU"
	segmentVersion byte   = 1

	CompressionNone   = "none"
	CompressionSnappy = "snappy"
	CompressionZstd   = "zstd"
)

// WriteSegment writes a segment header followed by message frames to w.
// Only CompressionNone is supported; snappy/zstd will be added in Task 24.
func WriteSegment(w io.Writer, msgs []Message, compression string) error {
	if compression != CompressionNone {
		return fmt.Errorf("compression %q not yet supported", compression)
	}

	// Segment header: [4-byte magic][1-byte version]
	if err := binary.Write(w, binary.BigEndian, segmentMagic); err != nil {
		return fmt.Errorf("write magic: %w", err)
	}
	if _, err := w.Write([]byte{segmentVersion}); err != nil {
		return fmt.Errorf("write version: %w", err)
	}

	for i, msg := range msgs {
		if err := writeMessageFrame(w, msg); err != nil {
			return fmt.Errorf("write message frame %d: %w", i, err)
		}
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

	// Validate version
	var version [1]byte
	if _, err := io.ReadFull(sr, version[:]); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	if version[0] != segmentVersion {
		return nil, fmt.Errorf("unsupported segment version: %d", version[0])
	}

	var msgs []Message
	for {
		msg, err := readMessageFrame(sr)
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
