package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/maksim/camu/internal/log"
)

// WriteMessageFrames writes messages in WAL frame format:
// [4B len][frame][4B CRC32] per message.
// This reuses the same frame format as WAL entries so that replicated data
// can be written directly into a follower's WAL without re-encoding.
func WriteMessageFrames(w io.Writer, msgs []log.Message) error {
	for i, msg := range msgs {
		var frameBuf bytes.Buffer
		if err := marshalMessageFrame(&frameBuf, msg); err != nil {
			return fmt.Errorf("wire: serialize message %d: %w", i, err)
		}
		frameBytes := frameBuf.Bytes()
		checksum := crc32.ChecksumIEEE(frameBytes)

		entryLen := uint32(len(frameBytes))
		if err := binary.Write(w, binary.BigEndian, entryLen); err != nil {
			return fmt.Errorf("wire: write entry length: %w", err)
		}
		if _, err := w.Write(frameBytes); err != nil {
			return fmt.Errorf("wire: write frame: %w", err)
		}
		if err := binary.Write(w, binary.BigEndian, checksum); err != nil {
			return fmt.Errorf("wire: write CRC: %w", err)
		}
	}
	return nil
}

// ReadMessageFrames reads all available message frames from the reader.
// It stops at EOF and returns the messages read so far. A CRC mismatch is
// treated as a hard error.
func ReadMessageFrames(r io.Reader) ([]log.Message, error) {
	var msgs []log.Message
	for {
		var entryLen uint32
		if err := binary.Read(r, binary.BigEndian, &entryLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("wire: read entry length: %w", err)
		}

		frameBytes := make([]byte, entryLen)
		if _, err := io.ReadFull(r, frameBytes); err != nil {
			return nil, fmt.Errorf("wire: read frame: %w", err)
		}

		var storedCRC uint32
		if err := binary.Read(r, binary.BigEndian, &storedCRC); err != nil {
			return nil, fmt.Errorf("wire: read CRC: %w", err)
		}

		if crc32.ChecksumIEEE(frameBytes) != storedCRC {
			return nil, fmt.Errorf("wire: CRC mismatch on frame")
		}

		msg, err := unmarshalMessageFrame(bytes.NewReader(frameBytes))
		if err != nil {
			return nil, fmt.Errorf("wire: decode message frame: %w", err)
		}

		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// marshalMessageFrame serializes a Message into the WAL frame format:
// [8B offset][8B timestamp][4B key len][key][4B value len][value]
// [4B header count]([4B hk len][hk][4B hv len][hv])...
func marshalMessageFrame(w io.Writer, msg log.Message) error {
	if err := binary.Write(w, binary.BigEndian, msg.Offset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, msg.Timestamp); err != nil {
		return err
	}
	if err := wirePutBytes(w, msg.Key); err != nil {
		return err
	}
	if err := wirePutBytes(w, msg.Value); err != nil {
		return err
	}
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
		if err := wirePutBytes(w, []byte(k)); err != nil {
			return err
		}
		if err := wirePutBytes(w, []byte(msg.Headers[k])); err != nil {
			return err
		}
	}
	return nil
}

// unmarshalMessageFrame deserializes a Message from the WAL frame format.
func unmarshalMessageFrame(r io.Reader) (log.Message, error) {
	var msg log.Message

	if err := binary.Read(r, binary.BigEndian, &msg.Offset); err != nil {
		return msg, err
	}
	if err := binary.Read(r, binary.BigEndian, &msg.Timestamp); err != nil {
		return msg, err
	}

	key, err := wireGetBytes(r)
	if err != nil {
		return msg, err
	}
	msg.Key = key

	value, err := wireGetBytes(r)
	if err != nil {
		return msg, err
	}
	msg.Value = value

	var headerCount uint32
	if err := binary.Read(r, binary.BigEndian, &headerCount); err != nil {
		return msg, err
	}
	if headerCount > 0 {
		msg.Headers = make(map[string]string, headerCount)
		for range headerCount {
			hk, err := wireGetBytes(r)
			if err != nil {
				return msg, err
			}
			hv, err := wireGetBytes(r)
			if err != nil {
				return msg, err
			}
			msg.Headers[string(hk)] = string(hv)
		}
	}
	return msg, nil
}

// wirePutBytes writes a 4-byte big-endian length prefix followed by b.
func wirePutBytes(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	if len(b) > 0 {
		_, err := w.Write(b)
		return err
	}
	return nil
}

// wireGetBytes reads a 4-byte big-endian length-prefixed byte slice.
func wireGetBytes(r io.Reader) ([]byte, error) {
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
