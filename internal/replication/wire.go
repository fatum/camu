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

// Envelope version byte for batch wire format.
const envelopeVersion = uint8(1)

// WriteBatchFrames writes a single batch envelope in the WAL batch format:
//
//	[4B total_envelope_length]
//	[1B envelope_version = 1]
//	[8B producer_id]
//	[8B sequence]
//	[4B message_count]
//	[per message: [4B frame_length][frame_bytes][4B CRC32]]
//	[4B CRC32 over entire envelope payload]
func WriteBatchFrames(w io.Writer, batch log.Batch) error {
	// Build the envelope payload into a buffer so we can compute its length
	// and trailing CRC before writing.
	var payload bytes.Buffer

	// Version byte
	if err := payload.WriteByte(envelopeVersion); err != nil {
		return fmt.Errorf("wire: write envelope version: %w", err)
	}
	// Producer metadata
	if err := binary.Write(&payload, binary.BigEndian, batch.ProducerID); err != nil {
		return fmt.Errorf("wire: write producer_id: %w", err)
	}
	if err := binary.Write(&payload, binary.BigEndian, batch.Sequence); err != nil {
		return fmt.Errorf("wire: write sequence: %w", err)
	}
	// Message count
	msgCount := uint32(len(batch.Messages))
	if err := binary.Write(&payload, binary.BigEndian, msgCount); err != nil {
		return fmt.Errorf("wire: write message_count: %w", err)
	}
	// Per-message frames
	for i, msg := range batch.Messages {
		var frameBuf bytes.Buffer
		if err := marshalMessageFrame(&frameBuf, msg); err != nil {
			return fmt.Errorf("wire: serialize message %d: %w", i, err)
		}
		frameBytes := frameBuf.Bytes()
		checksum := crc32.ChecksumIEEE(frameBytes)

		if err := binary.Write(&payload, binary.BigEndian, uint32(len(frameBytes))); err != nil {
			return fmt.Errorf("wire: write frame length: %w", err)
		}
		if _, err := payload.Write(frameBytes); err != nil {
			return fmt.Errorf("wire: write frame: %w", err)
		}
		if err := binary.Write(&payload, binary.BigEndian, checksum); err != nil {
			return fmt.Errorf("wire: write frame CRC: %w", err)
		}
	}

	payloadBytes := payload.Bytes()

	// Envelope CRC covers the entire payload (version + metadata + messages).
	envelopeCRC := crc32.ChecksumIEEE(payloadBytes)

	// Total envelope length = payload + 4B envelope CRC.
	totalLen := uint32(len(payloadBytes) + 4)

	// Write to the actual writer.
	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return fmt.Errorf("wire: write envelope length: %w", err)
	}
	if _, err := w.Write(payloadBytes); err != nil {
		return fmt.Errorf("wire: write envelope payload: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, envelopeCRC); err != nil {
		return fmt.Errorf("wire: write envelope CRC: %w", err)
	}
	return nil
}

// ReadBatchFrames reads batch envelopes until EOF and returns all batches
// with their producer metadata.
func ReadBatchFrames(r io.Reader) ([]log.BatchFrame, error) {
	var frames []log.BatchFrame
	for {
		var totalLen uint32
		if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("wire: read envelope length: %w", err)
		}

		envelopeBytes := make([]byte, totalLen)
		if _, err := io.ReadFull(r, envelopeBytes); err != nil {
			return nil, fmt.Errorf("wire: read envelope: %w", err)
		}

		// The last 4 bytes are the envelope CRC.
		if totalLen < 4 {
			return nil, fmt.Errorf("wire: envelope too short")
		}
		payloadBytes := envelopeBytes[:totalLen-4]
		storedCRC := binary.BigEndian.Uint32(envelopeBytes[totalLen-4:])

		if crc32.ChecksumIEEE(payloadBytes) != storedCRC {
			return nil, fmt.Errorf("wire: envelope CRC mismatch")
		}

		pr := bytes.NewReader(payloadBytes)

		// Version byte
		var version uint8
		if err := binary.Read(pr, binary.BigEndian, &version); err != nil {
			return nil, fmt.Errorf("wire: read envelope version: %w", err)
		}
		if version != envelopeVersion {
			return nil, fmt.Errorf("wire: unsupported envelope version %d", version)
		}

		var meta log.BatchMeta
		if err := binary.Read(pr, binary.BigEndian, &meta.ProducerID); err != nil {
			return nil, fmt.Errorf("wire: read producer_id: %w", err)
		}
		if err := binary.Read(pr, binary.BigEndian, &meta.Sequence); err != nil {
			return nil, fmt.Errorf("wire: read sequence: %w", err)
		}

		var msgCount uint32
		if err := binary.Read(pr, binary.BigEndian, &msgCount); err != nil {
			return nil, fmt.Errorf("wire: read message_count: %w", err)
		}
		meta.MessageCount = int(msgCount)
		for i := range msgCount {
			var frameLen uint32
			if err := binary.Read(pr, binary.BigEndian, &frameLen); err != nil {
				return nil, fmt.Errorf("wire: read frame length %d: %w", i, err)
			}

			frameBytes := make([]byte, frameLen)
			if _, err := io.ReadFull(pr, frameBytes); err != nil {
				return nil, fmt.Errorf("wire: read frame %d: %w", i, err)
			}

			var frameCRC uint32
			if err := binary.Read(pr, binary.BigEndian, &frameCRC); err != nil {
				return nil, fmt.Errorf("wire: read frame CRC %d: %w", i, err)
			}
			if crc32.ChecksumIEEE(frameBytes) != frameCRC {
				return nil, fmt.Errorf("wire: CRC mismatch on frame %d", i)
			}

			if len(frameBytes) < 8 {
				return nil, fmt.Errorf("wire: frame %d too short", i)
			}
			offset := binary.BigEndian.Uint64(frameBytes[:8])
			if i == 0 {
				meta.FirstOffset = offset
			}
			meta.LastOffset = offset
		}

		raw := make([]byte, 0, 4+len(envelopeBytes))
		raw = binary.BigEndian.AppendUint32(raw, totalLen)
		raw = append(raw, envelopeBytes...)
		frames = append(frames, log.BatchFrame{Data: raw, Meta: meta})
	}
	return frames, nil
}

// WriteMessageFrames writes messages as a single batch envelope with
// ProducerID=0 (no idempotency). This preserves the existing API for
// callers that don't need producer metadata.
func WriteMessageFrames(w io.Writer, msgs []log.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	return WriteBatchFrames(w, log.Batch{
		ProducerID: 0,
		Sequence:   0,
		Messages:   msgs,
	})
}

// ReadMessageFrames reads batch envelopes and flattens all messages,
// discarding producer metadata. This preserves the existing API for
// callers that don't need producer metadata.
func ReadMessageFrames(r io.Reader) ([]log.Message, error) {
	frames, err := ReadBatchFrames(r)
	if err != nil {
		return nil, err
	}
	var msgs []log.Message
	for _, frame := range frames {
		pr := bytes.NewReader(frame.Data[4 : len(frame.Data)-4])
		var version uint8
		if err := binary.Read(pr, binary.BigEndian, &version); err != nil {
			return nil, err
		}
		if version != envelopeVersion {
			return nil, fmt.Errorf("wire: unsupported envelope version %d", version)
		}
		var producerID, sequence uint64
		var msgCount uint32
		if err := binary.Read(pr, binary.BigEndian, &producerID); err != nil {
			return nil, err
		}
		if err := binary.Read(pr, binary.BigEndian, &sequence); err != nil {
			return nil, err
		}
		if err := binary.Read(pr, binary.BigEndian, &msgCount); err != nil {
			return nil, err
		}
		for i := uint32(0); i < msgCount; i++ {
			var frameLen uint32
			if err := binary.Read(pr, binary.BigEndian, &frameLen); err != nil {
				return nil, err
			}
			frameBytes := make([]byte, frameLen)
			if _, err := io.ReadFull(pr, frameBytes); err != nil {
				return nil, err
			}
			var frameCRC uint32
			if err := binary.Read(pr, binary.BigEndian, &frameCRC); err != nil {
				return nil, err
			}
			msg, err := unmarshalMessageFrame(bytes.NewReader(frameBytes))
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
		}
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
