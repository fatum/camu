package log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// ReadFrom reads messages from the WAL starting at startOffset, up to limit messages.
func (w *WAL) ReadFrom(startOffset uint64, limit int) ([]Message, error) {
	if limit <= 0 {
		return nil, nil
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.readFromLocked(startOffset, limit)
}

// WalkFrom reads messages from the WAL starting at startOffset and calls visit
// for each decoded message. Returning false from visit stops the scan early.
func (w *WAL) WalkFrom(startOffset uint64, limit int, visit func(Message) bool) error {
	if limit <= 0 {
		return nil
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.walkFromLocked(startOffset, limit, visit)
}

// ReadFromLocked is like ReadFrom but assumes the caller already holds the
// partition-level lock.
func (w *WAL) ReadFromLocked(startOffset uint64, limit int) ([]Message, error) {
	if limit <= 0 {
		return nil, nil
	}

	return w.readFromLocked(startOffset, limit)
}

// WalkFromLocked is like WalkFrom but assumes the caller already holds the
// partition-level lock.
func (w *WAL) WalkFromLocked(startOffset uint64, limit int, visit func(Message) bool) error {
	if limit <= 0 {
		return nil
	}

	return w.walkFromLocked(startOffset, limit, visit)
}

func (w *WAL) readFromLocked(startOffset uint64, limit int) ([]Message, error) {
	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	result := make([]Message, 0, limit)
	for _, chunk := range chunks[startIdx:] {
		msgs, err := readChunkMessages(w.chunkPathLocked(chunk), startOffset, limit-len(result))
		if err != nil {
			return nil, err
		}
		result = append(result, msgs...)
		if len(result) >= limit {
			return result[:limit], nil
		}
	}

	return result, nil
}

func (w *WAL) walkFromLocked(startOffset uint64, limit int, visit func(Message) bool) error {
	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	seen := 0
	for _, chunk := range chunks[startIdx:] {
		if err := walkChunkMessages(w.chunkPathLocked(chunk), startOffset, limit-seen, func(msg Message) bool {
			seen++
			if visit != nil && !visit(msg) {
				return false
			}
			return limit <= 0 || seen < limit
		}); err != nil {
			return err
		}
		if limit > 0 && seen >= limit {
			return nil
		}
	}
	return nil
}

// ReadBatchesFrom reads batch envelopes from the WAL starting at startOffset.
// Unlike ReadFrom, it preserves batch boundaries and producer metadata.
func (w *WAL) ReadBatchesFrom(startOffset uint64) ([]Batch, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.readBatchesFromLocked(startOffset)
}

// ReadBatchesFromLocked is like ReadBatchesFrom but assumes the caller already
// holds the partition-level lock.
func (w *WAL) ReadBatchesFromLocked(startOffset uint64) ([]Batch, error) {
	return w.readBatchesFromLocked(startOffset)
}

func (w *WAL) readBatchesFromLocked(startOffset uint64) ([]Batch, error) {
	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	var result []Batch
	for _, chunk := range chunks[startIdx:] {
		batches, err := readChunkBatches(w.chunkPathLocked(chunk), startOffset)
		if err != nil {
			return nil, err
		}
		result = append(result, batches...)
	}
	return result, nil
}

// ReadBatchMetasFrom reads batch metadata from the WAL starting at startOffset
// without materializing full message payloads.
func (w *WAL) ReadBatchMetasFrom(startOffset uint64) ([]BatchMeta, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	var result []BatchMeta
	for _, chunk := range chunks[startIdx:] {
		metas, err := readChunkBatchMetas(w.chunkPathLocked(chunk), startOffset)
		if err != nil {
			return nil, err
		}
		result = append(result, metas...)
	}
	return result, nil
}

// ReadBatchFramesFrom reads raw batch envelopes from the WAL starting at
// startOffset. It preserves batch boundaries and producer metadata without
// materializing full messages.
func (w *WAL) ReadBatchFramesFrom(startOffset uint64) ([]BatchFrame, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.readBatchFramesFromLocked(startOffset)
}

// ReadBatchFramesFromLocked is like ReadBatchFramesFrom but assumes the caller
// already holds the partition-level lock.
func (w *WAL) ReadBatchFramesFromLocked(startOffset uint64) ([]BatchFrame, error) {
	return w.readBatchFramesFromLocked(startOffset)
}

func (w *WAL) readBatchFramesFromLocked(startOffset uint64) ([]BatchFrame, error) {
	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	var result []BatchFrame
	for _, chunk := range chunks[startIdx:] {
		frames, err := readChunkBatchFrames(w.chunkPathLocked(chunk), startOffset)
		if err != nil {
			return nil, err
		}
		result = append(result, frames...)
	}
	return result, nil
}

// readChunkBatches reads batch envelopes from a single chunk file, returning
// only batches that contain at least one message at or above startOffset.
func readChunkBatches(path string, startOffset uint64) ([]Batch, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat WAL chunk %q: %w", path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open WAL chunk %q: %w", path, err)
	}
	defer f.Close()

	var batches []Batch
	_, _, _, err = scanWALBatches(f, info.Size(), 0, func(b Batch) bool {
		// Include batch if its last message offset >= startOffset.
		if len(b.Messages) > 0 && b.Messages[len(b.Messages)-1].Offset >= startOffset {
			batches = append(batches, b)
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("read WAL chunk batches %q: %w", path, err)
	}
	return batches, nil
}

func readChunkBatchMetas(path string, startOffset uint64) ([]BatchMeta, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat WAL chunk %q: %w", path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open WAL chunk %q: %w", path, err)
	}
	defer f.Close()

	var metas []BatchMeta
	_, _, _, err = scanWALBatchMetas(f, info.Size(), 0, func(meta BatchMeta) bool {
		if meta.MessageCount > 0 && meta.LastOffset >= startOffset {
			metas = append(metas, meta)
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("read WAL chunk batch metas %q: %w", path, err)
	}
	return metas, nil
}

func readChunkBatchFrames(path string, startOffset uint64) ([]BatchFrame, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat WAL chunk %q: %w", path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open WAL chunk %q: %w", path, err)
	}
	defer f.Close()

	var frames []BatchFrame
	reader := io.NewSectionReader(f, 0, info.Size())
	var pos int64

	for {
		var envLenBuf [4]byte
		if _, err := io.ReadFull(reader, envLenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read WAL batch frame length %q: %w", path, err)
		}
		envLen := binary.BigEndian.Uint32(envLenBuf[:])
		if int64(envLen) > info.Size()-pos-4 {
			break
		}

		envBytes := make([]byte, envLen)
		if _, err := io.ReadFull(reader, envBytes); err != nil {
			break
		}
		if len(envBytes) < 4 {
			break
		}

		payload := envBytes[:len(envBytes)-4]
		storedCRC := binary.BigEndian.Uint32(envBytes[len(envBytes)-4:])
		if crc32.ChecksumIEEE(payload) != storedCRC {
			break
		}

		if len(payload) < 1 {
			break
		}
		version := payload[0]
		if version != walEnvelopeVersion {
			break
		}
		rest := payload[1:]

		var meta BatchMeta
		var ok bool
		meta.ProducerID, rest, ok = readUint64Bytes(rest)
		if !ok {
			break
		}
		meta.Sequence, rest, ok = readUint64Bytes(rest)
		if !ok {
			break
		}
		msgCount, rest, ok := readUint32Bytes(rest)
		if !ok {
			break
		}
		meta.MessageCount = int(msgCount)

		valid := true
		for i := uint32(0); i < msgCount; i++ {
			frameLen, next, ok := readUint32Bytes(rest)
			if !ok || uint32(len(next)) < frameLen+4 {
				valid = false
				break
			}
			frameBytes := next[:frameLen]
			frameCRC := binary.BigEndian.Uint32(next[frameLen : frameLen+4])
			if crc32.ChecksumIEEE(frameBytes) != frameCRC {
				valid = false
				break
			}
			offset, err := readMessageFrameOffsetBytes(frameBytes)
			if err != nil {
				valid = false
				break
			}
			if i == 0 {
				meta.FirstOffset = offset
			}
			meta.LastOffset = offset
			rest = next[frameLen+4:]
		}
		if !valid {
			break
		}

		if meta.MessageCount > 0 && meta.LastOffset >= startOffset {
			raw := make([]byte, 0, 4+len(envBytes))
			raw = binary.BigEndian.AppendUint32(raw, envLen)
			raw = append(raw, envBytes...)
			frames = append(frames, BatchFrame{Data: raw, Meta: meta})
		}
		pos += 4 + int64(envLen)
	}

	return frames, nil
}
