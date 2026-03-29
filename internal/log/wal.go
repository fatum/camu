package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	walChunkExt         = ".chunk"
	walFlushedChunkExt  = ".flushed" + walChunkExt
	walActiveChunkName  = "active" + walChunkExt
	defaultWALChunkSize = 64 * 1024 * 1024

	walEnvelopeVersion byte = 1
)

type walChunk struct {
	baseOffset uint64
	maxOffset  uint64
	size       int64
	active     bool
	flushed    bool
}

// WAL is a chunked write-ahead log stored under a sidecar directory.
// Each chunk file contains many framed WAL entries:
//
//	[4-byte entry length (uint32 big-endian)]
//	[message frame bytes]
//	[4-byte CRC32 over the message frame bytes]
type WAL struct {
	mu        sync.RWMutex
	path      string
	dir        string
	fsync      bool
	chunkSize  int64
	chunks     []walChunk // cached in-memory chunk list; reads do not rescan the directory
	activeFile *os.File   // open handle to the active chunk; avoids open/close per write
}

// OpenWAL opens (or creates) the chunked WAL rooted at path.
func OpenWAL(path string, fsync bool, chunkSize int64) (*WAL, error) {
	dir := walChunkDir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("open WAL dir %q: %w", dir, err)
	}
	if chunkSize <= 0 {
		chunkSize = defaultWALChunkSize
	}

	chunks, err := scanChunkMetas(dir)
	if err != nil {
		return nil, err
	}

	return &WAL{
		path:      path,
		dir:       dir,
		fsync:     fsync,
		chunkSize: chunkSize,
		chunks:    chunks,
	}, nil
}

func walChunkDir(path string) string {
	return path + ".segments"
}

func walChunkPath(dir string, baseOffset uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", baseOffset, walChunkExt))
}

func walFlushedChunkPath(dir string, baseOffset uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", baseOffset, walFlushedChunkExt))
}

func walActiveChunkPath(dir string) string {
	return filepath.Join(dir, walActiveChunkName)
}

func serializeWALEntry(msg Message) ([]byte, error) {
	var frameBuf bytes.Buffer
	if err := writeMessageFrame(&frameBuf, msg); err != nil {
		return nil, fmt.Errorf("WAL serialize message: %w", err)
	}
	frameBytes := frameBuf.Bytes()

	entry := make([]byte, 0, 4+len(frameBytes)+4)
	entry = binary.BigEndian.AppendUint32(entry, uint32(len(frameBytes)))
	entry = append(entry, frameBytes...)
	entry = binary.BigEndian.AppendUint32(entry, crc32.ChecksumIEEE(frameBytes))
	return entry, nil
}

// serializeWALBatch serializes a Batch into the envelope wire format:
//
//	[4B total_envelope_length]
//	[1B envelope_version]
//	[8B producer_id]
//	[8B sequence]
//	[4B message_count]
//	[per message: [4B frame_length][frame_bytes][4B CRC32]]
//	[4B CRC32 over entire envelope payload (everything after total_envelope_length)]
func serializeWALBatch(batch Batch) ([]byte, error) {
	// Serialize all message entries first.
	var entriesBuf bytes.Buffer
	for _, msg := range batch.Messages {
		entry, err := serializeWALEntry(msg)
		if err != nil {
			return nil, err
		}
		if _, err := entriesBuf.Write(entry); err != nil {
			return nil, fmt.Errorf("buffer WAL entry: %w", err)
		}
	}

	// envelope payload = version(1) + producer_id(8) + sequence(8) + msg_count(4) + entries
	payloadLen := 1 + 8 + 8 + 4 + entriesBuf.Len()
	// total on disk = total_length(4) + payload + crc(4)
	totalLen := 4 + payloadLen + 4

	out := make([]byte, 0, totalLen)
	out = binary.BigEndian.AppendUint32(out, uint32(payloadLen+4)) // total_envelope_length covers payload + envelope CRC
	out = append(out, walEnvelopeVersion)
	out = binary.BigEndian.AppendUint64(out, batch.ProducerID)
	out = binary.BigEndian.AppendUint64(out, batch.Sequence)
	out = binary.BigEndian.AppendUint32(out, uint32(len(batch.Messages)))
	out = append(out, entriesBuf.Bytes()...)

	// CRC covers everything after total_envelope_length (i.e. payload bytes).
	crc := crc32.ChecksumIEEE(out[4:])
	out = binary.BigEndian.AppendUint32(out, crc)
	return out, nil
}

// serializeWALEntries wraps messages in a zero-producer batch envelope for
// callers that don't have batch metadata (e.g. TruncateBefore rewrite).
func serializeWALEntries(msgs []Message) ([]byte, error) {
	return serializeWALBatch(Batch{Messages: msgs})
}

func parseChunkBase(name string) (uint64, bool) {
	if name == walActiveChunkName {
		return 0, false
	}
	for _, suffix := range []string{walFlushedChunkExt, walChunkExt} {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		base := strings.TrimSuffix(name, suffix)
		offset, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			return 0, false
		}
		return offset, true
	}
	return 0, false
}

func scanChunkMetas(dir string) ([]walChunk, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list WAL dir %q: %w", dir, err)
	}

	bases := make([]uint64, 0, len(entries))
	var active *walChunk
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if entry.Name() == walActiveChunkName {
			chunk, keep, err := scanChunkMeta(walActiveChunkPath(dir), 0, true)
			if err != nil {
				return nil, err
			}
			if keep {
				ch := chunk
				active = &ch
			}
			continue
		}
		if base, ok := parseChunkBase(entry.Name()); ok {
			bases = append(bases, base)
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	chunks := make([]walChunk, 0, len(bases))
	for _, base := range bases {
		path := walChunkPath(dir, base)
		flushed := false
		if _, err := os.Stat(path); err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("stat WAL chunk %q: %w", path, err)
			}
			path = walFlushedChunkPath(dir, base)
			flushed = true
		}
		chunk, keep, err := scanChunkMeta(path, base, false)
		if err != nil {
			return nil, err
		}
		if keep {
			chunk.flushed = flushed
			chunks = append(chunks, chunk)
		}
	}
	if active != nil {
		chunks = append(chunks, *active)
	}
	return chunks, nil
}

func scanChunkMeta(path string, baseOffset uint64, active bool) (walChunk, bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return walChunk{}, false, nil
		}
		return walChunk{}, false, fmt.Errorf("open WAL chunk %q: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return walChunk{}, false, fmt.Errorf("stat WAL chunk %q: %w", path, err)
	}

	validEnd, lastOffset, hasMessages, err := scanWALEntries(f, info.Size(), 0, func(Message) bool { return true })
	if err != nil {
		return walChunk{}, false, fmt.Errorf("scan WAL chunk %q: %w", path, err)
	}
	if validEnd != info.Size() {
		if err := f.Truncate(validEnd); err != nil {
			return walChunk{}, false, fmt.Errorf("truncate WAL chunk %q: %w", path, err)
		}
		infoSize := validEnd
		if infoSize == 0 {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return walChunk{}, false, fmt.Errorf("remove empty WAL chunk %q: %w", path, err)
			}
			return walChunk{}, false, nil
		}
	}
	if !hasMessages {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return walChunk{}, false, fmt.Errorf("remove empty WAL chunk %q: %w", path, err)
		}
		return walChunk{}, false, nil
	}

	if active {
		baseOffset = 0
		if validEnd > 0 {
			firstMsgs, err := readChunkMessages(path, 0, 1)
			if err != nil {
				return walChunk{}, false, err
			}
			if len(firstMsgs) > 0 {
				baseOffset = firstMsgs[0].Offset
			}
		}
	}

	return walChunk{
		baseOffset: baseOffset,
		maxOffset:  lastOffset,
		size:       validEnd,
		active:     active,
	}, true, nil
}

// scanWALBatches reads batch envelopes from r and calls visitBatch for each
// successfully decoded batch. It returns the file position up to which bytes
// are valid, the last message offset seen, whether any messages were found, and
// any fatal error.
//
// If msgLimit > 0, scanning stops after that many total messages have been visited.
func scanWALBatches(r io.ReaderAt, size int64, msgLimit int, visitBatch func(Batch) bool) (int64, uint64, bool, error) {
	reader := io.NewSectionReader(r, 0, size)

	var (
		pos        int64
		lastOffset uint64
		hasMessage bool
		totalSeen  int
	)

	for {
		// Read total_envelope_length (4B).
		var envLen uint32
		if err := binary.Read(reader, binary.BigEndian, &envLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return pos, lastOffset, hasMessage, fmt.Errorf("WAL read envelope length: %w", err)
		}

		if int64(envLen) > size-pos-4 {
			// Not enough data for the full envelope — treat as partial write.
			break
		}

		envBytes := make([]byte, envLen)
		if _, err := io.ReadFull(reader, envBytes); err != nil {
			break
		}

		// Verify envelope CRC (last 4 bytes cover everything before them).
		if len(envBytes) < 4 {
			break
		}
		payload := envBytes[:len(envBytes)-4]
		storedCRC := binary.BigEndian.Uint32(envBytes[len(envBytes)-4:])
		if crc32.ChecksumIEEE(payload) != storedCRC {
			break
		}

		// Parse header from payload.
		pr := bytes.NewReader(payload)

		var version byte
		if err := binary.Read(pr, binary.BigEndian, &version); err != nil {
			break
		}
		if version != walEnvelopeVersion {
			break
		}

		var producerID, sequence uint64
		var msgCount uint32
		if err := binary.Read(pr, binary.BigEndian, &producerID); err != nil {
			break
		}
		if err := binary.Read(pr, binary.BigEndian, &sequence); err != nil {
			break
		}
		if err := binary.Read(pr, binary.BigEndian, &msgCount); err != nil {
			break
		}

		// Read per-message frames from the remaining payload bytes.
		msgs := make([]Message, 0, msgCount)
		valid := true
		for i := uint32(0); i < msgCount; i++ {
			var frameLen uint32
			if err := binary.Read(pr, binary.BigEndian, &frameLen); err != nil {
				valid = false
				break
			}
			frameBytes := make([]byte, frameLen)
			if _, err := io.ReadFull(pr, frameBytes); err != nil {
				valid = false
				break
			}
			var frameCRC uint32
			if err := binary.Read(pr, binary.BigEndian, &frameCRC); err != nil {
				valid = false
				break
			}
			if crc32.ChecksumIEEE(frameBytes) != frameCRC {
				valid = false
				break
			}
			msg, err := readMessageFrame(bytes.NewReader(frameBytes))
			if err != nil {
				valid = false
				break
			}
			msgs = append(msgs, msg)
		}
		if !valid {
			break
		}

		// Advance position past this complete envelope.
		pos += 4 + int64(envLen)

		if len(msgs) > 0 {
			lastOffset = msgs[len(msgs)-1].Offset
			hasMessage = true
		}

		batch := Batch{
			ProducerID: producerID,
			Sequence:   sequence,
			Messages:   msgs,
		}
		totalSeen += len(msgs)

		if visitBatch != nil && !visitBatch(batch) {
			return pos, lastOffset, hasMessage, nil
		}
		if msgLimit > 0 && totalSeen >= msgLimit {
			return pos, lastOffset, hasMessage, nil
		}
	}

	return pos, lastOffset, hasMessage, nil
}

// scanWALBatchMetas reads batch envelopes from r and calls visitMeta for each
// successfully validated batch without decoding full messages. It still checks
// envelope and frame CRCs and validates the message-frame structure.
func scanWALBatchMetas(r io.ReaderAt, size int64, msgLimit int, visitMeta func(BatchMeta) bool) (int64, uint64, bool, error) {
	reader := io.NewSectionReader(r, 0, size)

	var (
		pos        int64
		lastOffset uint64
		hasMessage bool
		totalSeen  int
	)

	for {
		var envLen uint32
		if err := binary.Read(reader, binary.BigEndian, &envLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return pos, lastOffset, hasMessage, fmt.Errorf("WAL read envelope length: %w", err)
		}

		if int64(envLen) > size-pos-4 {
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

		pr := bytes.NewReader(payload)

		var version byte
		if err := binary.Read(pr, binary.BigEndian, &version); err != nil {
			break
		}
		if version != walEnvelopeVersion {
			break
		}

		var producerID, sequence uint64
		var msgCount uint32
		if err := binary.Read(pr, binary.BigEndian, &producerID); err != nil {
			break
		}
		if err := binary.Read(pr, binary.BigEndian, &sequence); err != nil {
			break
		}
		if err := binary.Read(pr, binary.BigEndian, &msgCount); err != nil {
			break
		}

		meta := BatchMeta{
			ProducerID:   producerID,
			Sequence:     sequence,
			MessageCount: int(msgCount),
		}
		valid := true
		for i := uint32(0); i < msgCount; i++ {
			var frameLen uint32
			if err := binary.Read(pr, binary.BigEndian, &frameLen); err != nil {
				valid = false
				break
			}
			frameBytes := make([]byte, frameLen)
			if _, err := io.ReadFull(pr, frameBytes); err != nil {
				valid = false
				break
			}
			var frameCRC uint32
			if err := binary.Read(pr, binary.BigEndian, &frameCRC); err != nil {
				valid = false
				break
			}
			if crc32.ChecksumIEEE(frameBytes) != frameCRC {
				valid = false
				break
			}

			offset, err := readMessageFrameOffset(bytes.NewReader(frameBytes))
			if err != nil {
				valid = false
				break
			}
			if i == 0 {
				meta.FirstOffset = offset
			}
			meta.LastOffset = offset
		}
		if !valid {
			break
		}

		pos += 4 + int64(envLen)
		if meta.MessageCount > 0 {
			lastOffset = meta.LastOffset
			hasMessage = true
		}
		totalSeen += meta.MessageCount

		if visitMeta != nil && !visitMeta(meta) {
			return pos, lastOffset, hasMessage, nil
		}
		if msgLimit > 0 && totalSeen >= msgLimit {
			return pos, lastOffset, hasMessage, nil
		}
	}

	return pos, lastOffset, hasMessage, nil
}

// scanWALEntries reads batch envelopes from r and calls visit for each
// individual message, flattening batch boundaries. This preserves the original
// call signature so that scanChunkMeta, readChunkMessages, Replay, etc. keep
// working unchanged.
func scanWALEntries(r io.ReaderAt, size int64, limit int, visit func(Message) bool) (int64, uint64, bool, error) {
	seen := 0
	stopped := false
	pos, lastOff, has, err := scanWALBatches(r, size, limit, func(b Batch) bool {
		for _, msg := range b.Messages {
			seen++
			if visit != nil && !visit(msg) {
				stopped = true
				return false
			}
			if limit > 0 && seen >= limit {
				stopped = true
				return false
			}
		}
		return true
	})
	_ = stopped
	return pos, lastOff, has, err
}

func readChunkMessages(path string, startOffset uint64, limit int) ([]Message, error) {
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

	capHint := limit
	if capHint <= 0 {
		capHint = 16
	}
	msgs := make([]Message, 0, capHint)
	_, _, _, err = scanWALEntries(f, info.Size(), 0, func(msg Message) bool {
		if msg.Offset < startOffset {
			return true
		}
		msgs = append(msgs, msg)
		return limit <= 0 || len(msgs) < limit
	})
	if err != nil {
		return nil, fmt.Errorf("read WAL chunk %q: %w", path, err)
	}
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, nil
}

func appendChunkBytes(path string, data []byte, fsync bool) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open WAL chunk for append %q: %w", path, err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("append WAL chunk %q: %w", path, err)
	}
	if fsync {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("fsync WAL chunk %q: %w", path, err)
		}
	}
	return nil
}

// openActiveFileLocked opens the active chunk file handle if it is not already
// open. Must be called with mu held.
func (w *WAL) openActiveFileLocked() error {
	if w.activeFile != nil {
		return nil
	}
	f, err := os.OpenFile(walActiveChunkPath(w.dir), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open WAL active chunk: %w", err)
	}
	w.activeFile = f
	return nil
}

// closeActiveFileLocked closes the active chunk file handle if open.
// Must be called with mu held.
func (w *WAL) closeActiveFileLocked() error {
	if w.activeFile == nil {
		return nil
	}
	err := w.activeFile.Close()
	w.activeFile = nil
	return err
}

func (w *WAL) activeChunkLocked() *walChunk {
	for i := range w.chunks {
		if w.chunks[i].active {
			return &w.chunks[i]
		}
	}
	return nil
}

func (w *WAL) flushedChunksLocked() []walChunk {
	flushed := make([]walChunk, 0, len(w.chunks))
	for _, chunk := range w.chunks {
		if chunk.flushed {
			flushed = append(flushed, chunk)
		}
	}
	return flushed
}

func (w *WAL) snapshotChunksLocked(includeActive bool, includeFlushed bool) []walChunk {
	chunks := make([]walChunk, 0, len(w.chunks))
	for _, chunk := range w.chunks {
		if (!chunk.active || includeActive) && (!chunk.flushed || includeFlushed) {
			chunks = append(chunks, chunk)
		}
	}
	return chunks
}

func (w *WAL) sealActiveLocked() error {
	active := w.activeChunkLocked()
	if active == nil || active.size == 0 {
		return nil
	}

	// Close the open file handle before renaming the active chunk.
	if err := w.closeActiveFileLocked(); err != nil {
		return fmt.Errorf("close WAL active chunk before seal: %w", err)
	}

	oldPath := walActiveChunkPath(w.dir)
	newPath := walChunkPath(w.dir, active.baseOffset)
	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("seal WAL active chunk: %w", err)
	}
	active.active = false
	sort.Slice(w.chunks, func(i, j int) bool {
		if w.chunks[i].baseOffset == w.chunks[j].baseOffset {
			if w.chunks[i].active == w.chunks[j].active {
				return false
			}
			return !w.chunks[i].active
		}
		return w.chunks[i].baseOffset < w.chunks[j].baseOffset
	})
	return nil
}

func (w *WAL) markFlushedLocked(beforeOffset uint64) error {
	for i := range w.chunks {
		chunk := &w.chunks[i]
		if chunk.active || chunk.flushed || chunk.maxOffset >= beforeOffset {
			continue
		}
		oldPath := walChunkPath(w.dir, chunk.baseOffset)
		newPath := walFlushedChunkPath(w.dir, chunk.baseOffset)
		if err := os.Rename(oldPath, newPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("mark WAL chunk flushed: %w", err)
		}
		chunk.flushed = true
	}
	return nil
}

func rewriteChunkFile(dir, path string, data []byte, fsync bool) error {
	tmp, err := os.CreateTemp(dir, "*.tmp")
	if err != nil {
		return fmt.Errorf("create WAL temp chunk: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write WAL temp chunk: %w", err)
	}
	if fsync {
		if err := tmp.Sync(); err != nil {
			cleanup()
			return fmt.Errorf("fsync WAL temp chunk: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close WAL temp chunk: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename WAL chunk %q: %w", path, err)
	}
	return nil
}

// Append writes a single message to the WAL.
func (w *WAL) Append(msg Message) error {
	return w.AppendBatch([]Message{msg})
}

// AppendBatch appends messages to the active WAL chunk with a zero-producer
// batch envelope, rotating when the configured chunk size is reached.
func (w *WAL) AppendBatch(msgs []Message) error {
	return w.AppendBatchWithMeta(Batch{Messages: msgs})
}

// AppendBatchLocked is like AppendBatch but assumes the caller already holds
// the partition-level lock.
func (w *WAL) AppendBatchLocked(msgs []Message) error {
	return w.AppendBatchWithMetaLocked(Batch{Messages: msgs})
}

// AppendBatchWithMeta appends a batch (with producer metadata) to the active
// WAL chunk, rotating when the configured chunk size is reached. Each call
// writes exactly one batch envelope.
func (w *WAL) AppendBatchWithMeta(batch Batch) error {
	if len(batch.Messages) == 0 {
		return nil
	}

	data, err := serializeWALBatch(batch)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.appendBatchWithMetaLocked(batch, data)
}

// AppendBatchWithMetaLocked is like AppendBatchWithMeta but assumes the caller
// already holds the partition-level lock. The WAL's internal mutex is not acquired.
func (w *WAL) AppendBatchWithMetaLocked(batch Batch) error {
	if len(batch.Messages) == 0 {
		return nil
	}

	data, err := serializeWALBatch(batch)
	if err != nil {
		return err
	}

	return w.appendBatchWithMetaLocked(batch, data)
}

func (w *WAL) appendBatchWithMetaLocked(batch Batch, data []byte) error {
	firstOffset := batch.Messages[0].Offset
	lastOffset := batch.Messages[len(batch.Messages)-1].Offset

	active := w.activeChunkLocked()
	if active == nil {
		w.chunks = append(w.chunks, walChunk{
			baseOffset: firstOffset,
			maxOffset:  firstOffset,
			active:     true,
		})
		active = &w.chunks[len(w.chunks)-1]
	}
	if active.size > 0 && active.size+int64(len(data)) > w.chunkSize {
		if err := w.sealActiveLocked(); err != nil {
			return err
		}
		w.chunks = append(w.chunks, walChunk{
			baseOffset: firstOffset,
			maxOffset:  firstOffset,
			active:     true,
		})
		active = &w.chunks[len(w.chunks)-1]
	}
	if active.size == 0 {
		active.baseOffset = firstOffset
	}
	if err := w.openActiveFileLocked(); err != nil {
		return err
	}
	if _, err := w.activeFile.Write(data); err != nil {
		return fmt.Errorf("append WAL chunk: %w", err)
	}
	if w.fsync {
		if err := w.activeFile.Sync(); err != nil {
			return fmt.Errorf("fsync WAL chunk: %w", err)
		}
	}
	active.size += int64(len(data))
	active.maxOffset = lastOffset
	return nil
}

// Replay reads all valid WAL chunks in offset order.
func (w *WAL) Replay() ([]Message, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.replayLocked()
}

// ReplayLocked is like Replay but assumes the caller already holds the
// partition-level lock.
func (w *WAL) ReplayLocked() ([]Message, error) {
	return w.replayLocked()
}

func (w *WAL) replayLocked() ([]Message, error) {
	chunks := w.snapshotChunksLocked(true, false)
	msgs := make([]Message, 0, len(chunks))
	for _, chunk := range chunks {
		chunkMsgs, err := readChunkMessages(w.chunkPathLocked(chunk), chunk.baseOffset, 0)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, chunkMsgs...)
	}
	return msgs, nil
}

// Seal seals the active chunk so it becomes a flush candidate.
func (w *WAL) Seal() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.sealActiveLocked()
}

// SealLocked is like Seal but assumes the caller already holds the
// partition-level lock.
func (w *WAL) SealLocked() error {
	return w.sealActiveLocked()
}

// ReplaySealed reads only sealed WAL chunks in offset order. This is the
// normal leader flush path: seal the active chunk, flush sealed chunks, keep
// the new active chunk for subsequent unflushed writes.
func (w *WAL) ReplaySealed() ([]Message, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.replaySealedLocked()
}

// ReplaySealedLocked is like ReplaySealed but assumes the caller already holds
// the partition-level lock.
func (w *WAL) ReplaySealedLocked() ([]Message, error) {
	return w.replaySealedLocked()
}

func (w *WAL) replaySealedLocked() ([]Message, error) {
	chunks := w.snapshotChunksLocked(false, false)
	msgs := make([]Message, 0, len(chunks))
	for _, chunk := range chunks {
		chunkMsgs, err := readChunkMessages(w.chunkPathLocked(chunk), chunk.baseOffset, 0)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, chunkMsgs...)
	}
	return msgs, nil
}

// MarkFlushed marks sealed chunks strictly below beforeOffset as flushed-retained.
func (w *WAL) MarkFlushed(beforeOffset uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.markFlushedLocked(beforeOffset)
}

// MarkFlushedLocked is like MarkFlushed but assumes the caller already holds
// the partition-level lock.
func (w *WAL) MarkFlushedLocked(beforeOffset uint64) error {
	return w.markFlushedLocked(beforeOffset)
}

func (w *WAL) chunkPathLocked(chunk walChunk) string {
	if chunk.active {
		return walActiveChunkPath(w.dir)
	}
	if chunk.flushed {
		return walFlushedChunkPath(w.dir, chunk.baseOffset)
	}
	return walChunkPath(w.dir, chunk.baseOffset)
}

// TruncateBefore deletes old chunks and rewrites at most one partially covered
// chunk to preserve messages at or above the requested offset.
func (w *WAL) TruncateBefore(offset uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.truncateBeforeLocked(offset)
}

// TruncateBeforeLocked is like TruncateBefore but assumes the caller already
// holds the partition-level lock.
func (w *WAL) TruncateBeforeLocked(offset uint64) error {
	return w.truncateBeforeLocked(offset)
}

func (w *WAL) truncateBeforeLocked(offset uint64) error {
	if len(w.chunks) == 0 {
		return nil
	}

	// Close the active file handle since truncation may remove or rewrite
	// the active chunk file. It will be lazily reopened on the next append.
	if err := w.closeActiveFileLocked(); err != nil {
		return fmt.Errorf("close WAL active chunk before truncate: %w", err)
	}

	chunks := append([]walChunk(nil), w.chunks...)
	keepIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= offset })

	for _, chunk := range chunks[:keepIdx] {
		path := w.chunkPathLocked(chunk)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove WAL chunk %q: %w", path, err)
		}
	}

	if keepIdx == len(chunks) {
		w.chunks = nil
		return nil
	}

	kept := append([]walChunk(nil), chunks[keepIdx:]...)
	first := kept[0]
	if first.baseOffset < offset {
		oldPath := w.chunkPathLocked(first)
		msgs, err := readChunkMessages(oldPath, offset, 0)
		if err != nil {
			return err
		}
		if len(msgs) == 0 {
			if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove truncated WAL chunk %q: %w", oldPath, err)
			}
			kept = kept[1:]
		} else {
			data, err := serializeWALEntries(msgs)
			if err != nil {
				return err
			}
			newBase := msgs[0].Offset
			newPath := oldPath
			if !first.active {
				newPath = walChunkPath(w.dir, newBase)
			}
			if err := rewriteChunkFile(w.dir, newPath, data, w.fsync); err != nil {
				return err
			}
			if oldPath != newPath {
				if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("remove old WAL chunk %q: %w", oldPath, err)
				}
			}
			kept[0] = walChunk{
				baseOffset: newBase,
				maxOffset:  msgs[len(msgs)-1].Offset,
				size:       int64(len(data)),
				active:     first.active,
			}
		}
	}

	w.chunks = kept
	sort.Slice(w.chunks, func(i, j int) bool {
		if w.chunks[i].baseOffset == w.chunks[j].baseOffset {
			if w.chunks[i].active == w.chunks[j].active {
				return false
			}
			return !w.chunks[i].active
		}
		return w.chunks[i].baseOffset < w.chunks[j].baseOffset
	})
	return nil
}

// Close releases WAL resources including the open active chunk file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()


	return w.closeActiveFileLocked()
}
