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
	appendMu  sync.Mutex
	path      string
	dir       string
	fsync     bool
	chunkSize int64
	chunks    []walChunk // cached in-memory chunk list; reads do not rescan the directory
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

func serializeWALEntries(msgs []Message) ([]byte, error) {
	var buf bytes.Buffer
	for _, msg := range msgs {
		entry, err := serializeWALEntry(msg)
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(entry); err != nil {
			return nil, fmt.Errorf("buffer WAL entry: %w", err)
		}
	}
	return buf.Bytes(), nil
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

func scanWALEntries(r io.ReaderAt, size int64, limit int, visit func(Message) bool) (int64, uint64, bool, error) {
	reader := io.NewSectionReader(r, 0, size)

	var (
		pos        int64
		lastOffset uint64
		hasMessage bool
		seen       int
	)

	for {
		var entryLen uint32
		if err := binary.Read(reader, binary.BigEndian, &entryLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return pos, lastOffset, hasMessage, fmt.Errorf("WAL read entry length: %w", err)
		}
		pos += 4

		frameBytes := make([]byte, entryLen)
		if _, err := io.ReadFull(reader, frameBytes); err != nil {
			pos -= 4
			break
		}
		pos += int64(entryLen)

		var storedCRC uint32
		if err := binary.Read(reader, binary.BigEndian, &storedCRC); err != nil {
			pos -= 4 + int64(entryLen)
			break
		}
		pos += 4

		if crc32.ChecksumIEEE(frameBytes) != storedCRC {
			pos -= 4 + int64(entryLen) + 4
			break
		}

		msg, err := readMessageFrame(bytes.NewReader(frameBytes))
		if err != nil {
			pos -= 4 + int64(entryLen) + 4
			break
		}

		lastOffset = msg.Offset
		hasMessage = true
		seen++
		if visit != nil && !visit(msg) {
			return pos, lastOffset, hasMessage, nil
		}
		if limit > 0 && seen >= limit {
			return pos, lastOffset, hasMessage, nil
		}
	}

	return pos, lastOffset, hasMessage, nil
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

// AppendBatch appends messages to the active WAL chunk, rotating when the
// configured chunk size is reached.
func (w *WAL) AppendBatch(msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.appendMu.Lock()
	defer w.appendMu.Unlock()

	activePath := walActiveChunkPath(w.dir)
	for _, msg := range msgs {
		entry, err := serializeWALEntry(msg)
		if err != nil {
			return err
		}
		active := w.activeChunkLocked()
		if active == nil {
			w.chunks = append(w.chunks, walChunk{
				baseOffset: msg.Offset,
				maxOffset:  msg.Offset,
				active:     true,
			})
			active = &w.chunks[len(w.chunks)-1]
		}
		if active.size > 0 && active.size+int64(len(entry)) > w.chunkSize {
			if err := w.sealActiveLocked(); err != nil {
				return err
			}
			w.chunks = append(w.chunks, walChunk{
				baseOffset: msg.Offset,
				maxOffset:  msg.Offset,
				active:     true,
			})
			active = &w.chunks[len(w.chunks)-1]
		}
		if active.size == 0 {
			active.baseOffset = msg.Offset
		}
		if err := appendChunkBytes(activePath, entry, w.fsync); err != nil {
			return err
		}
		active.size += int64(len(entry))
		active.maxOffset = msg.Offset
	}
	return nil
}

// Replay reads all valid WAL chunks in offset order.
func (w *WAL) Replay() ([]Message, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

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
	w.appendMu.Lock()
	defer w.appendMu.Unlock()

	return w.sealActiveLocked()
}

// ReplaySealed reads only sealed WAL chunks in offset order. This is the
// normal leader flush path: seal the active chunk, flush sealed chunks, keep
// the new active chunk for subsequent unflushed writes.
func (w *WAL) ReplaySealed() ([]Message, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

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
	w.appendMu.Lock()
	defer w.appendMu.Unlock()

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
	w.appendMu.Lock()
	defer w.appendMu.Unlock()

	if len(w.chunks) == 0 {
		return nil
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

// Close releases WAL resources. Chunked WAL has no long-lived file handles.
func (w *WAL) Close() error {
	return nil
}
