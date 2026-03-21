package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WAL is a write-ahead log that persists messages to disk before they are
// flushed into segments. Each entry is framed with a length prefix and a
// CRC32 checksum so that partial writes caused by crashes are detected and
// silently skipped during replay.
//
// WAL entry format (on disk):
//
//	[4-byte entry length (uint32 big-endian)]
//	[message frame bytes  (same format as segment.go writeMessageFrame)]
//	[4-byte CRC32         (uint32 big-endian, over the message frame bytes)]
type WAL struct {
	mu    sync.Mutex
	file  *os.File
	fsync bool
}

// OpenWAL opens (or creates) the WAL file at path. It does NOT auto-replay;
// the caller must call Replay() explicitly when recovering after a crash.
func OpenWAL(path string, fsync bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL %q: %w", path, err)
	}
	return &WAL{file: f, fsync: fsync}, nil
}

// Append serializes msg into a framed WAL entry and writes it to disk.
// It also appends msg to the in-memory buffer for use by UnflushedFrom.
func (w *WAL) Append(msg Message) error {
	// Serialize the message frame into a buffer so we can measure its length
	// and compute a CRC before writing anything to disk.
	var frameBuf bytes.Buffer
	if err := writeMessageFrame(&frameBuf, msg); err != nil {
		return fmt.Errorf("WAL serialize message: %w", err)
	}
	frameBytes := frameBuf.Bytes()

	checksum := crc32.ChecksumIEEE(frameBytes)

	w.mu.Lock()
	defer w.mu.Unlock()

	// Write to end of file.
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("WAL seek end: %w", err)
	}

	// [4-byte entry length]
	entryLen := uint32(len(frameBytes))
	if err := binary.Write(w.file, binary.BigEndian, entryLen); err != nil {
		return fmt.Errorf("WAL write entry length: %w", err)
	}
	// [message frame bytes]
	if _, err := w.file.Write(frameBytes); err != nil {
		return fmt.Errorf("WAL write frame: %w", err)
	}
	// [4-byte CRC32]
	if err := binary.Write(w.file, binary.BigEndian, checksum); err != nil {
		return fmt.Errorf("WAL write CRC: %w", err)
	}

	if w.fsync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("WAL fsync: %w", err)
		}
	}
	return nil
}

// AppendBatch writes multiple messages to the WAL with a single fsync.
// This is much more efficient than calling Append in a loop for batch produces.
func (w *WAL) AppendBatch(msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	// Pre-serialize all entries before taking the lock.
	type entry struct {
		frameBytes []byte
		checksum   uint32
	}
	entries := make([]entry, len(msgs))
	for i, msg := range msgs {
		var frameBuf bytes.Buffer
		if err := writeMessageFrame(&frameBuf, msg); err != nil {
			return fmt.Errorf("WAL serialize message %d: %w", i, err)
		}
		fb := frameBuf.Bytes()
		entries[i] = entry{
			frameBytes: fb,
			checksum:   crc32.ChecksumIEEE(fb),
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("WAL seek end: %w", err)
	}

	for _, e := range entries {
		entryLen := uint32(len(e.frameBytes))
		if err := binary.Write(w.file, binary.BigEndian, entryLen); err != nil {
			return fmt.Errorf("WAL write entry length: %w", err)
		}
		if _, err := w.file.Write(e.frameBytes); err != nil {
			return fmt.Errorf("WAL write frame: %w", err)
		}
		if err := binary.Write(w.file, binary.BigEndian, e.checksum); err != nil {
			return fmt.Errorf("WAL write CRC: %w", err)
		}
	}

	if w.fsync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("WAL fsync: %w", err)
		}
	}

	return nil
}

// Replay reads all valid entries from the beginning of the WAL file.
func (w *WAL) Replay() ([]Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.replayLocked()
}

// replayLocked reads all valid entries from the file. Caller must hold w.mu.
func (w *WAL) replayLocked() ([]Message, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("WAL seek start: %w", err)
	}

	var msgs []Message
	for {
		var entryLen uint32
		if err := binary.Read(w.file, binary.BigEndian, &entryLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("WAL read entry length: %w", err)
		}

		frameBytes := make([]byte, entryLen)
		if _, err := io.ReadFull(w.file, frameBytes); err != nil {
			break
		}

		var storedCRC uint32
		if err := binary.Read(w.file, binary.BigEndian, &storedCRC); err != nil {
			break
		}

		if crc32.ChecksumIEEE(frameBytes) != storedCRC {
			break
		}

		msg, err := readMessageFrame(bytes.NewReader(frameBytes))
		if err != nil {
			break
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// TruncateBefore rewrites the WAL keeping only messages with Offset >= offset.
// It writes kept entries to a temp file, then atomically renames it over the
// original. The in-memory buffer is also trimmed accordingly.
func (w *WAL) TruncateBefore(offset uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Replay WAL from disk to find entries to keep.
	keep, err := w.replayLocked()
	if err != nil {
		return fmt.Errorf("WAL truncate replay: %w", err)
	}
	// Filter to keep only messages >= offset.
	filtered := keep[:0]
	for _, m := range keep {
		if m.Offset >= offset {
			filtered = append(filtered, m)
		}
	}
	keep = filtered

	// Build the new WAL file content in memory, then write atomically.
	tmpPath := w.file.Name() + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("WAL truncate create temp: %w", err)
	}

	for _, msg := range keep {
		var frameBuf bytes.Buffer
		if err := writeMessageFrame(&frameBuf, msg); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("WAL truncate serialize: %w", err)
		}
		frameBytes := frameBuf.Bytes()
		checksum := crc32.ChecksumIEEE(frameBytes)

		if err := binary.Write(tmp, binary.BigEndian, uint32(len(frameBytes))); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("WAL truncate write length: %w", err)
		}
		if _, err := tmp.Write(frameBytes); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("WAL truncate write frame: %w", err)
		}
		if err := binary.Write(tmp, binary.BigEndian, checksum); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("WAL truncate write CRC: %w", err)
		}
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("WAL truncate sync temp: %w", err)
	}
	tmp.Close()

	originalPath := w.file.Name()

	// Close current file before rename (required on Windows; harmless on Unix)
	w.file.Close()

	if err := os.Rename(tmpPath, originalPath); err != nil {
		return fmt.Errorf("WAL truncate rename: %w", err)
	}

	// Reopen the file so subsequent Append calls work correctly.
	f, err := os.OpenFile(originalPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("WAL truncate reopen: %w", err)
	}
	w.file = f
	return nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
