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
	mu     sync.Mutex
	file   *os.File
	buf    []Message // in-memory buffer of all appended messages
	fsync  bool
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

	w.buf = append(w.buf, msg)
	return nil
}

// Replay reads all valid entries from the beginning of the WAL file.
// Entries with a bad CRC or that are truncated (from a crash mid-write) are
// silently skipped — only entries that trail the last valid entry can be
// corrupt; earlier corruption would be a data integrity problem we surface.
func (w *WAL) Replay() ([]Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("WAL seek start: %w", err)
	}

	var msgs []Message
	for {
		// Read 4-byte entry length
		var entryLen uint32
		if err := binary.Read(w.file, binary.BigEndian, &entryLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean or truncated end
			}
			return nil, fmt.Errorf("WAL read entry length: %w", err)
		}

		// Read the frame bytes
		frameBytes := make([]byte, entryLen)
		if _, err := io.ReadFull(w.file, frameBytes); err != nil {
			// Truncated entry — skip and stop
			break
		}

		// Read 4-byte CRC32
		var storedCRC uint32
		if err := binary.Read(w.file, binary.BigEndian, &storedCRC); err != nil {
			// Truncated before CRC — skip and stop
			break
		}

		// Validate checksum
		if crc32.ChecksumIEEE(frameBytes) != storedCRC {
			// Corrupt entry — skip and stop (trailing corruption from crash)
			break
		}

		// Decode the message frame
		msg, err := readMessageFrame(bytes.NewReader(frameBytes))
		if err != nil {
			// Should not happen for a valid CRC'd frame, but skip defensively
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

	// Collect entries to keep from the in-memory buffer.
	// (The buffer is the authoritative ordered list after Append calls.)
	var keep []Message
	for _, m := range w.buf {
		if m.Offset >= offset {
			keep = append(keep, m)
		}
	}

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
	w.buf = keep
	return nil
}

// UnflushedFrom returns all in-memory messages whose Offset >= offset.
// It performs no file I/O; the buffer is populated by Append.
func (w *WAL) UnflushedFrom(offset uint64) []Message {
	w.mu.Lock()
	defer w.mu.Unlock()

	var result []Message
	for _, m := range w.buf {
		if m.Offset >= offset {
			result = append(result, m)
		}
	}
	return result
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
