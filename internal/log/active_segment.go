package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// ActiveSegment is the writable tail of a partition log. Raw RecordBatch bytes
// are appended sequentially to a single file; in-memory indices are maintained
// for fast offset and timestamp lookups.
type ActiveSegment struct {
	mu         sync.Mutex
	file       *os.File
	dir        string
	baseOffset int64
	size       int64
	offsetIdx  []IndexEntry
	timeIdx    []TimestampIndexEntry
	largestTS  int64 // largest MaxTimestamp seen (for monotonic timeIdx)
}

// SegmentFilename returns the canonical log filename for the given base offset,
// e.g. "00000000000000000000.log".
func SegmentFilename(baseOffset int64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

// SidecarFilename returns the canonical index filename for the given base offset,
// e.g. "00000000000000000000.index".
func SidecarFilename(baseOffset int64) string {
	return fmt.Sprintf("%020d.index", baseOffset)
}

// OpenActiveSegment opens (or creates) the segment log file rooted at dir with
// the given baseOffset. The directory is created if it does not exist.
func OpenActiveSegment(dir string, baseOffset int64) (*ActiveSegment, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("active_segment: mkdir %s: %w", dir, err)
	}

	path := filepath.Join(dir, SegmentFilename(baseOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("active_segment: open %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("active_segment: stat %s: %w", path, err)
	}

	return &ActiveSegment{
		file:       f,
		dir:        dir,
		baseOffset: baseOffset,
		size:       info.Size(),
	}, nil
}

// Append writes a raw RecordBatch to the segment file and updates in-memory
// indices. The batch must be a valid Kafka v2 RecordBatch.
func (s *ActiveSegment) Append(batch []byte) error {
	hdr, err := ReadRecordBatchHeader(batch)
	if err != nil {
		return fmt.Errorf("active_segment: append: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	position := s.size

	if _, err := s.file.Write(batch); err != nil {
		return fmt.Errorf("active_segment: write: %w", err)
	}

	s.size += int64(len(batch))

	s.offsetIdx = append(s.offsetIdx, IndexEntry{
		BaseOffset:     hdr.FirstOffset,
		LastOffset:     hdr.LastOffset(),
		Position:       position,
		BatchSize:      int32(len(batch)),
		FirstTimestamp: hdr.FirstTimestamp,
		MaxTimestamp:   hdr.MaxTimestamp,
	})

	// Only advance the timestamp index when MaxTimestamp is strictly greater
	// than the last recorded timestamp (monotonicity guarantee).
	if hdr.MaxTimestamp > s.largestTS {
		s.timeIdx = append(s.timeIdx, TimestampIndexEntry{
			Timestamp:  hdr.MaxTimestamp,
			BaseOffset: hdr.FirstOffset,
		})
		s.largestTS = hdr.MaxTimestamp
	}

	return nil
}

// AppendFromReader appends a single RecordBatch whose header has already been
// parsed and whose body (batch[HeaderSize:]) is streamed from body. This
// avoids materializing the full batch in memory: only the 61-byte header is
// buffered, the body flows directly from the reader to the file via io.Copy.
// bodySize is the number of bytes after the header (total batch size minus
// RecordBatchHeaderSize).
func (s *ActiveSegment) AppendFromReader(hdr RecordBatchHeader, headerBytes []byte, body io.Reader, bodySize int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	position := s.size

	if _, err := s.file.Write(headerBytes); err != nil {
		s.size = position
		_ = s.file.Truncate(position)
		_, _ = s.file.Seek(position, io.SeekStart)
		return fmt.Errorf("active_segment: write header: %w", err)
	}
	s.size += int64(len(headerBytes))

	if bodySize > 0 {
		n, err := io.Copy(s.file, io.LimitReader(body, bodySize))
		if err != nil || n != bodySize {
			s.size = position
			_ = s.file.Truncate(position)
			_, _ = s.file.Seek(position, io.SeekStart)
			if err != nil {
				return fmt.Errorf("active_segment: stream body: %w", err)
			}
			return fmt.Errorf("active_segment: short body stream: wrote %d of %d", n, bodySize)
		}
		s.size += bodySize
	}

	totalSize := int32(len(headerBytes)) + int32(bodySize)
	s.offsetIdx = append(s.offsetIdx, IndexEntry{
		BaseOffset:     hdr.FirstOffset,
		LastOffset:     hdr.LastOffset(),
		Position:       position,
		BatchSize:      totalSize,
		FirstTimestamp: hdr.FirstTimestamp,
		MaxTimestamp:   hdr.MaxTimestamp,
	})

	if hdr.MaxTimestamp > s.largestTS {
		s.timeIdx = append(s.timeIdx, TimestampIndexEntry{
			Timestamp:  hdr.MaxTimestamp,
			BaseOffset: hdr.FirstOffset,
		})
		s.largestTS = hdr.MaxTimestamp
	}

	return nil
}

// CompactThrough drops complete RecordBatches through offset and returns a new
// active segment containing only the remaining tail. Callers must use this
// only after the dropped range is durable elsewhere. It never publishes data.
func (s *ActiveSegment) CompactThrough(offset int64) (*ActiveSegment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	retain := 0
	for retain < len(s.offsetIdx) && s.offsetIdx[retain].LastOffset <= offset {
		retain++
	}
	if retain == 0 {
		return s, false, nil
	}

	nextBase := s.baseOffset
	start := s.size
	if retain < len(s.offsetIdx) {
		nextBase = s.offsetIdx[retain].BaseOffset
		start = s.offsetIdx[retain].Position
	} else if len(s.offsetIdx) > 0 {
		nextBase = s.offsetIdx[len(s.offsetIdx)-1].LastOffset + 1
	}

	temporary, err := os.CreateTemp(s.dir, ".compact-*.log")
	if err != nil {
		return s, false, fmt.Errorf("active_segment: create compact file: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanupTemporary := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if start < s.size {
		if _, err := io.Copy(temporary, io.NewSectionReader(s.file, start, s.size-start)); err != nil {
			cleanupTemporary()
			return s, false, fmt.Errorf("active_segment: copy compact tail: %w", err)
		}
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return s, false, fmt.Errorf("active_segment: close compact file: %w", err)
	}

	newPath := filepath.Join(s.dir, SegmentFilename(nextBase))
	if err := os.Rename(temporaryPath, newPath); err != nil {
		_ = os.Remove(temporaryPath)
		return s, false, fmt.Errorf("active_segment: install compact file: %w", err)
	}
	oldPath := filepath.Join(s.dir, SegmentFilename(s.baseOffset))
	if err := s.file.Close(); err != nil {
		return s, false, fmt.Errorf("active_segment: close compact source: %w", err)
	}
	if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
		return s, false, fmt.Errorf("active_segment: remove compact source: %w", err)
	}

	compacted, err := OpenActiveSegment(s.dir, nextBase)
	if err != nil {
		return s, false, err
	}
	if err := compacted.Recover(); err != nil {
		_ = compacted.Close()
		return s, false, fmt.Errorf("active_segment: recover compact tail: %w", err)
	}
	return compacted, true, nil
}

// ReadAt reads len(buf) bytes from the segment file starting at byte offset off.
// It delegates directly to os.File.ReadAt, which is safe for concurrent use.
func (s *ActiveSegment) ReadAt(buf []byte, off int64) (int, error) {
	return s.file.ReadAt(buf, off)
}

// NextOffset returns the offset of the next record to be appended. If the
// segment is empty it returns baseOffset.
func (s *ActiveSegment) NextOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.offsetIdx) == 0 {
		return s.baseOffset
	}
	return s.offsetIdx[len(s.offsetIdx)-1].LastOffset + 1
}

// Size returns the current byte size of the segment file.
func (s *ActiveSegment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// BaseOffset returns the base offset of the segment (immutable).
func (s *ActiveSegment) BaseOffset() int64 {
	return s.baseOffset
}

// Dir returns the directory that contains the segment file (immutable).
func (s *ActiveSegment) Dir() string {
	return s.dir
}

// OffsetIndex returns a copy of the in-memory offset index.
func (s *ActiveSegment) OffsetIndex() []IndexEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]IndexEntry, len(s.offsetIdx))
	copy(cp, s.offsetIdx)
	return cp
}

// TimestampIndex returns a copy of the in-memory timestamp index.
func (s *ActiveSegment) TimestampIndex() []TimestampIndexEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]TimestampIndexEntry, len(s.timeIdx))
	copy(cp, s.timeIdx)
	return cp
}

// LookupOffset returns the IndexEntry whose batch contains the given offset.
// Returns false if the offset is not covered by any batch in the segment.
func (s *ActiveSegment) LookupOffset(offset int64) (IndexEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.offsetIdx) == 0 {
		return IndexEntry{}, false
	}

	// Find the largest BaseOffset <= offset.
	i := sort.Search(len(s.offsetIdx), func(i int) bool { return s.offsetIdx[i].BaseOffset > offset }) - 1
	if i < 0 {
		return IndexEntry{}, false
	}

	entry := s.offsetIdx[i]
	if offset > entry.LastOffset {
		return IndexEntry{}, false
	}

	return entry, true
}

// LookupTimestamp returns the IndexEntry for the first batch whose MaxTimestamp
// is >= target. Returns false if target is beyond all timestamps in the segment.
func (s *ActiveSegment) LookupTimestamp(target int64) (IndexEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.timeIdx) == 0 {
		return IndexEntry{}, false
	}

	// Find the first timestamp index entry with Timestamp >= target.
	i := sort.Search(len(s.timeIdx), func(i int) bool { return s.timeIdx[i].Timestamp >= target })
	if i >= len(s.timeIdx) {
		return IndexEntry{}, false
	}

	// Map the TimestampIndexEntry.BaseOffset back to the offset index.
	baseOffset := s.timeIdx[i].BaseOffset
	j := sort.Search(len(s.offsetIdx), func(j int) bool { return s.offsetIdx[j].BaseOffset > baseOffset }) - 1
	if j < 0 {
		return IndexEntry{}, false
	}

	return s.offsetIdx[j], true
}

// Close flushes and closes the underlying file.
func (s *ActiveSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

// Path returns the absolute path of the segment log file.
func (s *ActiveSegment) Path() string {
	return filepath.Join(s.dir, SegmentFilename(s.baseOffset))
}

// File returns the underlying segment file. Callers may use it for zero-copy
// operations such as sendfile (via io.Copy with io.NewSectionReader). The
// file remains valid until Close is called.
func (s *ActiveSegment) File() *os.File {
	return s.file
}

// WithOffsetIndex calls fn with the live offset index slice while holding a
// read lock. This avoids the copy that OffsetIndex() performs. The slice must
// not be retained after fn returns.
func (s *ActiveSegment) WithOffsetIndex(fn func([]IndexEntry)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.offsetIdx)
}

// SidecarPath returns the absolute path of the segment index (sidecar) file.
func (s *ActiveSegment) SidecarPath() string {
	return filepath.Join(s.dir, SidecarFilename(s.baseOffset))
}

// Seal syncs and closes the segment file, then writes the in-memory indices to
// the sidecar file. It returns the absolute paths of both files.
// Seal must not be called concurrently with any other method.
func (s *ActiveSegment) Seal() (segmentPath, sidecarPath string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Sync to disk.
	if err = s.file.Sync(); err != nil {
		return "", "", fmt.Errorf("active_segment: seal: sync: %w", err)
	}

	// 2. Close the segment file.
	if err = s.file.Close(); err != nil {
		return "", "", fmt.Errorf("active_segment: seal: close: %w", err)
	}

	// 3 & 4. Write sidecar file from in-memory indices.
	sp := s.SidecarPath()
	f, err := os.Create(sp)
	if err != nil {
		return "", "", fmt.Errorf("active_segment: seal: create sidecar %s: %w", sp, err)
	}
	if werr := WriteSidecar(f, s.offsetIdx, s.timeIdx); werr != nil {
		f.Close()
		return "", "", fmt.Errorf("active_segment: seal: write sidecar: %w", werr)
	}
	if err = f.Close(); err != nil {
		return "", "", fmt.Errorf("active_segment: seal: close sidecar: %w", err)
	}

	// 5. Return both paths.
	return s.Path(), sp, nil
}

// Recover scans the segment file from the beginning, rebuilding in-memory
// indices from valid batches and truncating any trailing corrupt or partial data.
// It follows the same model as Kafka's Log.recoverLog().
func (s *ActiveSegment) Recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Get current file size.
	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("active_segment: recover: stat: %w", err)
	}
	fileSize := info.Size()

	// 2. Clear existing in-memory indices.
	s.offsetIdx = s.offsetIdx[:0]
	s.timeIdx = s.timeIdx[:0]
	s.largestTS = 0

	var position int64
	hdrBuf := make([]byte, RecordBatchHeaderSize)

	// 3. Scan from position 0.
	for {
		// a. If not enough bytes remain for a header, stop.
		if position+RecordBatchHeaderSize > fileSize {
			break
		}

		// b. Read the 61-byte header.
		if _, err := s.file.ReadAt(hdrBuf, position); err != nil {
			break
		}

		hdr, err := ReadRecordBatchHeader(hdrBuf)
		if err != nil {
			break
		}

		// c. Compute full batch size.
		batchSize := int64(hdr.RecordBatchSize())

		// d. Sanity checks.
		if batchSize < RecordBatchHeaderSize || position+batchSize > fileSize {
			break
		}

		// e. Read the full batch and validate CRC.
		batchBuf := make([]byte, batchSize)
		if _, err := s.file.ReadAt(batchBuf, position); err != nil {
			break
		}
		if err := ValidateRecordBatchCRC(batchBuf); err != nil {
			break
		}

		// f. Update in-memory indices (same logic as Append).
		s.offsetIdx = append(s.offsetIdx, IndexEntry{
			BaseOffset:     hdr.FirstOffset,
			LastOffset:     hdr.LastOffset(),
			Position:       position,
			BatchSize:      int32(batchSize),
			FirstTimestamp: hdr.FirstTimestamp,
			MaxTimestamp:   hdr.MaxTimestamp,
		})
		if hdr.MaxTimestamp > s.largestTS {
			s.timeIdx = append(s.timeIdx, TimestampIndexEntry{
				Timestamp:  hdr.MaxTimestamp,
				BaseOffset: hdr.FirstOffset,
			})
			s.largestTS = hdr.MaxTimestamp
		}

		position += batchSize
	}

	// 4. Truncate the file after the last valid batch.
	if err := s.file.Truncate(position); err != nil {
		return fmt.Errorf("active_segment: recover: truncate: %w", err)
	}

	// 5. Update tracked size.
	s.size = position

	return nil
}

// TruncateFrom removes all batches at and above offset from the active
// segment, preserving the earlier prefix and rebuilding the in-memory indexes.
func (s *ActiveSegment) TruncateFrom(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.offsetIdx) == 0 {
		return nil
	}

	truncatePos := s.size
	keepCount := len(s.offsetIdx)
	for i, entry := range s.offsetIdx {
		if entry.BaseOffset >= offset {
			truncatePos = entry.Position
			keepCount = i
			break
		}
	}

	if err := s.file.Truncate(truncatePos); err != nil {
		return fmt.Errorf("active_segment: truncate_from: %w", err)
	}
	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("active_segment: truncate_from: seek: %w", err)
	}

	s.size = truncatePos
	s.offsetIdx = append([]IndexEntry(nil), s.offsetIdx[:keepCount]...)
	s.timeIdx = s.timeIdx[:0]
	s.largestTS = 0
	for _, entry := range s.offsetIdx {
		if entry.MaxTimestamp > s.largestTS {
			s.timeIdx = append(s.timeIdx, TimestampIndexEntry{
				Timestamp:  entry.MaxTimestamp,
				BaseOffset: entry.BaseOffset,
			})
			s.largestTS = entry.MaxTimestamp
		}
	}

	return nil
}
