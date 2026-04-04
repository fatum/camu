package log

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// sidecarMagic is the four-byte file-type tag "CIDX" (0x43 0x49 0x44 0x58).
	sidecarMagic = uint32(0x43494458)
	// sidecarVersion is the current encoding version.
	sidecarVersion = uint8(1)
	// indexEntrySize is the on-disk size of one IndexEntry in bytes.
	// Layout: [8B BaseOffset][8B LastOffset][8B Position][4B BatchSize][4B padding][8B FirstTimestamp][8B MaxTimestamp]
	indexEntrySize = 48
	// tsIndexEntrySize is the on-disk size of one TimestampIndexEntry in bytes.
	// Layout: [8B Timestamp][8B BaseOffset]
	tsIndexEntrySize = 16
)

// WriteSidecar serialises entries and tsEntries into the CIDX sidecar format
// and writes the result to w.
//
// Format:
//
//	[4B magic][1B version][4B offset_count][4B ts_count]
//	[offset_count × 48B IndexEntry records]
//	[ts_count     × 16B TimestampIndexEntry records]
func WriteSidecar(w io.Writer, entries []IndexEntry, tsEntries []TimestampIndexEntry) error {
	bw := binary.BigEndian

	// Header
	var hdr [13]byte
	bw.PutUint32(hdr[0:4], sidecarMagic)
	hdr[4] = sidecarVersion
	bw.PutUint32(hdr[5:9], uint32(len(entries)))
	bw.PutUint32(hdr[9:13], uint32(len(tsEntries)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("sidecar: write header: %w", err)
	}

	// IndexEntry records — 48 bytes each.
	var rec [indexEntrySize]byte
	for _, e := range entries {
		bw.PutUint64(rec[0:8], uint64(e.BaseOffset))
		bw.PutUint64(rec[8:16], uint64(e.LastOffset))
		bw.PutUint64(rec[16:24], uint64(e.Position))
		bw.PutUint32(rec[24:28], uint32(e.BatchSize))
		// bytes 28-31: padding (already zero from var decl, stays zero between
		// iterations because we only write to rec[0:28] and rec[32:48])
		rec[28] = 0
		rec[29] = 0
		rec[30] = 0
		rec[31] = 0
		bw.PutUint64(rec[32:40], uint64(e.FirstTimestamp))
		bw.PutUint64(rec[40:48], uint64(e.MaxTimestamp))
		if _, err := w.Write(rec[:]); err != nil {
			return fmt.Errorf("sidecar: write index entry: %w", err)
		}
	}

	// TimestampIndexEntry records — 16 bytes each.
	var tsRec [tsIndexEntrySize]byte
	for _, ts := range tsEntries {
		bw.PutUint64(tsRec[0:8], uint64(ts.Timestamp))
		bw.PutUint64(tsRec[8:16], uint64(ts.BaseOffset))
		if _, err := w.Write(tsRec[:]); err != nil {
			return fmt.Errorf("sidecar: write ts index entry: %w", err)
		}
	}

	return nil
}

// ReadSidecar parses a CIDX sidecar blob and returns the two index slices.
// It validates the magic bytes, version, and that data is long enough to
// contain the declared number of records.
func ReadSidecar(data []byte) ([]IndexEntry, []TimestampIndexEntry, error) {
	const headerSize = 13 // 4 + 1 + 4 + 4

	if len(data) < headerSize {
		return nil, nil, fmt.Errorf("sidecar: data too short for header (%d bytes)", len(data))
	}

	bw := binary.BigEndian

	magic := bw.Uint32(data[0:4])
	if magic != sidecarMagic {
		return nil, nil, fmt.Errorf("sidecar: invalid magic 0x%08X (want 0x%08X)", magic, sidecarMagic)
	}

	version := data[4]
	if version != sidecarVersion {
		return nil, nil, fmt.Errorf("sidecar: unsupported version %d (want %d)", version, sidecarVersion)
	}

	offsetCount := int(bw.Uint32(data[5:9]))
	tsCount := int(bw.Uint32(data[9:13]))

	wantLen := headerSize + offsetCount*indexEntrySize + tsCount*tsIndexEntrySize
	if len(data) < wantLen {
		return nil, nil, fmt.Errorf(
			"sidecar: data too short: have %d bytes, need %d (%d index + %d ts entries)",
			len(data), wantLen, offsetCount, tsCount,
		)
	}

	entries := make([]IndexEntry, offsetCount)
	off := headerSize
	for i := range entries {
		e := &entries[i]
		e.BaseOffset = int64(bw.Uint64(data[off+0 : off+8]))
		e.LastOffset = int64(bw.Uint64(data[off+8 : off+16]))
		e.Position = int64(bw.Uint64(data[off+16 : off+24]))
		e.BatchSize = int32(bw.Uint32(data[off+24 : off+28]))
		// bytes off+28 .. off+31 are padding — skip
		e.FirstTimestamp = int64(bw.Uint64(data[off+32 : off+40]))
		e.MaxTimestamp = int64(bw.Uint64(data[off+40 : off+48]))
		off += indexEntrySize
	}

	tsEntries := make([]TimestampIndexEntry, tsCount)
	for i := range tsEntries {
		ts := &tsEntries[i]
		ts.Timestamp = int64(bw.Uint64(data[off+0 : off+8]))
		ts.BaseOffset = int64(bw.Uint64(data[off+8 : off+16]))
		off += tsIndexEntrySize
	}

	return entries, tsEntries, nil
}
