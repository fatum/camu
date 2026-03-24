package log

import (
	"bytes"
	"encoding/json"
	"sort"
	"time"
)

// SegmentRef describes a single segment file stored in S3.
type SegmentRef struct {
	BaseOffset uint64    `json:"base_offset"`
	EndOffset  uint64    `json:"end_offset"`
	Epoch      uint64    `json:"epoch"`
	Key        string    `json:"key"`
	CreatedAt  time.Time `json:"created_at"`
}

// EpochEntry records when a new epoch started and at which offset.
type EpochEntry struct {
	Epoch       uint64 `json:"epoch"`
	StartOffset uint64 `json:"start_offset"`
}

// indexJSON is the wire format for the object-style index.
type indexJSON struct {
	Segments      []SegmentRef `json:"segments"`
	EpochHistory  []EpochEntry `json:"epoch_history,omitempty"`
	HighWatermark uint64       `json:"high_watermark,omitempty"`
}

// Index maps offset ranges to S3 segment keys, sorted by BaseOffset.
// It is stored as index.json per partition and updated after each segment flush.
type Index struct {
	segments      []SegmentRef
	epochHistory  []EpochEntry
	highWatermark uint64
}

// NewIndex returns an empty Index.
func NewIndex() *Index {
	return &Index{}
}

// Add inserts ref into the index in sorted order by BaseOffset.
func (idx *Index) Add(ref SegmentRef) {
	// A newly flushed segment may replace the prior tail segment after leader
	// reassignment, e.g. old tail 46-46 replaced by new 46-48. Only remove
	// segments fully contained within the new segment's range. Partial overlaps
	// (e.g. existing 12-13 vs new 13-15) must be kept to avoid losing offsets
	// that only exist in the old segment.
	filtered := idx.segments[:0]
	for _, existing := range idx.segments {
		fullyContained := ref.BaseOffset <= existing.BaseOffset && existing.EndOffset <= ref.EndOffset
		if !fullyContained {
			filtered = append(filtered, existing)
		}
	}
	idx.segments = filtered

	pos := sort.Search(len(idx.segments), func(i int) bool {
		return idx.segments[i].BaseOffset >= ref.BaseOffset
	})
	idx.segments = append(idx.segments, SegmentRef{})
	copy(idx.segments[pos+1:], idx.segments[pos:])
	idx.segments[pos] = ref
}

// Lookup finds the segment that contains the given offset using binary search.
// Returns the SegmentRef and true if found, or zero value and false otherwise.
func (idx *Index) Lookup(offset uint64) (SegmentRef, bool) {
	// Find the rightmost segment with BaseOffset <= offset
	n := len(idx.segments)
	pos := sort.Search(n, func(i int) bool {
		return idx.segments[i].BaseOffset > offset
	})
	// pos is the first segment with BaseOffset > offset; candidate is pos-1
	if pos == 0 {
		return SegmentRef{}, false
	}
	ref := idx.segments[pos-1]
	if offset > ref.EndOffset {
		return SegmentRef{}, false
	}
	return ref, true
}

// RemoveBefore removes and returns all segments whose EndOffset is strictly
// less than the given offset.
func (idx *Index) RemoveBefore(offset uint64) []SegmentRef {
	var removed []SegmentRef
	keep := idx.segments[:0]
	for _, ref := range idx.segments {
		if ref.EndOffset < offset {
			removed = append(removed, ref)
		} else {
			keep = append(keep, ref)
		}
	}
	idx.segments = keep
	return removed
}

// RemoveExpired removes and returns all segments where CreatedAt + retention
// is before now. Used by the background retention cleanup goroutine (Task 18).
func (idx *Index) RemoveExpired(retention time.Duration) []SegmentRef {
	now := time.Now()
	var removed []SegmentRef
	keep := idx.segments[:0]
	for _, ref := range idx.segments {
		if ref.CreatedAt.Add(retention).Before(now) {
			removed = append(removed, ref)
		} else {
			keep = append(keep, ref)
		}
	}
	idx.segments = keep
	return removed
}

// NextOffset returns EndOffset+1 of the last segment, or 0 if the index is empty.
func (idx *Index) NextOffset() uint64 {
	if len(idx.segments) == 0 {
		return 0
	}
	return idx.segments[len(idx.segments)-1].EndOffset + 1
}

// SetHighWatermark sets the high-watermark offset on the index.
func (idx *Index) SetHighWatermark(hw uint64) {
	idx.highWatermark = hw
}

// HighWatermark returns the high-watermark offset stored in the index.
func (idx *Index) HighWatermark() uint64 {
	return idx.highWatermark
}

// SetEpochHistory replaces the epoch history entries in the index.
func (idx *Index) SetEpochHistory(eh []EpochEntry) {
	idx.epochHistory = eh
}

// EpochHistory returns the epoch history entries stored in the index.
func (idx *Index) EpochHistory() []EpochEntry {
	return idx.epochHistory
}

// MarshalJSON serializes the index as an object for S3 storage.
func (idx *Index) MarshalJSON() ([]byte, error) {
	return json.Marshal(indexJSON{
		Segments:      idx.segments,
		EpochHistory:  idx.epochHistory,
		HighWatermark: idx.highWatermark,
	})
}

// UnmarshalJSON deserializes the index from JSON, supporting both the legacy
// bare-array format and the current object format.
func (idx *Index) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '[' {
		// Legacy array format — segments only, no epoch history or HW.
		var segs []SegmentRef
		if err := json.Unmarshal(data, &segs); err != nil {
			return err
		}
		idx.segments = segs
		idx.epochHistory = nil
		idx.highWatermark = 0
	} else {
		// Current object format.
		var obj indexJSON
		if err := json.Unmarshal(data, &obj); err != nil {
			return err
		}
		idx.segments = obj.Segments
		idx.epochHistory = obj.EpochHistory
		idx.highWatermark = obj.HighWatermark
	}
	sort.Slice(idx.segments, func(i, j int) bool {
		return idx.segments[i].BaseOffset < idx.segments[j].BaseOffset
	})
	return nil
}
