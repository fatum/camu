package log

import (
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

// Index maps offset ranges to S3 segment keys, sorted by BaseOffset.
// It is stored as index.json per partition and updated after each segment flush.
type Index struct {
	segments []SegmentRef
}

// NewIndex returns an empty Index.
func NewIndex() *Index {
	return &Index{}
}

// Add inserts ref into the index in sorted order by BaseOffset.
func (idx *Index) Add(ref SegmentRef) {
	pos := sort.Search(len(idx.segments), func(i int) bool {
		return idx.segments[i].BaseOffset >= ref.BaseOffset
	})
	// Insert at pos
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

// MarshalJSON serializes the index to JSON for S3 storage.
func (idx *Index) MarshalJSON() ([]byte, error) {
	return json.Marshal(idx.segments)
}

// UnmarshalJSON deserializes the index from JSON.
func (idx *Index) UnmarshalJSON(data []byte) error {
	var segs []SegmentRef
	if err := json.Unmarshal(data, &segs); err != nil {
		return err
	}
	idx.segments = segs
	return nil
}
