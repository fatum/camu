package log

import (
	"sort"
	"time"
)

// SegmentRef describes a single segment file stored in S3.
type SegmentRef struct {
	BaseOffset     uint64    `json:"base_offset"`
	EndOffset      uint64    `json:"end_offset"`
	MinTimestamp   int64     `json:"min_timestamp,omitempty"`
	MaxTimestamp   int64     `json:"max_timestamp,omitempty"`
	Epoch          uint64    `json:"epoch"`
	Key            string    `json:"key"`
	OffsetIndexKey string    `json:"offset_index_key,omitempty"`
	MetaKey        string    `json:"meta_key,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

func (r SegmentRef) OffsetIndexObjectKey() string {
	if r.OffsetIndexKey != "" {
		return r.OffsetIndexKey
	}
	return SegmentOffsetIndexKey(r.Key)
}

func (r SegmentRef) MetaObjectKey() string {
	if r.MetaKey != "" {
		return r.MetaKey
	}
	return SegmentMetadataKey(r.Key)
}

// EpochEntry records when a new epoch started and at which offset.
type EpochEntry struct {
	Epoch       uint64 `json:"epoch"`
	StartOffset uint64 `json:"start_offset"`
}

// Index maps offset ranges to S3 segment keys, sorted by BaseOffset.
// It is an in-memory cache populated from S3 LIST on startup and updated on flush.
type Index struct {
	segments      []SegmentRef
	baseOffsets   []uint64
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
	idx.rebuildOffsets()
}

// Lookup finds the segment that contains the given offset using binary search.
// Returns the SegmentRef and true if found, or zero value and false otherwise.
func (idx *Index) Lookup(offset uint64) (SegmentRef, bool) {
	// Find the rightmost segment with BaseOffset <= offset
	n := len(idx.baseOffsets)
	pos := sort.Search(n, func(i int) bool {
		return idx.baseOffsets[i] > offset
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

// SegmentsFrom returns the ordered segment refs starting with the segment that
// contains offset. If maxSegments <= 0, all remaining segments are returned.
func (idx *Index) SegmentsFrom(offset uint64, maxSegments int) []SegmentRef {
	n := len(idx.baseOffsets)
	pos := sort.Search(n, func(i int) bool {
		return idx.baseOffsets[i] > offset
	})
	if pos == 0 {
		return nil
	}
	start := pos - 1
	if offset > idx.segments[start].EndOffset {
		return nil
	}

	end := len(idx.segments)
	if maxSegments > 0 && start+maxSegments < end {
		end = start + maxSegments
	}
	return append([]SegmentRef(nil), idx.segments[start:end]...)
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
	idx.rebuildOffsets()
	return removed
}

// RemoveObjectKeys removes and returns segments matching any of the provided
// object keys.
func (idx *Index) RemoveObjectKeys(keys ...string) []SegmentRef {
	if len(keys) == 0 {
		return nil
	}
	match := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		match[key] = struct{}{}
	}
	if len(match) == 0 {
		return nil
	}

	var removed []SegmentRef
	keep := idx.segments[:0]
	for _, ref := range idx.segments {
		if _, ok := match[ref.Key]; ok {
			removed = append(removed, ref)
			continue
		}
		if _, ok := match[ref.OffsetIndexObjectKey()]; ok {
			removed = append(removed, ref)
			continue
		}
		if _, ok := match[ref.MetaObjectKey()]; ok {
			removed = append(removed, ref)
			continue
		}
		keep = append(keep, ref)
	}
	idx.segments = keep
	idx.rebuildOffsets()
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
	idx.rebuildOffsets()
	return removed
}

// NextOffset returns EndOffset+1 of the last segment, or 0 if the index is empty.
func (idx *Index) NextOffset() uint64 {
	if len(idx.segments) == 0 {
		return 0
	}
	return idx.segments[len(idx.segments)-1].EndOffset + 1
}

// FirstOffset returns the base offset of the first retained segment.
func (idx *Index) FirstOffset() (uint64, bool) {
	if len(idx.segments) == 0 {
		return 0, false
	}
	return idx.segments[0].BaseOffset, true
}

// SetHighWatermark sets the high-watermark offset on the index.
func (idx *Index) SetHighWatermark(hw uint64) {
	idx.highWatermark = hw
}

// HighWatermark returns the high-watermark offset stored in the index.
func (idx *Index) HighWatermark() uint64 {
	return idx.highWatermark
}

// FirstOffsetForTimestamp returns the first segment base offset whose retained
// timestamp window could contain the requested timestamp.
func (idx *Index) FirstOffsetForTimestamp(timestamp int64) (uint64, bool) {
	for _, seg := range idx.segments {
		if seg.MaxTimestamp == 0 || seg.MaxTimestamp >= timestamp {
			return seg.BaseOffset, true
		}
	}
	return 0, false
}

// SetEpochHistory replaces the epoch history entries in the index.
func (idx *Index) SetEpochHistory(eh []EpochEntry) {
	idx.epochHistory = eh
}

// EpochHistory returns the epoch history entries stored in the index.
func (idx *Index) EpochHistory() []EpochEntry {
	return idx.epochHistory
}

func (idx *Index) rebuildOffsets() {
	idx.baseOffsets = make([]uint64, len(idx.segments))
	for i, seg := range idx.segments {
		idx.baseOffsets[i] = seg.BaseOffset
	}
}
