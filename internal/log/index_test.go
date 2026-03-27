package log

import "testing"

func TestIndex_AddAndLookup(t *testing.T) {
	idx := NewIndex()

	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 99, Key: "seg-0"})
	idx.Add(SegmentRef{BaseOffset: 100, EndOffset: 199, Key: "seg-1"})

	// Lookup offset 50 should find seg-0
	ref, ok := idx.Lookup(50)
	if !ok {
		t.Fatal("expected to find offset 50, got not found")
	}
	if ref.BaseOffset != 0 {
		t.Errorf("expected BaseOffset=0, got %d", ref.BaseOffset)
	}
	if ref.Key != "seg-0" {
		t.Errorf("expected Key=seg-0, got %s", ref.Key)
	}

	// Lookup offset 150 should find seg-1
	ref, ok = idx.Lookup(150)
	if !ok {
		t.Fatal("expected to find offset 150, got not found")
	}
	if ref.BaseOffset != 100 {
		t.Errorf("expected BaseOffset=100, got %d", ref.BaseOffset)
	}
	if ref.Key != "seg-1" {
		t.Errorf("expected Key=seg-1, got %s", ref.Key)
	}

	// Lookup offset 200 should not find anything
	_, ok = idx.Lookup(200)
	if ok {
		t.Error("expected offset 200 to not be found, but it was")
	}
	if got := idx.baseOffsets; len(got) != 2 || got[0] != 0 || got[1] != 100 {
		t.Fatalf("baseOffsets = %v, want [0 100]", got)
	}
}


func TestIndex_RemoveBefore(t *testing.T) {
	idx := NewIndex()
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 99, Key: "seg-0"})
	idx.Add(SegmentRef{BaseOffset: 100, EndOffset: 199, Key: "seg-1"})
	idx.Add(SegmentRef{BaseOffset: 200, EndOffset: 299, Key: "seg-2"})

	// Remove before offset 150: segments with EndOffset < 150 should be removed
	// seg-0 has EndOffset=99 < 150 -> removed
	// seg-1 has EndOffset=199 >= 150 -> kept
	removed := idx.RemoveBefore(150)

	if len(removed) != 1 {
		t.Fatalf("expected 1 removed, got %d", len(removed))
	}
	if removed[0].Key != "seg-0" {
		t.Errorf("expected removed key=seg-0, got %s", removed[0].Key)
	}
	if got := idx.baseOffsets; len(got) != 2 || got[0] != 100 || got[1] != 200 {
		t.Fatalf("baseOffsets after RemoveBefore = %v, want [100 200]", got)
	}

	// Lookup offset 50 should now fail (seg-0 was removed)
	_, ok := idx.Lookup(50)
	if ok {
		t.Error("expected offset 50 to not be found after RemoveBefore")
	}
}

func TestIndex_NextOffset(t *testing.T) {
	idx := NewIndex()

	// Empty index returns 0
	if next := idx.NextOffset(); next != 0 {
		t.Errorf("expected NextOffset=0 for empty index, got %d", next)
	}

	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 99, Key: "seg-0"})

	// After adding seg-0 (EndOffset=99), NextOffset should be 100
	if next := idx.NextOffset(); next != 100 {
		t.Errorf("expected NextOffset=100, got %d", next)
	}
}

func TestIndex_Add_ReplacesOverlappingTail(t *testing.T) {
	idx := NewIndex()
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 15, Key: "seg-0-15"})
	idx.Add(SegmentRef{BaseOffset: 16, EndOffset: 16, Key: "seg-16-16"})

	// Jepsen leader reassignment can flush a recovered tail plus new writes
	// into a replacement segment that overlaps the prior tail.
	idx.Add(SegmentRef{BaseOffset: 16, EndOffset: 17, Key: "seg-16-17"})

	ref, ok := idx.Lookup(17)
	if !ok {
		t.Fatal("expected to find offset 17 after overlapping replacement")
	}
	if ref.Key != "seg-16-17" {
		t.Fatalf("expected replacement segment, got %s", ref.Key)
	}

	ref, ok = idx.Lookup(16)
	if !ok {
		t.Fatal("expected to find offset 16 after overlapping replacement")
	}
	if ref.Key != "seg-16-17" {
		t.Fatalf("expected replacement segment for offset 16, got %s", ref.Key)
	}

	if got := len(idx.segments); got != 2 {
		t.Fatalf("expected 2 segments after replacement, got %d", got)
	}
}

func TestIndex_Add_ReplacesContainedSegments(t *testing.T) {
	idx := NewIndex()
	idx.Add(SegmentRef{BaseOffset: 46, EndOffset: 46, Key: "seg-46-46"})
	idx.Add(SegmentRef{BaseOffset: 47, EndOffset: 47, Key: "seg-47-47"})
	idx.Add(SegmentRef{BaseOffset: 48, EndOffset: 48, Key: "seg-48-48"})

	idx.Add(SegmentRef{BaseOffset: 46, EndOffset: 48, Key: "seg-46-48"})

	for _, offset := range []uint64{46, 47, 48} {
		ref, ok := idx.Lookup(offset)
		if !ok {
			t.Fatalf("expected to find offset %d after replacement", offset)
		}
		if ref.Key != "seg-46-48" {
			t.Fatalf("expected merged segment for offset %d, got %s", offset, ref.Key)
		}
	}

	if got := len(idx.segments); got != 1 {
		t.Fatalf("expected 1 segment after contained replacement, got %d", got)
	}
}

func TestIndex_Add_KeepsPartiallyOverlappingSegment(t *testing.T) {
	idx := NewIndex()
	// Old leader flushed segments 0-0 through 12-13
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 0, Key: "seg-0-0"})
	idx.Add(SegmentRef{BaseOffset: 12, EndOffset: 13, Key: "seg-12-13"})

	// New leader recovery flushes 13-15 from WAL. This partially overlaps
	// with seg-12-13 but must NOT remove it — offset 12 is only there.
	idx.Add(SegmentRef{BaseOffset: 13, EndOffset: 15, Key: "seg-13-15"})

	// Offset 12 must still be reachable via the old segment.
	ref, ok := idx.Lookup(12)
	if !ok {
		t.Fatal("expected to find offset 12 from partially overlapping old segment")
	}
	if ref.Key != "seg-12-13" {
		t.Fatalf("expected seg-12-13, got %s", ref.Key)
	}

	// Offset 13 should come from the new segment (higher base wins in sorted order).
	ref, ok = idx.Lookup(14)
	if !ok {
		t.Fatal("expected to find offset 14")
	}
	if ref.Key != "seg-13-15" {
		t.Fatalf("expected seg-13-15, got %s", ref.Key)
	}

	if got := len(idx.segments); got != 3 {
		t.Fatalf("expected 3 segments (old kept + new), got %d", got)
	}
}

func TestIndex_SegmentsFrom(t *testing.T) {
	idx := NewIndex()
	idx.Add(SegmentRef{BaseOffset: 0, EndOffset: 9, Key: "seg-0"})
	idx.Add(SegmentRef{BaseOffset: 10, EndOffset: 19, Key: "seg-10"})
	idx.Add(SegmentRef{BaseOffset: 20, EndOffset: 29, Key: "seg-20"})

	segs := idx.SegmentsFrom(12, 2)
	if len(segs) != 2 {
		t.Fatalf("len(SegmentsFrom) = %d, want 2", len(segs))
	}
	if segs[0].Key != "seg-10" || segs[1].Key != "seg-20" {
		t.Fatalf("SegmentsFrom returned keys %q, %q", segs[0].Key, segs[1].Key)
	}

	if segs := idx.SegmentsFrom(30, 0); segs != nil {
		t.Fatalf("SegmentsFrom(30) = %v, want nil", segs)
	}
}

