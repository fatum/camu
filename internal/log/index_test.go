package log

import (
	"encoding/json"
	"testing"
	"time"
)

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
}

func TestIndex_MarshalJSON(t *testing.T) {
	idx := NewIndex()
	now := time.Now().UTC().Truncate(time.Second)
	idx.Add(SegmentRef{
		BaseOffset: 0,
		EndOffset:  99,
		Epoch:      1,
		Key:        "seg-0",
		CreatedAt:  now,
	})

	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	idx2 := NewIndex()
	if err := json.Unmarshal(data, idx2); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	ref, ok := idx2.Lookup(50)
	if !ok {
		t.Fatal("expected to find offset 50 after unmarshal")
	}
	if ref.BaseOffset != 0 {
		t.Errorf("expected BaseOffset=0, got %d", ref.BaseOffset)
	}
	if ref.Key != "seg-0" {
		t.Errorf("expected Key=seg-0, got %s", ref.Key)
	}
	if ref.Epoch != 1 {
		t.Errorf("expected Epoch=1, got %d", ref.Epoch)
	}
	if !ref.CreatedAt.Equal(now) {
		t.Errorf("expected CreatedAt=%v, got %v", now, ref.CreatedAt)
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
