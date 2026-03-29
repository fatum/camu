package log

import "testing"

func TestFormatSegmentKey(t *testing.T) {
	key := FormatSegmentKey("orders", 0, 0, 99, 1)
	want := "orders/0/00000000000000000000-99-1.segment"
	if key != want {
		t.Fatalf("got %q, want %q", key, want)
	}
}

func TestFormatSegmentKey_LargeOffset(t *testing.T) {
	key := FormatSegmentKey("events", 2, 10000, 10099, 3)
	want := "events/2/00000000000000010000-10099-3.segment"
	if key != want {
		t.Fatalf("got %q, want %q", key, want)
	}
}

func TestParseSegmentKey(t *testing.T) {
	base, end, epoch, err := ParseSegmentKey("orders/0/00000000000000000000-99-1.segment")
	if err != nil {
		t.Fatal(err)
	}
	if base != 0 || end != 99 || epoch != 1 {
		t.Fatalf("got base=%d end=%d epoch=%d", base, end, epoch)
	}
}

func TestParseSegmentKey_Invalid(t *testing.T) {
	_, _, _, err := ParseSegmentKey("orders/0/index.json")
	if err == nil {
		t.Fatal("expected error for non-segment key")
	}
}

func TestSegmentKeyRoundTrip(t *testing.T) {
	key := FormatSegmentKey("t", 0, 12345, 12400, 7)
	base, end, epoch, err := ParseSegmentKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if base != 12345 || end != 12400 || epoch != 7 {
		t.Fatalf("round-trip failed: base=%d end=%d epoch=%d", base, end, epoch)
	}
}

func TestSegmentKeySidecars(t *testing.T) {
	key := FormatSegmentKey("orders", 0, 0, 99, 1)
	idxKey := SegmentOffsetIndexKey(key)
	metaKey := SegmentMetadataKey(key)
	if idxKey != "orders/0/00000000000000000000-99-1.offset.idx" {
		t.Fatalf("offset index key: %q", idxKey)
	}
	if metaKey != "orders/0/00000000000000000000-99-1.meta.json" {
		t.Fatalf("metadata key: %q", metaKey)
	}
}

func TestListSegmentPrefix(t *testing.T) {
	prefix := ListSegmentPrefix("orders", 0)
	if prefix != "orders/0/" {
		t.Fatalf("got %q", prefix)
	}
}

func TestListSegmentStartAfter(t *testing.T) {
	key := ListSegmentStartAfter("orders", 0, 5000)
	want := "orders/0/00000000000000005000"
	if key != want {
		t.Fatalf("got %q, want %q", key, want)
	}
}

func TestSegmentRefsFromKeys(t *testing.T) {
	keys := []string{
		"orders/0/00000000000000000000-99-1.segment",
		"orders/0/00000000000000000000-99-1.offset.idx",
		"orders/0/00000000000000000000-99-1.meta.json",
		"orders/0/00000000000000000100-199-1.segment",
		"orders/0/state.json",
	}
	refs := SegmentRefsFromKeys(keys)
	if len(refs) != 2 {
		t.Fatalf("got %d refs, want 2", len(refs))
	}
	if refs[0].BaseOffset != 0 || refs[0].EndOffset != 99 {
		t.Fatalf("ref[0]: base=%d end=%d", refs[0].BaseOffset, refs[0].EndOffset)
	}
	if refs[1].BaseOffset != 100 || refs[1].EndOffset != 199 {
		t.Fatalf("ref[1]: base=%d end=%d", refs[1].BaseOffset, refs[1].EndOffset)
	}
}
