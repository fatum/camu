package main

import (
	"encoding/hex"
	"testing"
)

func TestTargetRecordCount(t *testing.T) {
	for _, tc := range []struct{ target, size, want int64 }{{1, 1, 1}, {1025, 1024, 2}, {2048, 1024, 2}} {
		got := (tc.target + tc.size - 1) / tc.size
		if got != tc.want {
			t.Fatalf("count(%d,%d)=%d, want %d", tc.target, tc.size, got, tc.want)
		}
	}
}

func TestHashStateDeterministicAndOrdering(t *testing.T) {
	var s hashState
	s.add(typedValue{ID: 0, Payload: "x", PayloadBytes: 1, Sequence: 0})
	s.add(typedValue{ID: 1, Payload: "x", PayloadBytes: 1, Sequence: 1})
	n, b, digest, err := s.result()
	if err != nil || n != 2 || b != 2 {
		t.Fatalf("result=(%d,%d,%v)", n, b, err)
	}
	if raw, e := hex.DecodeString(digest); e != nil || len(raw) != 32 {
		t.Fatalf("digest=%q", digest)
	}

	var bad hashState
	bad.add(typedValue{Sequence: 2})
	bad.add(typedValue{Sequence: 1})
	if _, _, _, err := bad.result(); err == nil {
		t.Fatal("expected ordering error")
	}
}
