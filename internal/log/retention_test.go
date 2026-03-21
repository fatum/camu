package log

import (
	"testing"
	"time"
)

func TestRetention_RemovesExpiredSegments(t *testing.T) {
	idx := NewIndex()
	now := time.Now()

	idx.Add(SegmentRef{
		BaseOffset: 0, EndOffset: 99, Epoch: 1,
		Key:       "topic/0/0-1.segment",
		CreatedAt: now.Add(-48 * time.Hour),
	})
	idx.Add(SegmentRef{
		BaseOffset: 100, EndOffset: 199, Epoch: 1,
		Key:       "topic/0/100-1.segment",
		CreatedAt: now.Add(-1 * time.Hour),
	})

	expired := idx.RemoveExpired(24 * time.Hour)
	if len(expired) != 1 {
		t.Fatalf("RemoveExpired() removed %d, want 1", len(expired))
	}
	if expired[0].Key != "topic/0/0-1.segment" {
		t.Errorf("removed wrong segment: %q", expired[0].Key)
	}
}
