package server

import "testing"

// TestDisklessFlushUploadMs verifies the upload-millis parser used by the
// orphan sweep's grace filter. The node id may itself contain dashes, so the
// millis are the second-to-last dash-separated component.
func TestDisklessFlushUploadMs(t *testing.T) {
	cases := []struct {
		key string
		ms  int64
		ok  bool
	}{
		{"_diskless/000/camu-benchmark-01-1786127035675-3951.data", 1786127035675, true},
		{"_diskless/042/node1-1234567890123-0.data", 1234567890123, true},
		{"_diskless/000/node1-123.data", 0, false}, // missing seq
		{"_diskless_merge/t/0/00000000000000000000-00000000000001731320.data", 0, false}, // not a flush object
		{"_diskless/000/node1-nope-3951.data", 0, false}, // non-numeric millis
	}
	for _, c := range cases {
		ms, ok := disklessFlushUploadMs(c.key)
		if ok != c.ok || (ok && ms != c.ms) {
			t.Fatalf("disklessFlushUploadMs(%q) = (%d, %v), want (%d, %v)", c.key, ms, ok, c.ms, c.ok)
		}
	}
}
