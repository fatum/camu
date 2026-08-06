package server

import (
	"encoding/base64"
	"unicode/utf8"
)

func readableHighWatermark(ps *partitionState) (uint64, bool) {
	if ps == nil {
		return 0, false
	}
	if ps.replicaState != nil {
		return ps.replicaState.HighWatermark(), true
	}
	if ps.followerHW > 0 {
		return ps.followerHW, true
	}
	return 0, false
}

// tryString returns the string representation of b if it is valid UTF-8,
// otherwise returns a base64-encoded version.
func tryString(b []byte) string {
	if !utf8.Valid(b) {
		return base64.StdEncoding.EncodeToString(b)
	}
	return string(b)
}
