package log

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

const segmentKeySuffix = ".segment"

// FormatSegmentKey returns the S3 key for a segment with zero-padded base offset.
// Format: {topic}/{partition}/{020d baseOffset}-{endOffset}-{epoch}.segment
func FormatSegmentKey(topic string, partition int, baseOffset, endOffset, epoch uint64) string {
	return fmt.Sprintf("%s/%d/%020d-%d-%d%s", topic, partition, baseOffset, endOffset, epoch, segmentKeySuffix)
}

// ParseSegmentKey extracts baseOffset, endOffset, and epoch from a segment S3 key.
func ParseSegmentKey(key string) (baseOffset, endOffset, epoch uint64, err error) {
	base := filepath.Base(key)
	if !strings.HasSuffix(base, segmentKeySuffix) {
		return 0, 0, 0, fmt.Errorf("not a segment key: %q", key)
	}
	name := strings.TrimSuffix(base, segmentKeySuffix)
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid segment key format: %q", key)
	}
	baseOffset, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse base offset: %w", err)
	}
	endOffset, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse end offset: %w", err)
	}
	epoch, err = strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse epoch: %w", err)
	}
	return baseOffset, endOffset, epoch, nil
}

// ListSegmentPrefix returns the S3 prefix for listing all objects of a partition.
func ListSegmentPrefix(topic string, partition int) string {
	return fmt.Sprintf("%s/%d/", topic, partition)
}

// ListSegmentStartAfter returns the S3 StartAfter key for seeking to a target offset.
func ListSegmentStartAfter(topic string, partition int, offset uint64) string {
	return fmt.Sprintf("%s/%d/%020d", topic, partition, offset)
}

// SegmentRefsFromKeys parses a list of S3 keys into SegmentRefs.
// Non-segment keys are silently skipped.
func SegmentRefsFromKeys(keys []string) []SegmentRef {
	var refs []SegmentRef
	for _, key := range keys {
		if !strings.HasSuffix(key, segmentKeySuffix) {
			continue
		}
		base, end, epoch, err := ParseSegmentKey(key)
		if err != nil {
			continue
		}
		refs = append(refs, SegmentRef{
			BaseOffset: base,
			EndOffset:  end,
			Epoch:      epoch,
			Key:        key,
		})
	}
	return refs
}
