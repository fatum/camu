package diskless

import (
	"context"
	"fmt"

	"github.com/maksim/camu/internal/storage"
)

// Reader fetches RecordBatch data from S3 using MetaStore segment metadata.
type Reader struct {
	s3   *storage.S3Client
	meta MetaStore
}

// NewReader creates a Reader backed by s3 and meta.
func NewReader(s3 *storage.S3Client, meta MetaStore) *Reader {
	return &Reader{s3: s3, meta: meta}
}

// Fetch returns concatenated RecordBatch bytes for [fromOffset, ...) up to maxBytes,
// along with the committed high watermark (the highest offset durably
// materialized for the partition). Returns (nil, committedHead, nil) if
// fromOffset >= committedHead or no segments match. The committed head is
// distinct from the allocation counter: it only reflects registered segments,
// so a reader never sees offsets that were allocated but not yet persisted.
func (r *Reader) Fetch(ctx context.Context, topic string, partition int, fromOffset int64, maxBytes int) ([]byte, int64, error) {
	committedHead, err := r.meta.GetCommittedHead(ctx, topic, partition)
	if err != nil {
		return nil, 0, fmt.Errorf("get committed head: %w", err)
	}
	if fromOffset >= committedHead {
		return nil, committedHead, nil
	}

	refs, err := r.meta.QuerySegments(ctx, topic, partition, fromOffset, maxBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("query segments: %w", err)
	}
	if len(refs) == 0 {
		return nil, committedHead, nil
	}

	// Only expose refs at or below the committed head. A flush registers its
	// refs before advancing the committed head, so a reader querying between
	// the two never sees a partially-registered batch (un-acked data).
	var result []byte
	for _, ref := range refs {
		if ref.EndOffset > committedHead {
			continue
		}
		chunk, err := r.s3.GetRange(ctx, ref.FileKey, ref.ByteOffset, ref.ByteLength)
		if err != nil {
			return nil, 0, fmt.Errorf("s3 get range %s [%d:%d]: %w",
				ref.FileKey, ref.ByteOffset, ref.ByteOffset+ref.ByteLength, err)
		}
		result = append(result, chunk...)
	}

	return result, committedHead, nil
}
