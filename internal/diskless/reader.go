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
// along with the high watermark (partition head). Returns (nil, head, nil) if
// fromOffset >= head or no segments match.
func (r *Reader) Fetch(ctx context.Context, topic string, partition int, fromOffset int64, maxBytes int) ([]byte, int64, error) {
	head, err := r.meta.GetPartitionHead(ctx, topic, partition)
	if err != nil {
		return nil, 0, fmt.Errorf("get partition head: %w", err)
	}
	if fromOffset >= head {
		return nil, head, nil
	}

	refs, err := r.meta.QuerySegments(ctx, topic, partition, fromOffset, maxBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("query segments: %w", err)
	}
	if len(refs) == 0 {
		return nil, head, nil
	}

	var result []byte
	for _, ref := range refs {
		chunk, err := r.s3.GetRange(ctx, ref.FileKey, ref.ByteOffset, ref.ByteLength)
		if err != nil {
			return nil, 0, fmt.Errorf("s3 get range %s [%d:%d]: %w",
				ref.FileKey, ref.ByteOffset, ref.ByteOffset+ref.ByteLength, err)
		}
		result = append(result, chunk...)
	}

	return result, head, nil
}
