package diskless

import (
	"context"
	"fmt"

	"github.com/maksim/camu/internal/log"
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
	//
	// Pre-allocate the concatenated result from the refs' byte lengths and read
	// each range directly into it, avoiding per-range allocations and copies.
	var total int64
	for _, ref := range refs {
		if ref.EndOffset > committedHead {
			continue
		}
		total += ref.ByteLength
	}
	var result []byte
	if total > 0 {
		result = make([]byte, total)
	}
	pos := int64(0)
	for _, ref := range refs {
		if ref.EndOffset > committedHead {
			continue
		}
		if err := r.s3.GetRangeInto(ctx, ref.FileKey, ref.ByteOffset, ref.ByteLength, result[pos:pos+ref.ByteLength]); err != nil {
			return nil, 0, fmt.Errorf("s3 get range %s [%d:%d]: %w",
				ref.FileKey, ref.ByteOffset, ref.ByteOffset+ref.ByteLength, err)
		}
		// Uploaded diskless objects deliberately contain raw RecordBatch bytes;
		// logical offsets are assigned by the metastore commit. A ref can cover
		// several batches (a compacted merge object), so walk the self-framing
		// batches and patch each base offset into the returned copy, leaving
		// immutable S3 data safe for retries/compaction.
		if err := patchRefOffsets(result[pos:pos+ref.ByteLength], ref.BaseOffset); err != nil {
			return nil, 0, fmt.Errorf("patch fetched ref at base offset %d: %w", ref.BaseOffset, err)
		}
		pos += ref.ByteLength
	}

	return result, committedHead, nil
}

// patchRefOffsets overwrites the stored base offset of every self-framing
// RecordBatch in batchRange with its logical offset, starting at base. Uploaded
// diskless objects hold raw batches whose stored base offset is 0; the ref
// assigns the committed bases. A fresh ref spans a single batch, while a
// compacted ref spans the concatenated source batches, so every batch must be
// patched rather than only the first.
func patchRefOffsets(batchRange []byte, base int64) error {
	next := base
	batchPos := 0
	for batchPos < len(batchRange) {
		hdr, err := log.ReadRecordBatchHeader(batchRange[batchPos:])
		if err != nil {
			return fmt.Errorf("read batch header at offset %d: %w", next, err)
		}
		if err := log.PatchRecordBatchFirstOffset(batchRange[batchPos:], next); err != nil {
			return fmt.Errorf("patch batch at offset %d: %w", next, err)
		}
		next += int64(hdr.NumRecords)
		batchPos += int(hdr.RecordBatchSize())
	}
	return nil
}
