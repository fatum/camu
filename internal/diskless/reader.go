package diskless

import (
	"context"
	"fmt"
	"math"

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

// scanChunkSize bounds how much of a ref is streamed while locating the byte
// position of a fetch's starting offset. Compacted refs can be tens of MiB;
// scanning in chunks keeps the seek cost proportional to the offset's position
// instead of the ref's total size.
const scanChunkSize = 4 << 20

// scanRefForOffset walks a ref's self-framing RecordBatches to find the byte
// position (relative to ref.ByteOffset) of the first batch whose offset range
// reaches fromOffset, and that batch's base offset. fromOffset must lie within
// [ref.BaseOffset, ref.EndOffset). The ref is streamed in bounded chunks so a
// large compacted object is never downloaded in full just to locate a page.
// Stored batches carry base offset 0 (they are patched on read), so the walk
// tracks the logical offset cursor itself rather than trusting stored bases.
func (r *Reader) scanRefForOffset(ctx context.Context, ref SegmentRef, fromOffset int64) (int64, int64, error) {
	if fromOffset <= ref.BaseOffset {
		return 0, ref.BaseOffset, nil
	}
	read := int64(0)
	offset := ref.BaseOffset
	buf := make([]byte, scanChunkSize)
	for read < ref.ByteLength {
		n := ref.ByteLength - read
		if n > scanChunkSize {
			n = scanChunkSize
		}
		chunk := buf[:n]
		if err := r.s3.GetRangeInto(ctx, ref.FileKey, ref.ByteOffset+read, n, chunk); err != nil {
			return 0, 0, fmt.Errorf("scan ref %s at byte %d: %w", ref.FileKey, read, err)
		}
		pos := 0
		for pos < len(chunk) {
			if len(chunk)-pos < log.RecordBatchHeaderSize {
				break // next batch header crosses the chunk boundary; refetch from pos
			}
			hdr, err := log.ReadRecordBatchHeader(chunk[pos:])
			if err != nil {
				return 0, 0, fmt.Errorf("scan ref %s batch header at offset %d: %w", ref.FileKey, offset, err)
			}
			size := int(hdr.RecordBatchSize())
			if size < log.RecordBatchHeaderSize {
				return 0, 0, fmt.Errorf("scan ref %s batch at offset %d: invalid size %d", ref.FileKey, offset, size)
			}
			if offset+int64(hdr.NumRecords) > fromOffset {
				return read + int64(pos), offset, nil
			}
			offset += int64(hdr.NumRecords)
			pos += size
		}
		if pos <= 0 {
			return 0, 0, fmt.Errorf("scan ref %s: no parseable batch at byte %d", ref.FileKey, read)
		}
		read += int64(pos)
	}
	return 0, 0, fmt.Errorf("scan ref %s: fromOffset %d beyond ref end %d", ref.FileKey, fromOffset, ref.EndOffset)
}

// extendToWholeBatches grows data (fetched from fileKey at absolute position
// filePos) until it ends on a self-framing RecordBatch boundary, fetching the
// missing tail of the final batch from S3 when the read budget split it. It
// never shrinks data.
func (r *Reader) extendToWholeBatches(ctx context.Context, fileKey string, filePos int64, data []byte) ([]byte, error) {
	pos := 0
	for pos < len(data) {
		if len(data)-pos < log.RecordBatchHeaderSize {
			extra := make([]byte, log.RecordBatchHeaderSize-(len(data)-pos))
			if err := r.s3.GetRangeInto(ctx, fileKey, filePos+int64(len(data)), int64(len(extra)), extra); err != nil {
				return nil, fmt.Errorf("extend ref %s header at byte %d: %w", fileKey, filePos+int64(len(data)), err)
			}
			data = append(data, extra...)
		}
		hdr, err := log.ReadRecordBatchHeader(data[pos:])
		if err != nil {
			return nil, fmt.Errorf("extend ref %s header at byte %d: %w", fileKey, filePos+int64(pos), err)
		}
		size := int(hdr.RecordBatchSize())
		if size < log.RecordBatchHeaderSize {
			return nil, fmt.Errorf("extend ref %s batch at byte %d: invalid size %d", fileKey, filePos+int64(pos), size)
		}
		end := pos + size
		if end > len(data) {
			extra := make([]byte, end-len(data))
			if err := r.s3.GetRangeInto(ctx, fileKey, filePos+int64(len(data)), int64(len(extra)), extra); err != nil {
				return nil, fmt.Errorf("extend ref %s body at byte %d: %w", fileKey, filePos+int64(len(data)), err)
			}
			data = append(data, extra...)
		}
		pos = end
	}
	return data, nil
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
	// A compacted ref can span far more than the byte budget and its data can
	// begin before fromOffset. The first ref is scanned to the byte position of
	// the batch that reaches fromOffset, and every ref is then read only up to
	// the remaining budget (extended to a whole-batch boundary) instead of
	// being downloaded in full.
	budget := int64(maxBytes)
	if budget <= 0 {
		budget = math.MaxInt64
	}
	var result []byte
	first := true
	for _, ref := range refs {
		if ref.EndOffset > committedHead {
			continue
		}
		if int64(len(result)) >= budget {
			break
		}
		startByte := ref.ByteOffset
		batchBase := ref.BaseOffset
		if first {
			skip, base, scanErr := r.scanRefForOffset(ctx, ref, fromOffset)
			if scanErr != nil {
				return nil, 0, fmt.Errorf("scan ref %s for offset %d: %w", ref.FileKey, fromOffset, scanErr)
			}
			startByte += skip
			batchBase = base
			first = false
		}
		avail := ref.ByteOffset + ref.ByteLength - startByte
		if avail <= 0 {
			continue
		}
		want := budget - int64(len(result))
		if want > avail {
			want = avail
		}
		slice := make([]byte, want)
		if err := r.s3.GetRangeInto(ctx, ref.FileKey, startByte, want, slice); err != nil {
			return nil, 0, fmt.Errorf("s3 get range %s [%d:%d]: %w",
				ref.FileKey, startByte, startByte+want, err)
		}
		if len(slice) > 0 {
			slice, err = r.extendToWholeBatches(ctx, ref.FileKey, startByte, slice)
			if err != nil {
				return nil, 0, err
			}
			if err := patchRefOffsets(slice, batchBase); err != nil {
				return nil, 0, fmt.Errorf("patch fetched ref at base offset %d: %w", batchBase, err)
			}
			result = append(result, slice...)
		}
	}

	// A single ref can far exceed the byte budget: a compacted merge object is
	// built up to the full compaction target (64MB), so QuerySegments's
	// ref-level cap cannot bound the response on its own. Trim to whole
	// self-framing record batches within maxBytes so a client that requested a
	// smaller budget (e.g. Kafka FetchMaxPartitionBytes=16MB) is never served
	// an oversized record batch that clients treat as a budget violation.
	result = trimResultBatches(result, maxBytes)
	return result, committedHead, nil
}

// trimResultBatches bounds RecordBatch bytes to whole batches within maxBytes.
// It always keeps the first batch even when that batch alone exceeds the
// budget, so a fetch always makes progress (Kafka semantics). A malformed
// trailing range is left as-is rather than truncated mid-batch. maxBytes <= 0
// means unbounded.
func trimResultBatches(data []byte, maxBytes int) []byte {
	if maxBytes <= 0 || len(data) <= maxBytes {
		return data
	}
	end := 0
	batchCount := 0
	for pos := 0; pos < len(data); {
		hdr, err := log.ReadRecordBatchHeader(data[pos:])
		if err != nil {
			break
		}
		size := int(hdr.RecordBatchSize())
		if batchCount > 0 && end+size > maxBytes {
			break
		}
		end += size
		pos += size
		batchCount++
	}
	return data[:end]
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
