package diskless

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

// Writer flushes buffered RecordBatch entries to S3 and registers segments.
type Writer struct {
	s3     *storage.S3Client
	meta   MetaStore
	nodeID string
	seq    atomic.Int64
}

// NewWriter creates a Writer that flushes to s3 and registers with meta.
func NewWriter(s3 *storage.S3Client, meta MetaStore, nodeID string) *Writer {
	return &Writer{s3: s3, meta: meta, nodeID: nodeID}
}

// Flush writes entries to S3 and registers segment metadata.
func (w *Writer) Flush(ctx context.Context, entries []BufferEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// 1. Read NumRecords from each entry's RecordBatch header.
	allocs := make([]OffsetAllocation, len(entries))
	for i, e := range entries {
		hdr, err := log.ReadRecordBatchHeader(e.Batch)
		if err != nil {
			w.sendError(entries, fmt.Errorf("read header for entry %d: %w", i, err))
			return err
		}
		allocs[i] = OffsetAllocation{
			Topic:     e.Topic,
			Partition: e.Partition,
			Count:     int(hdr.NumRecords),
		}
	}

	// 2. Allocate offsets.
	results, err := w.meta.AllocateOffsets(ctx, allocs)
	if err != nil {
		w.sendError(entries, fmt.Errorf("allocate offsets: %w", err))
		return err
	}

	// 3. Patch BaseOffset into each batch.
	for i, r := range results {
		if err := log.PatchRecordBatchFirstOffset(entries[i].Batch, r.BaseOffset); err != nil {
			w.sendError(entries, fmt.Errorf("patch offset for entry %d: %w", i, err))
			return err
		}
	}

	// 4. Concatenate all batches, tracking byte offsets.
	totalSize := 0
	for _, e := range entries {
		totalSize += len(e.Batch)
	}
	data := make([]byte, 0, totalSize)
	batchRefs := make([]BatchRef, len(entries))
	for i, e := range entries {
		byteOffset := int64(len(data))
		data = append(data, e.Batch...)
		batchRefs[i] = BatchRef{
			Topic:      e.Topic,
			Partition:  e.Partition,
			BaseOffset: results[i].BaseOffset,
			EndOffset:  results[i].BaseOffset + int64(allocs[i].Count),
			ByteOffset: byteOffset,
			ByteLength: int64(len(e.Batch)),
		}
	}

	// 5. S3 PUT.
	seq := w.seq.Add(1) - 1
	fileKey := fmt.Sprintf("_diskless/%s/%d-%d.data", w.nodeID, time.Now().UnixMilli(), seq)
	if err := w.s3.Put(ctx, fileKey, data, storage.PutOpts{}); err != nil {
		w.sendError(entries, fmt.Errorf("s3 put: %w", err))
		return err
	}

	// 6. Register segment.
	seg := SegmentRecord{
		FileKey:   fileKey,
		Batches:   batchRefs,
		CreatedAt: time.Now(),
		SizeBytes: int64(len(data)),
	}
	if err := w.meta.RegisterSegment(ctx, seg); err != nil {
		w.sendError(entries, fmt.Errorf("register segment: %w", err))
		return err
	}

	// 7. Notify success.
	for i, e := range entries {
		e.Done <- FlushResult{BaseOffset: results[i].BaseOffset}
	}
	return nil
}

func (w *Writer) sendError(entries []BufferEntry, err error) {
	for _, e := range entries {
		e.Done <- FlushResult{Err: err}
	}
}
