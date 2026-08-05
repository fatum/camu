package diskless

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

// Writer flushes buffered RecordBatch entries to S3 and registers segments.
type Writer struct {
	s3          *storage.S3Client
	meta        MetaStore
	nodeID      string
	seq         atomic.Int64
	commitMu    sync.Mutex
	commitTails map[string]chan struct{}
}

type pendingBatch struct {
	topic      string
	partition  int
	count      int
	producerID int64
	sequence   int64
	byteOffset int64
	byteLength int64
}

type commitTurn struct{ previous, done chan struct{} }

// maxFlushRetryBackoff bounds the retry backoff when materializing a flushed
// batch to object storage.
const maxFlushRetryBackoff = 5 * time.Second

// NewWriter creates a Writer that flushes to s3 and registers with meta.
func NewWriter(s3 *storage.S3Client, meta MetaStore, nodeID string) *Writer {
	return &Writer{s3: s3, meta: meta, nodeID: nodeID, commitTails: make(map[string]chan struct{})}
}

// reserveCommitTurns establishes partition submission order before any upload
// begins. Uploads can then complete in any order, but a later batch cannot
// validate its producer sequence before its predecessor commits or fails.
func (w *Writer) reserveCommitTurns(entries []BufferEntry) []commitTurn {
	w.commitMu.Lock()
	defer w.commitMu.Unlock()
	turns := make([]commitTurn, len(entries))
	for i, e := range entries {
		key := partitionKey(e.Topic, e.Partition)
		done := make(chan struct{})
		turns[i] = commitTurn{previous: w.commitTails[key], done: done}
		w.commitTails[key] = done
	}
	return turns
}

func finishTurn(turn commitTurn) { close(turn.done) }

// Flush writes entries to S3 and registers segment metadata.
//
// The object is immutable and uploaded before offsets exist. Each contained
// batch is then committed independently by a stable physical identity. This is
// deliberately the inverse of allocate-then-upload: an upload failure can only
// leave an orphan object, never a visible offset hole.
func (w *Writer) Flush(ctx context.Context, entries []BufferEntry) error {
	if len(entries) == 0 {
		return nil
	}
	turns := w.reserveCommitTurns(entries)

	// 1. Read NumRecords and producer metadata from each raw RecordBatch.
	batches := make([]pendingBatch, len(entries))
	for i, e := range entries {
		hdr, err := log.ReadRecordBatchHeader(e.Batch)
		if err != nil {
			w.sendError(entries, fmt.Errorf("read header for entry %d: %w", i, err))
			for _, turn := range turns {
				finishTurn(turn)
			}
			return err
		}
		producerID := hdr.ProducerID
		if producerID <= 0 {
			// Non-idempotent batches are encoded with ProducerID = -1.
			producerID = 0
		}
		batches[i] = pendingBatch{
			topic: e.Topic, partition: e.Partition, count: int(hdr.NumRecords),
			producerID: producerID, sequence: int64(hdr.FirstSequence),
		}
	}

	// 2. Concatenate raw batches, tracking immutable byte locations.
	totalSize := 0
	for _, e := range entries {
		totalSize += len(e.Batch)
	}
	data := make([]byte, 0, totalSize)
	for i, e := range entries {
		byteOffset := int64(len(data))
		data = append(data, e.Batch...)
		batches[i].byteOffset, batches[i].byteLength = byteOffset, int64(len(e.Batch))
	}

	// 3. Upload before assigning a logical offset. The key is fixed across PUT
	// retries; a failed PUT cannot change metadata.
	seq := w.seq.Add(1) - 1
	fileKey := fmt.Sprintf("_diskless/%s/%d-%d.data", w.nodeID, time.Now().UnixMilli(), seq)
	backoff := 100 * time.Millisecond
	var uploadErr error
	for {
		if err := w.s3.Put(ctx, fileKey, data, storage.PutOpts{}); err == nil {
			break
		} else {
			uploadErr = err
		}
		select {
		case <-ctx.Done():
			w.sendError(entries, fmt.Errorf("diskless upload phase file_key=%s: %w (last upload error: %v)", fileKey, ctx.Err(), uploadErr))
			for _, turn := range turns {
				finishTurn(turn)
			}
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxFlushRetryBackoff {
			backoff *= 2
		}
	}

	// 4. Commits are ordered within a partition. In particular, producer
	// sequences are ordered by submission, not by S3 completion. Different
	// partitions commit concurrently after the common upload completes.
	const maxConcurrentCommits = 16
	sem := make(chan struct{}, maxConcurrentCommits)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	created := time.Now()
	byPartition := make(map[string][]int)
	for i, e := range entries {
		byPartition[partitionKey(e.Topic, e.Partition)] = append(byPartition[partitionKey(e.Topic, e.Partition)], i)
	}
	for _, indexes := range byPartition {
		indexes := indexes
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, i := range indexes {
				e := entries[i]
				turn := turns[i]
				if turn.previous != nil {
					select {
					case <-turn.previous:
					case <-ctx.Done():
						e.Done <- FlushResult{Err: ctx.Err()}
						finishTurn(turn)
						continue
					}
				}
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					e.Done <- FlushResult{Err: ctx.Err()}
					finishTurn(turn)
					continue
				}
				pending := batches[i]
				batch := UploadedBatch{BatchID: fmt.Sprintf("%s:%d:%d", fileKey, pending.byteOffset, pending.byteLength), FileKey: fileKey, Topic: pending.topic, Partition: pending.partition, Count: pending.count, ProducerID: pending.producerID, Sequence: pending.sequence, ByteOffset: pending.byteOffset, ByteLength: pending.byteLength, CreatedAt: created}
				result, err := w.meta.CommitUploadedBatches(ctx, []UploadedBatch{batch})
				<-sem
				if err != nil || len(result) != 1 {
					if err == nil {
						err = fmt.Errorf("commit uploaded batch %s: missing result", batch.BatchID)
					}
					err = fmt.Errorf("diskless commit phase batch_id=%s topic=%s partition=%d: %w", batch.BatchID, e.Topic, e.Partition, err)
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					e.Done <- FlushResult{Err: err}
					finishTurn(turn)
					continue
				}
				e.Done <- FlushResult{BaseOffset: result[0].BaseOffset, Duplicate: result[0].Duplicate}
				finishTurn(turn)
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (w *Writer) sendError(entries []BufferEntry, err error) {
	for _, e := range entries {
		e.Done <- FlushResult{Err: err}
	}
}
