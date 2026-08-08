package diskless

import (
	"context"
	"fmt"
	"io"
	"strconv"
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

// DisklessShardCount is the number of key prefixes under _diskless/ that raw
// flush objects are spread across (seq % DisklessShardCount). The orphan sweep
// lists these fixed shards in parallel instead of listing every flush object at
// once, so listing cost and page count stay bounded as the log grows.
const DisklessShardCount = 64

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

// commitChunk is a group of same-partition batch indexes committed together in
// a single CommitUploadedBatches call.
type commitChunk struct {
	topic     string
	partition int
	indexes   []int
}

// maxBatchesPerCommit bounds how many same-partition batches one commit
// invocation may carry, keeping each DynamoDB transaction under its 100-item
// limit (one shared offsets update plus one segment ref per batch).
const maxBatchesPerCommit = 25

// maxFlushRetryBackoff bounds the retry backoff when materializing a flushed
// batch to object storage.
const maxFlushRetryBackoff = 5 * time.Second

// batchConcatReader exposes the raw RecordBatches of a flush as one contiguous
// byte stream without copying them into a single buffer. reset rewinds it, so
// upload retries re-send the same immutable bytes.
type batchConcatReader struct {
	entries []BufferEntry
	i, off  int
}

func newBatchConcatReader(entries []BufferEntry) *batchConcatReader {
	return &batchConcatReader{entries: entries}
}

func (r *batchConcatReader) reset() { r.i, r.off = 0, 0 }

func (r *batchConcatReader) Read(p []byte) (int, error) {
	for r.i < len(r.entries) {
		b := r.entries[r.i].Batch
		if r.off < len(b) {
			n := copy(p, b[r.off:])
			r.off += n
			return n, nil
		}
		r.i++
		r.off = 0
	}
	return 0, io.EOF
}

// NewWriter creates a Writer that flushes to s3 and registers with meta.
func NewWriter(s3 *storage.S3Client, meta MetaStore, nodeID string) *Writer {
	return &Writer{s3: s3, meta: meta, nodeID: nodeID, commitTails: make(map[string]chan struct{})}
}

// reserveCommitTurns establishes partition submission order before any upload
// begins. Uploads can then complete in any order, but a later chunk cannot
// validate its producer sequence before its predecessor commits or fails.
func (w *Writer) reserveCommitTurns(chunks []commitChunk) []commitTurn {
	w.commitMu.Lock()
	defer w.commitMu.Unlock()
	turns := make([]commitTurn, len(chunks))
	for i, c := range chunks {
		key := partitionKey(c.topic, c.partition)
		done := make(chan struct{})
		turns[i] = commitTurn{previous: w.commitTails[key], done: done}
		w.commitTails[key] = done
	}
	return turns
}

func finishTurn(turn commitTurn) { close(turn.done) }

// buildCommitChunks groups entries by partition in submission order, splitting
// each partition into runs of at most maxBatchesPerCommit.
func buildCommitChunks(entries []BufferEntry) []commitChunk {
	groups := make(map[string][]int)
	order := make([]string, 0, len(entries))
	for i, e := range entries {
		pk := partitionKey(e.Topic, e.Partition)
		if _, ok := groups[pk]; !ok {
			order = append(order, pk)
		}
		groups[pk] = append(groups[pk], i)
	}
	var chunks []commitChunk
	for _, pk := range order {
		indexes := groups[pk]
		topic, partition, _ := parsePartitionKey(pk)
		for start := 0; start < len(indexes); start += maxBatchesPerCommit {
			end := start + maxBatchesPerCommit
			if end > len(indexes) {
				end = len(indexes)
			}
			chunks = append(chunks, commitChunk{topic: topic, partition: partition, indexes: indexes[start:end]})
		}
	}
	return chunks
}

// formatBatchID builds the durable physical identity of one batch within a
// flush object: fileKey:byteOffset:byteLength.
func formatBatchID(fileKey string, byteOffset, byteLength int64) string {
	return fileKey + ":" + strconv.FormatInt(byteOffset, 10) + ":" + strconv.FormatInt(byteLength, 10)
}

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
	// Commit turns are reserved before the upload so submission order is
	// established even when a competing flush uploads faster: a later batch can
	// never validate its producer sequence before its predecessor commits or
	// fails.
	chunks := buildCommitChunks(entries)
	turns := w.reserveCommitTurns(chunks)
	finishAllTurns := func() {
		for _, turn := range turns {
			finishTurn(turn)
		}
	}

	// 1. Read NumRecords and producer metadata from each raw RecordBatch.
	batches := make([]pendingBatch, len(entries))
	for i, e := range entries {
		hdr, err := log.ReadRecordBatchHeader(e.Batch)
		if err != nil {
			w.sendError(entries, fmt.Errorf("read header for entry %d: %w", i, err))
			finishAllTurns()
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

	// 2. Track immutable byte locations; the payload is streamed to S3 from
	// the raw entry slices, never copied into a contiguous buffer.
	var totalSize int64
	for i, e := range entries {
		batches[i].byteOffset, batches[i].byteLength = totalSize, int64(len(e.Batch))
		totalSize += int64(len(e.Batch))
	}

	// 3. Upload before assigning a logical offset. The key is fixed across PUT
	// retries; a failed PUT cannot change metadata. The key carries the upload
	// millis and a monotonic per-writer sequence for uniqueness (two flushes in
	// the same millisecond must not collide), and is sharded by seq % N so the
	// orphan sweep can list bounded prefixes in parallel instead of one giant
	// listing. Nothing parses the key back; refs treat it as opaque.
	seq := w.seq.Add(1) - 1
	fileKey := fmt.Sprintf("_diskless/%03d/%s-%d-%d.data", seq%DisklessShardCount, w.nodeID, time.Now().UnixMilli(), seq)
	concat := newBatchConcatReader(entries)
	backoff := 100 * time.Millisecond
	var uploadErr error
	for {
		concat.reset()
		if err := w.s3.PutReader(ctx, fileKey, concat, totalSize, storage.PutOpts{}); err == nil {
			break
		} else {
			uploadErr = err
		}
		select {
		case <-ctx.Done():
			err := fmt.Errorf("%w: diskless upload phase file_key=%s: %v (last upload error: %v)", ErrProduceRetryable, fileKey, ctx.Err(), uploadErr)
			w.sendError(entries, err)
			finishAllTurns()
			return err
		case <-time.After(backoff):
		}
		if backoff < maxFlushRetryBackoff {
			backoff *= 2
		}
	}

	// 4. Commits are ordered within a partition. In particular, producer
	// sequences are ordered by submission, not by S3 completion. Different
	// partitions commit concurrently after the common upload completes, and
	// same-partition batches commit together in one all-or-nothing call.
	const maxConcurrentCommits = 16
	sem := make(chan struct{}, maxConcurrentCommits)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	created := time.Now()
	for ci, c := range chunks {
		ci, c := ci, c
		wg.Add(1)
		go func() {
			defer wg.Done()
			turn := turns[ci]
			if turn.previous != nil {
				select {
				case <-turn.previous:
				case <-ctx.Done():
					w.failChunk(entries, c.indexes, fmt.Errorf("%w: waiting to commit: %v", ErrProduceRetryable, ctx.Err()))
					finishTurn(turn)
					return
				}
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				w.failChunk(entries, c.indexes, fmt.Errorf("%w: waiting for commit slot: %v", ErrProduceRetryable, ctx.Err()))
				finishTurn(turn)
				return
			}
			uploads := make([]UploadedBatch, len(c.indexes))
			for j, i := range c.indexes {
				p := batches[i]
				uploads[j] = UploadedBatch{
					BatchID:    formatBatchID(fileKey, p.byteOffset, p.byteLength),
					FileKey:    fileKey,
					Topic:      p.topic,
					Partition:  p.partition,
					Count:      p.count,
					ProducerID: p.producerID,
					Sequence:   p.sequence,
					ByteOffset: p.byteOffset,
					ByteLength: p.byteLength,
					CreatedAt:  created,
				}
			}
			results, err := w.meta.CommitUploadedBatches(ctx, uploads)
			<-sem
			if err != nil || len(results) != len(c.indexes) {
				if err == nil {
					err = fmt.Errorf("commit uploaded batches: missing result")
				}
				err = fmt.Errorf("diskless commit phase file_key=%s topic=%s partition=%d: %w", fileKey, c.topic, c.partition, err)
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				w.failChunk(entries, c.indexes, err)
				finishTurn(turn)
				return
			}
			for j, i := range c.indexes {
				entries[i].Done <- FlushResult{BaseOffset: results[j].BaseOffset, Duplicate: results[j].Duplicate}
			}
			finishTurn(turn)
		}()
	}
	wg.Wait()
	return firstErr
}

// failChunk reports the same commit error to every produce request in a chunk,
// since a chunk commits atomically or not at all.
func (w *Writer) failChunk(entries []BufferEntry, indexes []int, err error) {
	for _, i := range indexes {
		entries[i].Done <- FlushResult{Err: err}
	}
}

func (w *Writer) sendError(entries []BufferEntry, err error) {
	for _, e := range entries {
		e.Done <- FlushResult{Err: err}
	}
}
