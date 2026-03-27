package log

import (
	"fmt"
	"os"
	"sort"
)

// ReadFrom reads messages from the WAL starting at startOffset, up to limit messages.
func (w *WAL) ReadFrom(startOffset uint64, limit int) ([]Message, error) {
	if limit <= 0 {
		return nil, nil
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	result := make([]Message, 0, limit)
	for _, chunk := range chunks[startIdx:] {
		msgs, err := readChunkMessages(w.chunkPathLocked(chunk), startOffset, limit-len(result))
		if err != nil {
			return nil, err
		}
		result = append(result, msgs...)
		if len(result) >= limit {
			return result[:limit], nil
		}
	}

	return result, nil
}

// ReadBatchesFrom reads batch envelopes from the WAL starting at startOffset.
// Unlike ReadFrom, it preserves batch boundaries and producer metadata.
func (w *WAL) ReadBatchesFrom(startOffset uint64) ([]Batch, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	var result []Batch
	for _, chunk := range chunks[startIdx:] {
		batches, err := readChunkBatches(w.chunkPathLocked(chunk), startOffset)
		if err != nil {
			return nil, err
		}
		result = append(result, batches...)
	}
	return result, nil
}

// ReadBatchMetasFrom reads batch metadata from the WAL starting at startOffset
// without materializing full message payloads.
func (w *WAL) ReadBatchMetasFrom(startOffset uint64) ([]BatchMeta, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	chunks := append([]walChunk(nil), w.chunks...)
	startIdx := sort.Search(len(chunks), func(i int) bool { return chunks[i].maxOffset >= startOffset })

	var result []BatchMeta
	for _, chunk := range chunks[startIdx:] {
		metas, err := readChunkBatchMetas(w.chunkPathLocked(chunk), startOffset)
		if err != nil {
			return nil, err
		}
		result = append(result, metas...)
	}
	return result, nil
}

// readChunkBatches reads batch envelopes from a single chunk file, returning
// only batches that contain at least one message at or above startOffset.
func readChunkBatches(path string, startOffset uint64) ([]Batch, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat WAL chunk %q: %w", path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open WAL chunk %q: %w", path, err)
	}
	defer f.Close()

	var batches []Batch
	_, _, _, err = scanWALBatches(f, info.Size(), 0, func(b Batch) bool {
		// Include batch if its last message offset >= startOffset.
		if len(b.Messages) > 0 && b.Messages[len(b.Messages)-1].Offset >= startOffset {
			batches = append(batches, b)
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("read WAL chunk batches %q: %w", path, err)
	}
	return batches, nil
}

func readChunkBatchMetas(path string, startOffset uint64) ([]BatchMeta, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat WAL chunk %q: %w", path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open WAL chunk %q: %w", path, err)
	}
	defer f.Close()

	var metas []BatchMeta
	_, _, _, err = scanWALBatchMetas(f, info.Size(), 0, func(meta BatchMeta) bool {
		if meta.MessageCount > 0 && meta.LastOffset >= startOffset {
			metas = append(metas, meta)
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("read WAL chunk batch metas %q: %w", path, err)
	}
	return metas, nil
}
