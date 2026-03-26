package log

import "sort"

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
