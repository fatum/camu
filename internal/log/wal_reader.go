package log

// ReadFrom reads messages from the WAL starting at startOffset, up to limit messages.
// Uses the internal replayLocked() to read all entries, then filters.
func (w *WAL) ReadFrom(startOffset uint64, limit int) ([]Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	all, err := w.replayLocked()
	if err != nil {
		return nil, err
	}
	var result []Message
	for _, msg := range all {
		if msg.Offset >= startOffset {
			result = append(result, msg)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}
