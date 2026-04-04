package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	logstore "github.com/maksim/camu/internal/log"
)

func (s *Server) streamMessagesJSON(ctx context.Context, w http.ResponseWriter, topicName string, partitionID int, startOffset uint64, limit int, index *logstore.Index, ps *partitionState) (int, uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var readableHW uint64
	if ps != nil {
		ps.mu.RLock()
		hw, ok := readableHighWatermark(ps)
		ps.mu.RUnlock()
		if ok {
			readableHW = hw
		}
	}

	segmentIter := startConsumeIterator(ctx, func(visit func(logstore.Message) bool) error {
		_, err := s.fetcher.Walk(ctx, index, topicName, partitionID, startOffset, limit, visit)
		return err
	})

	// Active segment iterator: reads raw RecordBatch bytes from the in-memory
	// active segment and decodes them into messages. Data written via
	// AppendRawBatch lives here until the segment is sealed and flushed to S3.
	var activeIter *consumeIterator
	if ps != nil {
		ps.mu.RLock()
		activeSeg := ps.activeSegment
		ps.mu.RUnlock()
		if activeSeg != nil {
			activeIter = startConsumeIterator(ctx, func(visit func(logstore.Message) bool) error {
				offsetIdx := activeSeg.OffsetIndex()
				for _, entry := range offsetIdx {
					if readableHW > 0 && uint64(entry.BaseOffset) >= readableHW {
						break
					}
					if uint64(entry.LastOffset) < startOffset {
						continue
					}
					if entry.BatchSize <= 0 || entry.Position < 0 {
						continue
					}
					buf := make([]byte, entry.BatchSize)
					n, err := activeSeg.ReadAt(buf, entry.Position)
					if err != nil && n < int(entry.BatchSize) {
						break
					}
					msgs, err := logstore.DecodeRecordBatch(buf[:n])
					if err != nil {
						break
					}
					for _, m := range msgs {
						if m.Offset < startOffset {
							continue
						}
						if readableHW > 0 && m.Offset >= readableHW {
							return nil
						}
						if !visit(m) {
							return nil
						}
					}
				}
				return nil
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"messages":[`))
	enc := json.NewEncoder(w)

	written := 0
	nextOffset := startOffset
	writeMsg := func(m logstore.Message) error {
		if written > 0 {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		if err := enc.Encode(consumedMessage{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       string(m.Key),
			Value:     tryString(m.Value),
			Headers:   m.Headers,
		}); err != nil {
			return err
		}
		written++
		nextOffset = m.Offset + 1
		return nil
	}

	// pickLowest returns the message with the lowest offset from all non-nil
	// sources, popping the chosen iterator. Returns nil when all are exhausted.
	pickLowest := func() (*logstore.Message, error) {
		segMsg, err := segmentIter.peek()
		if err != nil {
			return nil, err
		}
		var activeMsg *logstore.Message
		if activeIter != nil {
			activeMsg, err = activeIter.peek()
			if err != nil {
				return nil, err
			}
		}
		// Find the candidate with the lowest offset. When offsets tie,
		// prefer active > sealed (active data is fresher).
		type candidate struct {
			msg  *logstore.Message
			pop  func()
			prio int // lower = prefer when offsets tie
		}
		candidates := []candidate{
			{activeMsg, func() { activeIter.pop() }, 0},
			{segMsg, func() { segmentIter.pop() }, 1},
		}

		var best *candidate
		for i := range candidates {
			c := &candidates[i]
			if c.msg == nil {
				continue
			}
			if best == nil || c.msg.Offset < best.msg.Offset || (c.msg.Offset == best.msg.Offset && c.prio < best.prio) {
				best = c
			}
		}
		if best == nil {
			return nil, nil
		}

		msg := best.msg
		chosenOffset := msg.Offset
		best.pop()

		// Pop duplicates at the same offset from other iterators.
		for i := range candidates {
			c := &candidates[i]
			if c.msg != nil && c.msg.Offset == chosenOffset && c.msg != msg {
				c.pop()
			}
		}
		return msg, nil
	}

	for written < limit {
		msg, err := pickLowest()
		if err != nil {
			return written, nextOffset, err
		}
		if msg == nil {
			break
		}
		if err := writeMsg(*msg); err != nil {
			return written, nextOffset, err
		}
	}

	_, _ = w.Write([]byte(`],"next_offset":`))
	_, _ = w.Write([]byte(strconv.FormatUint(nextOffset, 10)))
	_, _ = w.Write([]byte("}"))
	return written, nextOffset, nil
}
