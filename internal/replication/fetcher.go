package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/maksim/camu/internal/log"
)

const maxReplicaFetchBytes = 1 << 20

// ErrPartitionNotReady indicates the leader has not yet initialized the
// partition. The fetcher retries without counting toward the leader-down
// threshold.
var ErrPartitionNotReady = errors.New("partition not ready on leader")

// PartitionManager is the interface the fetcher needs from the server.
type PartitionManager interface {
	AppendReplicatedBatchStream(topic string, pid int, hdr log.RecordBatchHeader, headerBytes []byte, body io.Reader, bodySize int64) error
	TruncateLogFrom(topic string, pid int, offset uint64) error
	SyncFollowerSealedPrefix(ctx context.Context, topic string, pid int, activeBase uint64) uint64
	UpdateFollowerProgress(topic string, pid int, leaderEpoch, highWatermark, flushedOffset uint64)
}

// FetchResponse holds parsed response metadata from the leader.
type FetchResponse struct {
	TruncateTo     uint64
	HasTruncate    bool
	HighWatermark  uint64
	LeaderEpoch    uint64
	HasLeaderEpoch bool
	FlushedOffset  uint64
	ActiveBase     uint64
}

// fetchedBatches is the outcome of one fetch cycle.
type fetchedBatches struct {
	response   FetchResponse
	lastOffset uint64
	hasBatches bool
}

// OnLeaderDown is called when the follower detects leader failure.
type OnLeaderDown func(topic string, pid int)

// FollowerFetcher continuously pulls RecordBatches from the leader over a
// raw TCP connection and applies them to the local PartitionManager.
type FollowerFetcher struct {
	onLeaderDown OnLeaderDown
	readTimeout  time.Duration
}

// NewFollowerFetcher creates a FollowerFetcher with a leader-down callback
// and a read timeout applied to each fetch cycle. The timeout covers the
// leader's long-polling window plus network RTT; if the leader stops
// responding mid-fetch the connection is closed and the error counts toward
// leader-down detection.
func NewFollowerFetcher(onLeaderDown OnLeaderDown, readTimeout time.Duration) *FollowerFetcher {
	if readTimeout <= 0 {
		readTimeout = 30 * time.Second
	}
	return &FollowerFetcher{
		onLeaderDown: onLeaderDown,
		readTimeout:  readTimeout,
	}
}

// Run starts the fetch loop. It blocks until ctx is cancelled or the leader is
// considered down (more than 10 consecutive errors). localOffset is the next
// offset to request; localEpoch is the follower's current epoch.
func (f *FollowerFetcher) Run(
	ctx context.Context,
	topic string,
	pid int,
	leaderAddr string,
	localOffset uint64,
	localEpoch uint64,
	instanceID string,
	pm PartitionManager,
) {
	const maxBackoff = 5 * time.Second
	const caughtUpPollInterval = 100 * time.Millisecond
	backoff := 100 * time.Millisecond
	consecutiveErrors := 0
	var correlationID int32

	var conn net.Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if conn == nil {
			var d net.Dialer
			c, err := d.DialContext(ctx, "tcp", leaderAddr)
			if err != nil {
				slog.Warn("fetcher: dial leader",
					"topic", topic, "pid", pid, "leader", leaderAddr, "err", err)
				consecutiveErrors++
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				if consecutiveErrors > 10 {
					f.declareLeaderDown(topic, pid, consecutiveErrors)
					return
				}
				continue
			}
			conn = c
			slog.Debug("fetcher: connected to leader",
				"topic", topic, "pid", pid, "leader", leaderAddr)
		}

		slog.Debug("fetcher: fetch cycle",
			"topic", topic, "pid", pid, "offset", localOffset,
			"epoch", localEpoch, "leader", leaderAddr)

		correlationID++
		if err := conn.SetReadDeadline(time.Now().Add(f.readTimeout)); err != nil {
			conn.Close()
			conn = nil
			consecutiveErrors++
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		result, err := f.fetchFromLeader(conn, correlationID, topic, pid, localOffset, localEpoch, instanceID, pm)
		conn.SetReadDeadline(time.Time{})

		if result != nil && result.hasBatches {
			localOffset = result.lastOffset + 1
		}
		if err != nil {
			conn.Close()
			conn = nil
			if errors.Is(err, ErrPartitionNotReady) {
				slog.Debug("fetcher: partition not ready on leader, retrying",
					"topic", topic, "pid", pid)
			} else {
				slog.Warn("fetcher: fetch error",
					"topic", topic, "pid", pid, "err", err)
				consecutiveErrors++
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			if consecutiveErrors > 10 {
				f.declareLeaderDown(topic, pid, consecutiveErrors)
				return
			}
			continue
		}

		backoff = 100 * time.Millisecond
		consecutiveErrors = 0
		resp := &result.response

		if resp.HasTruncate {
			if err := pm.TruncateLogFrom(topic, pid, resp.TruncateTo); err != nil {
				slog.Warn("fetcher: TruncateLogFrom failed",
					"topic", topic, "pid", pid, "truncateTo", resp.TruncateTo, "err", err)
			}
			localOffset = resp.TruncateTo
			if resp.HasLeaderEpoch {
				localEpoch = resp.LeaderEpoch
			}
			pm.UpdateFollowerProgress(topic, pid, localEpoch, resp.HighWatermark, resp.FlushedOffset)
			continue
		}

		if result.hasBatches {
			slog.Debug("fetcher: replicated raw batches",
				"topic", topic, "pid", pid,
				"last_offset", result.lastOffset,
				"leader_hw", resp.HighWatermark)
		}
		if resp.ActiveBase > 0 {
			if syncedOffset := pm.SyncFollowerSealedPrefix(ctx, topic, pid, resp.ActiveBase); syncedOffset > localOffset {
				localOffset = syncedOffset
			}
		}

		if resp.LeaderEpoch > localEpoch {
			localEpoch = resp.LeaderEpoch
		}
		pm.UpdateFollowerProgress(topic, pid, localEpoch, resp.HighWatermark, resp.FlushedOffset)

		if !result.hasBatches {
			select {
			case <-ctx.Done():
				return
			case <-time.After(caughtUpPollInterval):
			}
		}
	}
}

func (f *FollowerFetcher) declareLeaderDown(topic string, pid int, errors int) {
	slog.Error("fetcher: too many consecutive errors, declaring leader down",
		"topic", topic, "pid", pid, "errors", errors)
	if f.onLeaderDown != nil {
		f.onLeaderDown(topic, pid)
	}
}

// fetchFromLeader sends a ReplicaFetchRequest over the persistent TCP
// connection and streams the response batches directly to the partition
// manager, avoiding materializing the full response in memory.
func (f *FollowerFetcher) fetchFromLeader(
	conn net.Conn,
	correlationID int32,
	topic string,
	pid int,
	offset uint64,
	epoch uint64,
	instanceID string,
	pm PartitionManager,
) (*fetchedBatches, error) {
	req := &ReplicaFetchRequest{
		CorrelationID: correlationID,
		Topic:         topic,
		PartitionID:   int32(pid),
		FromOffset:    offset,
		ReplicaID:     instanceID,
		ReplicaOffset: offset,
		ReplicaEpoch:  epoch,
		MaxBytes:      maxReplicaFetchBytes,
	}

	if _, err := conn.Write(EncodeRequest(req)); err != nil {
		return nil, fmt.Errorf("fetcher: write request: %w", err)
	}

	resp, batchLen, err := ReadResponseHeader(conn)
	if err != nil {
		return nil, fmt.Errorf("fetcher: read response: %w", err)
	}

	if resp.ErrorCode == ReplicaErrNotFound {
		drainBatchData(conn, batchLen)
		return nil, ErrPartitionNotReady
	}
	if resp.ErrorCode == ReplicaErrTruncate {
		drainBatchData(conn, batchLen)
		var result fetchedBatches
		fr := &result.response
		fr.TruncateTo = resp.TruncateTo
		fr.HasTruncate = true
		fr.LeaderEpoch = resp.LeaderEpoch
		fr.HasLeaderEpoch = true
		fr.HighWatermark = resp.HighWatermark
		return &result, nil
	}
	if resp.ErrorCode != ReplicaErrOK {
		drainBatchData(conn, batchLen)
		return nil, fmt.Errorf("fetcher: leader returned error code %d", resp.ErrorCode)
	}

	var result fetchedBatches
	fr := &result.response
	fr.HighWatermark = resp.HighWatermark
	fr.LeaderEpoch = resp.LeaderEpoch
	fr.HasLeaderEpoch = true
	fr.FlushedOffset = resp.FlushedOffset
	fr.ActiveBase = resp.ActiveBase

	if batchLen > 0 {
		if err := streamReplicaBatches(conn, batchLen, offset, func(hdr log.RecordBatchHeader, headerBytes []byte, body io.Reader, bodySize int64) error {
			if err := pm.AppendReplicatedBatchStream(topic, pid, hdr, headerBytes, body, bodySize); err != nil {
				return err
			}
			result.lastOffset = uint64(hdr.LastOffset())
			result.hasBatches = true
			return nil
		}); err != nil {
			return &result, err
		}
	}

	return &result, nil
}

// drainBatchData reads and discards batchLen bytes from r so the connection
// remains in a clean state for the next request.
func drainBatchData(r io.Reader, batchLen int32) {
	if batchLen <= 0 {
		return
	}
	_, _ = io.CopyN(io.Discard, r, int64(batchLen))
}

const maxReplicaBatchBytes = 16 << 20

// streamReplicaBatches reads self-framing RecordBatches from r, streaming
// each batch body directly to the append callback without materializing the
// full batch in memory. totalLen is the total remaining bytes to read.
func streamReplicaBatches(r io.Reader, totalLen int32, requestedOffset uint64, appendBatch func(hdr log.RecordBatchHeader, headerBytes []byte, body io.Reader, bodySize int64) error) error {
	remaining := totalLen
	for remaining > 0 {
		var headerBytes [log.RecordBatchHeaderSize]byte
		if int32(log.RecordBatchHeaderSize) > remaining {
			return fmt.Errorf("fetcher: batch header exceeds remaining data: %d > %d", log.RecordBatchHeaderSize, remaining)
		}
		if _, err := io.ReadFull(r, headerBytes[:]); err != nil {
			return fmt.Errorf("fetcher: read batch header: %w", err)
		}
		remaining -= int32(log.RecordBatchHeaderSize)

		header, err := log.ReadRecordBatchHeader(headerBytes[:])
		if err != nil {
			return fmt.Errorf("fetcher: parse batch header: %w", err)
		}
		batchSize := int32(header.RecordBatchSize())
		if batchSize < log.RecordBatchHeaderSize || batchSize > maxReplicaBatchBytes {
			return fmt.Errorf("fetcher: invalid replica batch size %d", batchSize)
		}
		bodySize := int64(batchSize - log.RecordBatchHeaderSize)
		if bodySize > int64(remaining) {
			return fmt.Errorf("fetcher: batch body exceeds remaining data: %d > %d", bodySize, remaining)
		}

		// Skip batches before the requested offset.
		if uint64(header.LastOffset()) < requestedOffset {
			if _, err := io.CopyN(io.Discard, r, bodySize); err != nil {
				return fmt.Errorf("fetcher: skip batch body: %w", err)
			}
			remaining -= int32(bodySize)
			continue
		}

		bodyReader := io.LimitReader(r, bodySize)
		if err := appendBatch(header, headerBytes[:], bodyReader, bodySize); err != nil {
			return fmt.Errorf("fetcher: append raw batch: %w", err)
		}
		remaining -= int32(bodySize)
	}
	return nil
}
