package replication

import (
	"bytes"
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
	AppendReplicatedRawBatch(ctx context.Context, topic string, pid int, batch []byte) error
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
}

// NewFollowerFetcher creates a FollowerFetcher with a leader-down callback.
func NewFollowerFetcher(onLeaderDown OnLeaderDown) *FollowerFetcher {
	return &FollowerFetcher{
		onLeaderDown: onLeaderDown,
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
		result, err := f.fetchFromLeader(conn, correlationID, topic, pid, localOffset, localEpoch, instanceID, func(batch []byte) error {
			return pm.AppendReplicatedRawBatch(ctx, topic, pid, batch)
		})

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
// connection and parses the response.
func (f *FollowerFetcher) fetchFromLeader(
	conn net.Conn,
	correlationID int32,
	topic string,
	pid int,
	offset uint64,
	epoch uint64,
	instanceID string,
	appendBatch func([]byte) error,
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

	resp, err := ReadResponse(conn)
	if err != nil {
		return nil, fmt.Errorf("fetcher: read response: %w", err)
	}

	if resp.ErrorCode == ReplicaErrNotFound {
		return nil, ErrPartitionNotReady
	}
	if resp.ErrorCode == ReplicaErrTruncate {
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
		return nil, fmt.Errorf("fetcher: leader returned error code %d", resp.ErrorCode)
	}

	var result fetchedBatches
	fr := &result.response
	fr.HighWatermark = resp.HighWatermark
	fr.LeaderEpoch = resp.LeaderEpoch
	fr.HasLeaderEpoch = true
	fr.FlushedOffset = resp.FlushedOffset
	fr.ActiveBase = resp.ActiveBase

	if len(resp.BatchData) > 0 {
		if err := readReplicaBatches(bytes.NewReader(resp.BatchData), offset, func(batch []byte, header log.RecordBatchHeader) error {
			if err := appendBatch(batch); err != nil {
				return err
			}
			result.lastOffset = uint64(header.LastOffset())
			result.hasBatches = true
			return nil
		}); err != nil {
			return &result, err
		}
	}

	return &result, nil
}

const maxReplicaBatchBytes = 16 << 20

// readReplicaBatches reads the concatenated RecordBatch stream one batch at a
// time. The protocol is self-framing: each batch has its total length in the
// first 12 bytes, so no response-sized buffer is needed.
func readReplicaBatches(r io.Reader, requestedOffset uint64, appendBatch func([]byte, log.RecordBatchHeader) error) error {
	for {
		var headerBytes [log.RecordBatchHeaderSize]byte
		_, err := io.ReadFull(r, headerBytes[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("fetcher: read batch header: %w", err)
		}
		header, err := log.ReadRecordBatchHeader(headerBytes[:])
		if err != nil {
			return fmt.Errorf("fetcher: parse batch header: %w", err)
		}
		batchSize := int(header.RecordBatchSize())
		if batchSize < log.RecordBatchHeaderSize || batchSize > maxReplicaBatchBytes {
			return fmt.Errorf("fetcher: invalid replica batch size %d", batchSize)
		}
		batch := make([]byte, batchSize)
		copy(batch, headerBytes[:])
		if _, err := io.ReadFull(r, batch[log.RecordBatchHeaderSize:]); err != nil {
			return fmt.Errorf("fetcher: read batch body: %w", err)
		}
		if uint64(header.LastOffset()) < requestedOffset {
			continue
		}
		if err := appendBatch(batch, header); err != nil {
			return fmt.Errorf("fetcher: append raw batch: %w", err)
		}
	}
}
