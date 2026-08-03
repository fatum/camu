package replication

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/maksim/camu/internal/log"
	"golang.org/x/net/http2"
)

// PartitionManager is the interface the fetcher needs from the server.
type PartitionManager interface {
	AppendReplicatedRawBatch(ctx context.Context, topic string, pid int, batch []byte) error
	TruncateLogFrom(topic string, pid int, offset uint64) error
	SyncFollowerSealedPrefix(ctx context.Context, topic string, pid int, activeBase uint64) uint64
	UpdateFollowerProgress(topic string, pid int, leaderEpoch, highWatermark, flushedOffset uint64)
}

// FetchResponse holds parsed response from leader.
type FetchResponse struct {
	TruncateTo     uint64
	HasTruncate    bool
	HighWatermark  uint64
	LeaderEpoch    uint64
	HasLeaderEpoch bool
	FlushedOffset  uint64
	ActiveBase     uint64
}

// fetchedBatches is the outcome of one fetch. Batches are appended while the
// response body is read, so the follower never holds an entire replica fetch
// response in memory.
type fetchedBatches struct {
	response   FetchResponse
	lastOffset uint64
	hasBatches bool
}

// OnLeaderDown is called when the follower detects leader failure.
type OnLeaderDown func(topic string, pid int)

// FollowerFetcher continuously pulls messages from the leader for a given
// topic/partition and applies them to the local PartitionManager.
type FollowerFetcher struct {
	httpClient   *http.Client
	onLeaderDown OnLeaderDown
}

// NewFollowerFetcher creates a FollowerFetcher with a shared HTTP client and
// a leader-down callback. The client should be created via NewH2CClient so
// that all partition fetches to the same leader multiplex over one connection.
func NewFollowerFetcher(httpClient *http.Client, onLeaderDown OnLeaderDown) *FollowerFetcher {
	return &FollowerFetcher{
		httpClient:   httpClient,
		onLeaderDown: onLeaderDown,
	}
}

// NewH2CClient creates an HTTP client that speaks h2c (HTTP/2 without TLS).
// A single client should be shared across all fetchers to multiplex
// partition fetches over one connection per leader.
func NewH2CClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
}

// Run starts the fetch loop. It blocks until ctx is cancelled or the leader is
// considered down (more than 5 consecutive errors). localOffset is the next
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

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		slog.Debug("fetcher: fetch cycle",
			"topic", topic, "pid", pid, "offset", localOffset,
			"epoch", localEpoch, "leader", leaderAddr)
		result, err := f.fetchFromLeader(ctx, leaderAddr, topic, pid, localOffset, localEpoch, instanceID, func(batch []byte) error {
			return pm.AppendReplicatedRawBatch(ctx, topic, pid, batch)
		})
		// A response may end after valid batches have been appended. Advance the
		// local offset before retrying so a broken HTTP stream cannot duplicate
		// those batches on the next request.
		if result != nil && result.hasBatches {
			localOffset = result.lastOffset + 1
		}
		if err != nil {
			isNotReady := strings.Contains(err.Error(), "404")
			if isNotReady {
				// Leader hasn't initialized partition yet — wait and retry
				// without counting toward leader-down threshold.
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
				slog.Error("fetcher: too many consecutive errors, declaring leader down",
					"topic", topic, "pid", pid, "errors", consecutiveErrors)
				if f.onLeaderDown != nil {
					f.onLeaderDown(topic, pid)
				}
				return
			}
			continue
		}

		// Success — reset error tracking.
		backoff = 100 * time.Millisecond
		consecutiveErrors = 0
		resp := &result.response

		// Handle divergence: truncate before appending anything.
		if resp.HasTruncate {
			if err := pm.TruncateLogFrom(topic, pid, resp.TruncateTo); err != nil {
				slog.Warn("fetcher: TruncateLogFrom failed",
					"topic", topic, "pid", pid, "truncateTo", resp.TruncateTo, "err", err)
			}
			localOffset = resp.TruncateTo
			if resp.HasLeaderEpoch {
				// The leader chose this exact epoch for the truncation boundary.
				// It may be lower when a follower reports an epoch unknown to the
				// leader, so this is deliberately not a monotonic update.
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
			// Sealed segments are already durable in shared storage. Do not copy
			// them from the leader: refresh the local index and resume from the
			// leader's active-segment base instead.
			if syncedOffset := pm.SyncFollowerSealedPrefix(ctx, topic, pid, resp.ActiveBase); syncedOffset > localOffset {
				localOffset = syncedOffset
			}
		}

		if resp.LeaderEpoch > localEpoch {
			localEpoch = resp.LeaderEpoch
		}
		pm.UpdateFollowerProgress(topic, pid, localEpoch, resp.HighWatermark, resp.FlushedOffset)

		// A successful empty response means the follower has caught up. Without
		// a delay, it immediately polls again and creates a tight request loop
		// across every follower partition.
		if !result.hasBatches {
			select {
			case <-ctx.Done():
				return
			case <-time.After(caughtUpPollInterval):
			}
		}
	}
}

// fetchFromLeader performs a single HTTP GET to the leader's replication
// endpoint and returns the parsed FetchResponse.
func (f *FollowerFetcher) fetchFromLeader(
	ctx context.Context,
	leaderAddr string,
	topic string,
	pid int,
	offset uint64,
	epoch uint64,
	instanceID string,
	appendBatch func([]byte) error,
) (*fetchedBatches, error) {
	url := fmt.Sprintf("http://%s/v1/internal/replicate/%s/%d?from_offset=%d",
		leaderAddr, topic, pid, offset)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("fetcher: build request: %w", err)
	}
	req.Header.Set("X-Replica-ID", instanceID)
	req.Header.Set("X-Replica-Offset", strconv.FormatUint(offset, 10))
	req.Header.Set("X-Replica-Epoch", strconv.FormatUint(epoch, 10))

	httpResp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetcher: http do: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusNotFound {
		// 404 = leader hasn't initialized this partition yet (startup race).
		// Retry without counting as a leader-down signal.
		return nil, fmt.Errorf("fetcher: partition not ready on leader (404)")
	}
	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetcher: leader returned status %d", httpResp.StatusCode)
	}

	var result fetchedBatches
	fr := &result.response

	if v := httpResp.Header.Get("X-Truncate-To"); v != "" {
		fr.TruncateTo, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fetcher: parse X-Truncate-To: %w", err)
		}
		fr.HasTruncate = true
	}
	if v := httpResp.Header.Get("X-High-Watermark"); v != "" {
		fr.HighWatermark, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fetcher: parse X-High-Watermark: %w", err)
		}
	}
	if v := httpResp.Header.Get("X-Leader-Epoch"); v != "" {
		fr.LeaderEpoch, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fetcher: parse X-Leader-Epoch: %w", err)
		}
		fr.HasLeaderEpoch = true
	}
	if v := httpResp.Header.Get("X-Flushed-Offset"); v != "" {
		fr.FlushedOffset, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fetcher: parse X-Flushed-Offset: %w", err)
		}
	}
	if v := httpResp.Header.Get("X-Active-Base"); v != "" {
		fr.ActiveBase, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fetcher: parse X-Active-Base: %w", err)
		}
	}

	if fr.HasTruncate {
		return &result, nil
	}
	if appendBatch == nil {
		return nil, fmt.Errorf("fetcher: append callback is required")
	}
	if err := readReplicaBatches(httpResp.Body, offset, func(batch []byte, header log.RecordBatchHeader) error {
		if err := appendBatch(batch); err != nil {
			return err
		}
		result.lastOffset = uint64(header.LastOffset())
		result.hasBatches = true
		return nil
	}); err != nil {
		return &result, err
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
