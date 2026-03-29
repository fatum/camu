package replication

import (
	"context"
	"crypto/tls"
	"fmt"
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
	AppendReplicatedBatches(ctx context.Context, topic string, pid int, batches []log.Batch) error
	TruncateWAL(topic string, pid int, beforeOffset uint64) error
	UpdateFollowerProgress(topic string, pid int, highWatermark, flushedOffset uint64)
}

// FetchResponse holds parsed response from leader.
type FetchResponse struct {
	Batches       []log.Batch
	TruncateTo    uint64
	HasTruncate   bool
	HighWatermark uint64
	LeaderEpoch   uint64
	FlushedOffset uint64
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
		resp, err := f.fetchFromLeader(ctx, leaderAddr, topic, pid, localOffset, localEpoch, instanceID)
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

		// Handle divergence: truncate before appending anything.
		if resp.HasTruncate {
			if err := pm.TruncateWAL(topic, pid, resp.TruncateTo); err != nil {
				slog.Warn("fetcher: TruncateWAL failed",
					"topic", topic, "pid", pid, "truncateTo", resp.TruncateTo, "err", err)
			}
			localOffset = resp.TruncateTo
			if resp.LeaderEpoch > localEpoch {
				localEpoch = resp.LeaderEpoch
			}
			pm.UpdateFollowerProgress(topic, pid, resp.HighWatermark, resp.FlushedOffset)
			continue
		}

		// Append new batches (preserving producer metadata for idempotency recovery).
		if len(resp.Batches) > 0 {
			var first, last uint64
			for _, b := range resp.Batches {
				if len(b.Messages) > 0 {
					if first == 0 || b.Messages[0].Offset < first {
						first = b.Messages[0].Offset
					}
					if b.Messages[len(b.Messages)-1].Offset > last {
						last = b.Messages[len(b.Messages)-1].Offset
					}
				}
			}
			if err := pm.AppendReplicatedBatches(ctx, topic, pid, resp.Batches); err != nil {
				slog.Warn("fetcher: AppendReplicatedBatches failed",
					"topic", topic, "pid", pid, "err", err)
			} else {
				slog.Debug("fetcher: replicated batches",
					"topic", topic, "pid", pid,
					"batch_count", len(resp.Batches),
					"offsets", fmt.Sprintf("%d-%d", first, last),
					"leader_hw", resp.HighWatermark)
				localOffset = last + 1
			}
		}

		if resp.LeaderEpoch > localEpoch {
			localEpoch = resp.LeaderEpoch
		}

		// Prune below the leader's flushed offset.
		if resp.FlushedOffset > 0 {
			if err := pm.TruncateWAL(topic, pid, resp.FlushedOffset); err != nil {
				slog.Warn("fetcher: prune TruncateWAL failed",
					"topic", topic, "pid", pid, "flushedOffset", resp.FlushedOffset, "err", err)
			}
		}
		pm.UpdateFollowerProgress(topic, pid, resp.HighWatermark, resp.FlushedOffset)
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
) (*FetchResponse, error) {
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

	var fr FetchResponse

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
	}
	if v := httpResp.Header.Get("X-Flushed-Offset"); v != "" {
		fr.FlushedOffset, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fetcher: parse X-Flushed-Offset: %w", err)
		}
	}

	batches, err := ReadBatchFrames(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("fetcher: read batch frames: %w", err)
	}
	fr.Batches = batches

	return &fr, nil
}
