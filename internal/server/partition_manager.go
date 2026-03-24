package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/fsutil"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/producer"
	"github.com/maksim/camu/internal/replication"
	"github.com/maksim/camu/internal/storage"
)

// partitionState holds per-partition runtime state.
type partitionState struct {
	appendMu      sync.Mutex
	wal           *log.WAL
	index         *log.Index
	indexETag     string // last known ETag of index.json in S3
	nextOffset    uint64
	epoch         uint64                    // always 0 in single-instance mode
	replicaState  *replication.ReplicaState // nil for rf=1
	isLeader      bool
	flushedOffset uint64 // highest offset flushed to S3
	epochHistory  *replication.EpochHistory
	fetchCancel context.CancelFunc // cancel follower fetch goroutine
	fetchDone  chan struct{}       // closed when fetch goroutine exits
}

// PartitionManager manages per-partition state including WAL, index, and batching.
type PartitionManager struct {
	mu          sync.RWMutex
	s3Client    *storage.S3Client
	diskCache   *log.DiskCache
	partitions  map[string]map[int]*partitionState // topic -> partitionID -> state
	routers     map[string]*producer.Router
	batcher     *producer.Batcher
	walDir      string
	walFsync    bool
	segmentsCfg config.SegmentsConfig

	// leaseChecker validates partition ownership before flushing to S3.
	leaseChecker func(topic string, partitionID int) bool

	// conditionalPutIndex allows tests to force index CAS behavior without
	// replacing the whole storage client.
	conditionalPutIndex func(ctx context.Context, key string, data []byte, etag string) (string, error)

	// globalID maps a unique int to (topic, partitionID) for the batcher callback.
	globalIDMu   sync.Mutex
	nextGlobalID int
	globalIDMap  map[int]topicPartition
	reverseMap   map[topicPartition]int
}

type topicPartition struct {
	topic       string
	partitionID int
}

// NewPartitionManager creates a new PartitionManager from config.
func NewPartitionManager(cfg *config.Config, s3Client *storage.S3Client) (*PartitionManager, error) {
	cacheDir := cfg.Cache.Directory
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "camu-cache")
	}
	maxSize := cfg.Cache.MaxSize
	if maxSize == 0 {
		maxSize = 10 * 1024 * 1024 * 1024 // 10 GB default
	}
	diskCache, err := log.NewDiskCache(cacheDir, maxSize)
	if err != nil {
		return nil, fmt.Errorf("partition manager: create disk cache: %w", err)
	}

	walDir := cfg.WAL.Directory
	if walDir == "" {
		walDir = filepath.Join(os.TempDir(), "camu-wal")
	}

	pm := &PartitionManager{
		s3Client:    s3Client,
		diskCache:   diskCache,
		partitions:  make(map[string]map[int]*partitionState),
		routers:     make(map[string]*producer.Router),
		walDir:      walDir,
		walFsync:    cfg.WAL.Fsync,
		segmentsCfg: cfg.Segments,
		globalIDMap: make(map[int]topicPartition),
		reverseMap:  make(map[topicPartition]int),
	}
	pm.conditionalPutIndex = pm.s3Client.ConditionalPut

	maxAge, err := cfg.Segments.MaxAgeDuration()
	if err != nil {
		return nil, fmt.Errorf("partition manager: parse max age: %w", err)
	}
	maxSize64 := cfg.Segments.MaxSize
	if maxSize64 == 0 {
		maxSize64 = 8 * 1024 * 1024 // 8 MB default
	}

	pm.batcher = producer.NewBatcher(producer.BatcherConfig{
		MaxSize: maxSize64,
		MaxAge:  maxAge,
		OnFlush: pm.onFlush,
	})

	return pm, nil
}

// getGlobalID returns a stable unique int for the given topic+partition.
func (pm *PartitionManager) getGlobalID(topic string, partitionID int) int {
	tp := topicPartition{topic: topic, partitionID: partitionID}
	pm.globalIDMu.Lock()
	defer pm.globalIDMu.Unlock()
	if id, ok := pm.reverseMap[tp]; ok {
		return id
	}
	id := pm.nextGlobalID
	pm.nextGlobalID++
	pm.globalIDMap[id] = tp
	pm.reverseMap[tp] = id
	return id
}

// resolveGlobalID converts a global batcher partition ID back to topic+partition.
func (pm *PartitionManager) resolveGlobalID(globalID int) (string, int, bool) {
	pm.globalIDMu.Lock()
	defer pm.globalIDMu.Unlock()
	tp, ok := pm.globalIDMap[globalID]
	if !ok {
		return "", 0, false
	}
	return tp.topic, tp.partitionID, true
}

// InitTopic initializes all partitions for the given topic.
// The epochs map provides the lease epoch for each partition (from acquired leases).
// Partitions not in the map use epoch 0 (single-instance / no coordination).
func (pm *PartitionManager) InitTopic(ctx context.Context, tc meta.TopicConfig, epochs map[int]uint64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[tc.Name]; exists {
		return nil // already initialized
	}

	topicPartitions := make(map[int]*partitionState)

	for pid := 0; pid < tc.Partitions; pid++ {
		epoch := epochs[pid] // 0 if not in map
		ps, err := pm.initPartition(ctx, tc.Name, pid, epoch)
		if err != nil {
			// Clean up already-initialized partitions on failure.
			for _, p := range topicPartitions {
				p.wal.Close()
			}
			return fmt.Errorf("init topic %q partition %d: %w", tc.Name, pid, err)
		}
		topicPartitions[pid] = ps
		// Ensure global ID mapping exists.
		pm.getGlobalID(tc.Name, pid)
	}

	pm.partitions[tc.Name] = topicPartitions
	pm.routers[tc.Name] = producer.NewRouter(tc.Partitions)
	slog.Info("topic_initialized", "topic", tc.Name, "partitions", tc.Partitions)
	return nil
}

// initPartition initializes a single partition: loads index, opens WAL, replays.
// The epoch comes from the acquired lease — if the WAL contains data from a
// previous epoch, it is discarded (another instance already took over those offsets).
func (pm *PartitionManager) initPartition(ctx context.Context, topic string, partitionID int, epoch uint64) (*partitionState, error) {
	// 1. Load or create index.
	indexKey := fmt.Sprintf("%s/%d/index.json", topic, partitionID)
	idx := log.NewIndex()
	var indexETag string

	data, etag, err := pm.s3Client.GetWithETag(ctx, indexKey)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("load index: %w", err)
		}
		// Index doesn't exist yet — use empty index.
	} else {
		if err := idx.UnmarshalJSON(data); err != nil {
			return nil, fmt.Errorf("unmarshal index: %w", err)
		}
		indexETag = etag
	}

	// 2. Open WAL.
	walPath := filepath.Join(pm.walDir, topic, fmt.Sprintf("%d.wal", partitionID))
	if err := os.MkdirAll(filepath.Dir(walPath), 0o755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	wal, err := log.OpenWAL(walPath, pm.walFsync)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	// 3. Check epoch fencing — if a previous epoch's WAL exists, discard it.
	epochFile := walPath + ".epoch"
	var prevEpoch uint64
	if epochData, err := os.ReadFile(epochFile); err == nil {
		fmt.Sscanf(string(epochData), "%d", &prevEpoch)
	}

	if epoch > prevEpoch && prevEpoch > 0 {
		// Epoch has advanced — another instance took over this partition.
		// Discard the stale WAL (those offsets were reassigned).
		slog.Warn("epoch fencing: discarding stale WAL",
			"topic", topic, "partition", partitionID,
			"wal_epoch", prevEpoch, "lease_epoch", epoch)
		wal.Close()
		os.Remove(walPath)
		wal, err = log.OpenWAL(walPath, pm.walFsync)
		if err != nil {
			return nil, fmt.Errorf("reopen WAL after epoch discard: %w", err)
		}
	}

	// Write current epoch to sidecar file.
	if err := fsutil.AtomicWriteFile(epochFile, []byte(fmt.Sprintf("%d", epoch)), 0o644); err != nil {
		wal.Close()
		return nil, fmt.Errorf("write epoch sidecar: %w", err)
	}

	// 4. Replay WAL to recover unflushed messages.
	msgs, err := wal.Replay()
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("replay WAL: %w", err)
	}

	// 5. Set nextOffset from max(index.NextOffset(), last WAL message offset + 1).
	nextOffset := idx.NextOffset()
	if len(msgs) > 0 {
		lastWALOffset := msgs[len(msgs)-1].Offset + 1
		if lastWALOffset > nextOffset {
			nextOffset = lastWALOffset
		}
	}

	slog.Info("partition_state_loaded",
		"topic", topic,
		"partition", partitionID,
		"epoch", epoch,
		"index_next_offset", idx.NextOffset(),
		"index_hw", idx.HighWatermark(),
		"wal_messages", len(msgs),
		"wal_first_offset", func() uint64 {
			if len(msgs) == 0 {
				return 0
			}
			return msgs[0].Offset
		}(),
		"wal_last_offset", func() uint64 {
			if len(msgs) == 0 {
				return 0
			}
			return msgs[len(msgs)-1].Offset
		}(),
		"next_offset", nextOffset,
	)

	return &partitionState{
		wal:        wal,
		index:      idx,
		indexETag:  indexETag,
		nextOffset: nextOffset,
		epoch:      epoch,
	}, nil
}

// Append adds a message to the given topic/partition, assigns an offset, writes to WAL,
// and enqueues for batching. Returns the assigned offset.
func (pm *PartitionManager) Append(ctx context.Context, topic string, partitionID int, msg log.Message) (uint64, error) {
	pm.mu.RLock()
	topicPartitions, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return 0, fmt.Errorf("topic %q not initialized", topic)
	}
	ps, ok := topicPartitions[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return 0, fmt.Errorf("partition %d not found for topic %q", partitionID, topic)
	}
	pm.mu.RUnlock()

	// Serialize offset assignment and WAL append per partition so messages
	// are durably written in offset order.
	ps.appendMu.Lock()
	defer ps.appendMu.Unlock()

	pm.mu.Lock()
	offset := ps.nextOffset
	ps.nextOffset++
	pm.mu.Unlock()

	msg.Offset = offset
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixNano()
	}

	slog.Debug("partition_append_assigned",
		"topic", topic,
		"partition", partitionID,
		"offset", offset,
		"next_offset", ps.nextOffset,
		"epoch", ps.epoch,
		"is_leader", ps.isLeader,
		"flushed_offset", ps.flushedOffset,
		"hw", func() uint64 {
			if ps.replicaState != nil {
				return ps.replicaState.HighWatermark()
			}
			return ps.index.HighWatermark()
		}(),
	)

	// Write to WAL.
	if err := ps.wal.Append(msg); err != nil {
		return 0, fmt.Errorf("WAL append: %w", err)
	}
	if ps.replicaState != nil {
		ps.replicaState.SetLeaderOffset(offset + 1)
		ps.replicaState.NotifyNewData()
	}

	// Notify batcher of size increase (no message data).
	globalID := pm.getGlobalID(topic, partitionID)
	msgSize := int64(len(msg.Key) + len(msg.Value) + 40)
	if err := pm.batcher.Append(globalID, msgSize); err != nil {
		return 0, fmt.Errorf("batcher append: %w", err)
	}

	return offset, nil
}

// AppendBatch writes multiple messages to the same partition with a single WAL fsync.
// Returns the assigned offsets for each message.
func (pm *PartitionManager) AppendBatch(ctx context.Context, topic string, partitionID int, msgs []log.Message) ([]uint64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	pm.mu.RLock()
	topicPartitions, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return nil, fmt.Errorf("topic %q not initialized", topic)
	}
	ps, ok := topicPartitions[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return nil, fmt.Errorf("partition %d not found for topic %q", partitionID, topic)
	}
	pm.mu.RUnlock()

	// Assign offsets for the entire batch.
	ps.appendMu.Lock()
	defer ps.appendMu.Unlock()

	pm.mu.Lock()
	offsets := make([]uint64, len(msgs))
	now := time.Now().UnixNano()
	for i := range msgs {
		offsets[i] = ps.nextOffset
		msgs[i].Offset = ps.nextOffset
		ps.nextOffset++
		if msgs[i].Timestamp == 0 {
			msgs[i].Timestamp = now
		}
	}
	pm.mu.Unlock()

	// Write entire batch to WAL with a single fsync.
	if err := ps.wal.AppendBatch(msgs); err != nil {
		return nil, fmt.Errorf("WAL append batch: %w", err)
	}
	if ps.replicaState != nil {
		ps.replicaState.SetLeaderOffset(offsets[len(offsets)-1] + 1)
		ps.replicaState.NotifyNewData()
	}

	// Notify batcher of total size increase (no message data).
	globalID := pm.getGlobalID(topic, partitionID)
	totalBatchSize := int64(0)
	for _, msg := range msgs {
		totalBatchSize += int64(len(msg.Key) + len(msg.Value) + 40)
	}
	if err := pm.batcher.Append(globalID, totalBatchSize); err != nil {
		return nil, fmt.Errorf("batcher append: %w", err)
	}

	return offsets, nil
}

// AppendReplicatedBatch writes messages to the given partition preserving their
// existing offsets (set by the leader). It does NOT reassign offsets.
func (pm *PartitionManager) AppendReplicatedBatch(ctx context.Context, topic string, partitionID int, msgs []log.Message) error {
	pm.mu.RLock()
	ps, ok := pm.partitions[topic][partitionID]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("partition %s/%d not found", topic, partitionID)
	}

	ps.appendMu.Lock()
	defer ps.appendMu.Unlock()

	// Write with existing offsets — do NOT reassign
	if err := ps.wal.AppendBatch(msgs); err != nil {
		return err
	}
	// Advance nextOffset to max(incoming) + 1
	if len(msgs) > 0 {
		maxOffset := msgs[len(msgs)-1].Offset
		if maxOffset+1 > ps.nextOffset {
			ps.nextOffset = maxOffset + 1
		}
	}
	return nil
}

// IsOwned returns true — in single-instance mode all partitions are owned.
func (pm *PartitionManager) IsOwned(topic string, partitionID int) bool {
	return true
}

// GetRouter returns the router for the given topic.
func (pm *PartitionManager) GetRouter(topic string) *producer.Router {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.routers[topic]
}

// RefreshIndex reloads a partition's index from S3 so reads on non-owner nodes
// see segments flushed by the current owner. This is a no-op if the partition
// is not initialized.
func (pm *PartitionManager) RefreshIndex(ctx context.Context, topic string, partitionID int) {
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return
	}
	ps, ok := tp[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return
	}
	pm.mu.RUnlock()

	indexKey := fmt.Sprintf("%s/%d/index.json", topic, partitionID)
	data, etag, err := pm.s3Client.GetWithETag(ctx, indexKey)
	if err != nil {
		return // stale index is better than no index
	}
	idx := log.NewIndex()
	if err := idx.UnmarshalJSON(data); err != nil {
		return
	}

	pm.mu.Lock()
	ps.index = idx
	ps.indexETag = etag
	pm.mu.Unlock()
}

// GetDiskCache returns the disk cache used by the partition manager.
func (pm *PartitionManager) GetDiskCache() *log.DiskCache {
	return pm.diskCache
}

// GetIndex returns the partition index.
func (pm *PartitionManager) GetIndex(topic string, partitionID int) *log.Index {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	tp, ok := pm.partitions[topic]
	if !ok {
		return nil
	}
	ps, ok := tp[partitionID]
	if !ok {
		return nil
	}
	return ps.index
}

// GetPartitionState returns the partitionState for the given topic/partition, or nil if not found.
func (pm *PartitionManager) GetPartitionState(topic string, partitionID int) *partitionState {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if parts, ok := pm.partitions[topic]; ok {
		return parts[partitionID]
	}
	return nil
}

// TruncateWAL removes WAL entries before the given offset for a partition.
func (pm *PartitionManager) TruncateWAL(topic string, pid int, beforeOffset uint64) error {
	ps := pm.GetPartitionState(topic, pid)
	if ps == nil {
		return fmt.Errorf("partition %s/%d not found", topic, pid)
	}
	return ps.wal.TruncateBefore(beforeOffset)
}

// CancelAllFetchLoops cancels all active follower fetch goroutines and waits
// for them to finish.
func (pm *PartitionManager) CancelAllFetchLoops() {
	pm.mu.RLock()
	var doneChans []chan struct{}
	for _, parts := range pm.partitions {
		for _, ps := range parts {
			if ps.fetchCancel != nil {
				ps.fetchCancel()
				if ps.fetchDone != nil {
					doneChans = append(doneChans, ps.fetchDone)
				}
			}
		}
	}
	pm.mu.RUnlock()
	for _, ch := range doneChans {
		<-ch
	}
}

// Shutdown stops the batcher (flushing remaining messages) and closes all WALs.
func (pm *PartitionManager) Shutdown(ctx context.Context) error {
	pm.batcher.Stop()

	pm.mu.Lock()
	defer pm.mu.Unlock()

	var firstErr error
	for _, topicPartitions := range pm.partitions {
		for _, ps := range topicPartitions {
			if err := ps.wal.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// SetLeaseChecker sets a callback to verify partition ownership before flushing.
// The server sets this after creating the partition manager.
func (pm *PartitionManager) SetLeaseChecker(fn func(topic string, partitionID int) bool) {
	pm.leaseChecker = fn
}

// onFlush is the batcher's flush callback. It reads unflushed messages from the
// WAL, serializes them into a segment, uploads to S3, writes to disk cache,
// updates the index, and truncates the WAL.
func (pm *PartitionManager) onFlush(globalPartitionID int) error {
	topic, partitionID, ok := pm.resolveGlobalID(globalPartitionID)
	if !ok {
		return fmt.Errorf("unknown global partition ID %d", globalPartitionID)
	}

	// Check lease validity before flushing — prevents writing to S3
	// after another instance has taken over this partition.
	if pm.leaseChecker != nil && !pm.leaseChecker(topic, partitionID) {
		slog.Warn("onFlush: lease expired, skipping flush",
			"topic", topic, "partition", partitionID)
		return nil
	}

	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return fmt.Errorf("topic %q not found during flush", topic)
	}
	ps, ok := tp[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return fmt.Errorf("partition %d not found for topic %q during flush", partitionID, topic)
	}
	pm.mu.RUnlock()

	// Read all messages from WAL — everything in it is unflushed by definition
	// (TruncateBefore removes flushed entries after each flush).
	msgs, err := ps.wal.Replay()
	if err != nil {
		return fmt.Errorf("WAL replay: %w", err)
	}
	if len(msgs) == 0 {
		slog.Debug("flush_skipped_empty_wal",
			"topic", topic,
			"partition", partitionID,
			"epoch", ps.epoch,
			"next_offset", ps.nextOffset,
			"flushed_offset", ps.flushedOffset,
			"index_hw", ps.index.HighWatermark(),
		)
		return nil
	}

	baseOffset := msgs[0].Offset
	endOffset := msgs[len(msgs)-1].Offset
	epoch := ps.epoch

	slog.Info("flush_begin",
		"topic", topic,
		"partition", partitionID,
		"epoch", epoch,
		"message_count", len(msgs),
		"base_offset", baseOffset,
		"end_offset", endOffset,
		"next_offset", ps.nextOffset,
		"flushed_offset", ps.flushedOffset,
		"hw", func() uint64 {
			if ps.replicaState != nil {
				return ps.replicaState.HighWatermark()
			}
			return ps.index.HighWatermark()
		}(),
		"is_leader", ps.isLeader,
	)

	// 1. Serialize messages into segment binary format.
	var segBuf bytes.Buffer
	compression := pm.segmentsCfg.Compression
	if compression == "" {
		compression = log.CompressionNone
	}
	if err := log.WriteSegment(&segBuf, msgs, compression); err != nil {
		return fmt.Errorf("write segment: %w", err)
	}
	segData := segBuf.Bytes()

	// 2. Upload segment to S3.
	segKey := fmt.Sprintf("%s/%d/%d-%d.segment", topic, partitionID, baseOffset, epoch)
	ctx := context.Background()
	if err := pm.s3Client.Put(ctx, segKey, segData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload segment: %w", err)
	}

	// 3. Write segment to disk cache.
	if err := pm.diskCache.Put(segKey, segData); err != nil {
		// Non-fatal: log but don't fail the flush.
		_ = err
	}

	// 4. Update index with retry on conflict.
	segRef := log.SegmentRef{
		BaseOffset: baseOffset,
		EndOffset:  endOffset,
		Epoch:      epoch,
		Key:        segKey,
		CreatedAt:  time.Now(),
	}

	indexKey := fmt.Sprintf("%s/%d/index.json", topic, partitionID)
	indexUpdated := false
	for retries := 0; retries < 5; retries++ {
		// Load current index from S3 (may have changed since init).
		currentIdx := log.NewIndex()
		var currentETag string

		data, etag, err := pm.s3Client.GetWithETag(ctx, indexKey)
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("load index for update: %w", err)
			}
			// Index doesn't exist yet, will create.
		} else {
			if err := currentIdx.UnmarshalJSON(data); err != nil {
				return fmt.Errorf("unmarshal index for update: %w", err)
			}
			currentETag = etag
		}

		// Add new segment ref.
		currentIdx.Add(segRef)
		hw := endOffset + 1
		if ps.replicaState != nil {
			hw = ps.replicaState.HighWatermark()
		}
		currentIdx.SetHighWatermark(hw)

		idxData, err := currentIdx.MarshalJSON()
		if err != nil {
			return fmt.Errorf("marshal updated index: %w", err)
		}

		newETag, err := pm.conditionalPutIndex(ctx, indexKey, idxData, currentETag)
		if err != nil {
			if errors.Is(err, storage.ErrConflict) {
				slog.Warn("flush_index_conflict",
					"topic", topic,
					"partition", partitionID,
					"epoch", epoch,
					"base_offset", baseOffset,
					"end_offset", endOffset,
					"retry", retries+1,
				)
				continue // retry
			}
			return fmt.Errorf("conditional put index: %w", err)
		}

		// Update in-memory index and etag.
		pm.mu.Lock()
		ps.index = currentIdx
		ps.indexETag = newETag
		pm.mu.Unlock()
		indexUpdated = true
		break
	}

	if !indexUpdated {
		slog.Error("flush_index_exhausted",
			"topic", topic,
			"partition", partitionID,
			"epoch", epoch,
			"base_offset", baseOffset,
			"end_offset", endOffset,
			"segment_key", segKey,
		)
		return fmt.Errorf("flush index exhausted after retries")
	}

	slog.Info("segment_flushed",
		"topic", topic,
		"partition", partitionID,
		"base_offset", baseOffset,
		"end_offset", endOffset,
		"size_bytes", len(segData),
	)

	// 5. Truncate WAL — but for replicated partitions, only truncate up to
	// the high watermark. Entries above HW haven't been acked by followers
	// yet, and followers need to read them from the WAL. Without this, the
	// batcher truncates WAL entries before followers can fetch them.
	truncateBefore := endOffset + 1
	if ps.replicaState != nil {
		hw := ps.replicaState.HighWatermark()
		if hw < truncateBefore {
			truncateBefore = hw
		}
	}
	if truncateBefore > 0 {
		if err := ps.wal.TruncateBefore(truncateBefore); err != nil {
			return fmt.Errorf("truncate WAL: %w", err)
		}
	}
	slog.Info("wal_truncated", "topic", topic, "partition", partitionID,
		"truncate_before", truncateBefore, "flushed_end", endOffset,
		"hw", func() uint64 {
			if ps.replicaState != nil {
				return ps.replicaState.HighWatermark()
			}
			return 0
		}())

	// 6. Track highest offset successfully flushed to S3.
	pm.mu.Lock()
	if endOffset > ps.flushedOffset {
		ps.flushedOffset = endOffset
	}
	pm.mu.Unlock()

	return nil
}
