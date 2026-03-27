package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/fsutil"
	"github.com/maksim/camu/internal/idempotency"
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
	nextOffset    uint64
	epoch         uint64                    // always 0 in single-instance mode
	replicaState  *replication.ReplicaState // nil for rf=1
	isLeader      bool
	flushedOffset uint64 // highest offset flushed to S3
	followerHW    uint64 // leader-advertised readable HW for follower reads
	epochHistory  *replication.EpochHistory
	fetchCancel   context.CancelFunc // cancel follower fetch goroutine
	fetchDone     chan struct{}      // closed when fetch goroutine exits
	globalID      int  // cached batcher partition ID, set on first append
	globalIDSet   bool // true once globalID has been resolved
}

// PartitionManager manages per-partition state including WAL, index, and batching.
type PartitionManager struct {
	mu           sync.RWMutex
	s3Client     *storage.S3Client
	diskCache    *log.DiskCache
	partitions   map[string]map[int]*partitionState // topic -> partitionID -> state
	routers      map[string]*producer.Router
	batcher      *producer.Batcher
	walDir       string
	walFsync     bool
	walChunkSize int64
	segmentsCfg  config.SegmentsConfig

	idempotencyMgr *idempotency.Manager

	// leaseChecker validates partition ownership before flushing to S3.
	leaseChecker func(topic string, partitionID int) bool


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
		s3Client:     s3Client,
		diskCache:    diskCache,
		partitions:   make(map[string]map[int]*partitionState),
		routers:      make(map[string]*producer.Router),
		walDir:       walDir,
		walFsync:     cfg.WAL.Fsync,
		walChunkSize: cfg.WAL.ChunkSize,
		segmentsCfg:  cfg.Segments,
		globalIDMap:  make(map[int]topicPartition),
		reverseMap:   make(map[topicPartition]int),
	}

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
	// 1. Load segment list from S3 by listing objects.
	prefix := log.ListSegmentPrefix(topic, partitionID)
	keys, err := pm.s3Client.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list segments for %s/%d: %w", topic, partitionID, err)
	}
	idx := log.NewIndex()
	for _, ref := range log.SegmentRefsFromKeys(keys) {
		idx.Add(ref)
	}

	// Load partition state (HW + epoch history) from state.json.
	stateKey := log.StateKey(topic, partitionID)
	if stateData, err := pm.s3Client.Get(ctx, stateKey); err == nil {
		var partState log.PartitionState
		if err := partState.Unmarshal(stateData); err != nil {
			slog.Warn("initPartition: bad state.json", "topic", topic, "partition", partitionID, "error", err)
		} else {
			idx.SetHighWatermark(partState.HighWatermark)
			if len(partState.EpochHistory) > 0 {
				idx.SetEpochHistory(partState.EpochHistory)
			}
		}
	}

	// 2. Open WAL.
	walPath := filepath.Join(pm.walDir, topic, fmt.Sprintf("%d.wal", partitionID))
	if err := os.MkdirAll(filepath.Dir(walPath), 0o755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	wal, err := log.OpenWAL(walPath, pm.walFsync, pm.walChunkSize)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	// 3. Check epoch fencing — if a previous epoch's WAL exists, discard it.
	epochFile := walPath + ".epoch"
	var prevEpoch uint64
	if epochData, err := os.ReadFile(epochFile); err == nil {
		_, _ = fmt.Sscanf(string(epochData), "%d", &prevEpoch)
	}

	if epoch > prevEpoch && prevEpoch > 0 {
		// Epoch has advanced — another instance took over this partition.
		// Discard the stale WAL (those offsets were reassigned).
		slog.Warn("epoch fencing: discarding stale WAL",
			"topic", topic, "partition", partitionID,
			"wal_epoch", prevEpoch, "lease_epoch", epoch)
		_ = wal.Close()
		_ = os.Remove(walPath)
		_ = os.RemoveAll(walPath + ".segments")
		wal, err = log.OpenWAL(walPath, pm.walFsync, pm.walChunkSize)
		if err != nil {
			return nil, fmt.Errorf("reopen WAL after epoch discard: %w", err)
		}
	}

	// Write current epoch to sidecar file.
	if err := fsutil.AtomicWriteFile(epochFile, []byte(fmt.Sprintf("%d", epoch)), 0o644); err != nil {
		_ = wal.Close()
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

	// Use cached globalID — set lazily on first append under appendMu.
	if !ps.globalIDSet {
		ps.globalID = pm.getGlobalID(topic, partitionID)
		ps.globalIDSet = true
	}
	globalID := ps.globalID
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

	return pm.appendBatchToPS(ps, topic, partitionID, msgs)
}

// appendBatchToPS is the inner implementation of AppendBatch that operates
// directly on a known partitionState, avoiding a redundant pm.mu.RLock lookup.
func (pm *PartitionManager) appendBatchToPS(ps *partitionState, topic string, partitionID int, msgs []log.Message) ([]uint64, error) {
	// Assign offsets for the entire batch.
	// appendMu serialises all access to ps.nextOffset — no additional lock needed.
	ps.appendMu.Lock()
	defer ps.appendMu.Unlock()

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

	// Write entire batch to WAL with a single fsync.
	if err := ps.wal.AppendBatch(msgs); err != nil {
		return nil, fmt.Errorf("WAL append batch: %w", err)
	}
	return offsets, pm.postAppend(ps, topic, partitionID, msgs, offsets)
}

// IdempotencyOpts carries idempotency parameters for AppendBatchWithMeta.
// When non-nil, the idempotency check, WAL write, and offset recording all
// happen atomically under appendMu — preventing sequence advance without
// data write and ensuring LastOffset is set before the lock releases.
type IdempotencyOpts struct {
	Manager  *idempotency.Manager
	Key      idempotency.PartitionKey
	Sequence uint64
}

// AppendBatchWithMeta writes messages with producer metadata to the WAL.
// If idem is non-nil, the idempotency check is performed atomically with
// the WAL write under appendMu. Returns ErrDuplicateSequence if duplicate.
func (pm *PartitionManager) AppendBatchWithMeta(ctx context.Context, topic string, partitionID int, batch log.Batch, idem *IdempotencyOpts) ([]uint64, error) {
	if len(batch.Messages) == 0 {
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

	return pm.appendBatchWithMetaToPS(ps, topic, partitionID, batch, idem)
}

// appendBatchWithMetaToPS is the inner implementation of AppendBatchWithMeta
// that operates directly on a known partitionState, avoiding a redundant
// pm.mu.RLock lookup when the caller already has ps.
func (pm *PartitionManager) appendBatchWithMetaToPS(ps *partitionState, topic string, partitionID int, batch log.Batch, idem *IdempotencyOpts) ([]uint64, error) {
	ps.appendMu.Lock()
	defer ps.appendMu.Unlock()

	// Idempotency gate under appendMu — atomic with WAL write.
	if idem != nil {
		if err := idem.Manager.CheckAndAdvance(batch.ProducerID, idem.Key, idem.Sequence, len(batch.Messages)); err != nil {
			return nil, err
		}
	}

	// Assign offsets — appendMu serialises all access to nextOffset,
	// so no additional pm.mu lock is needed here.
	offsets := make([]uint64, len(batch.Messages))
	now := time.Now().UnixNano()
	for i := range batch.Messages {
		offsets[i] = ps.nextOffset
		batch.Messages[i].Offset = ps.nextOffset
		ps.nextOffset++
		if batch.Messages[i].Timestamp == 0 {
			batch.Messages[i].Timestamp = now
		}
	}

	if err := ps.wal.AppendBatchWithMeta(batch); err != nil {
		// Rollback sequence on WAL failure so the producer can retry.
		if idem != nil {
			idem.Manager.RollbackSequence(batch.ProducerID, idem.Key, idem.Sequence)
		}
		return nil, fmt.Errorf("WAL append batch: %w", err)
	}

	// Record offset for purgatory join on duplicate — under appendMu so
	// it's always set before a duplicate retry can read it.
	if idem != nil {
		idem.Manager.RecordLastOffset(batch.ProducerID, idem.Key, offsets[len(offsets)-1])
	}

	return offsets, pm.postAppend(ps, topic, partitionID, batch.Messages, offsets)
}

func (pm *PartitionManager) postAppend(ps *partitionState, topic string, partitionID int, msgs []log.Message, offsets []uint64) error {
	if ps.replicaState != nil {
		ps.replicaState.SetLeaderOffset(offsets[len(offsets)-1] + 1)
		ps.replicaState.NotifyNewData()
	}

	// Use cached globalID — set lazily on first append under appendMu.
	if !ps.globalIDSet {
		ps.globalID = pm.getGlobalID(topic, partitionID)
		ps.globalIDSet = true
	}
	globalID := ps.globalID
	totalBatchSize := int64(0)
	for _, msg := range msgs {
		totalBatchSize += int64(len(msg.Key) + len(msg.Value) + 40)
	}
	if err := pm.batcher.Append(globalID, totalBatchSize); err != nil {
		return fmt.Errorf("batcher append: %w", err)
	}

	return nil
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

	prefix := log.ListSegmentPrefix(topic, partitionID)
	keys, err := pm.s3Client.List(ctx, prefix)
	if err != nil {
		slog.Warn("RefreshIndex: list failed", "topic", topic, "partition", partitionID, "error", err)
		return
	}
	idx := log.NewIndex()
	for _, ref := range log.SegmentRefsFromKeys(keys) {
		idx.Add(ref)
	}

	stateKey := log.StateKey(topic, partitionID)
	if stateData, err := pm.s3Client.Get(ctx, stateKey); err == nil {
		var state log.PartitionState
		if err := state.Unmarshal(stateData); err == nil {
			idx.SetHighWatermark(state.HighWatermark)
			idx.SetEpochHistory(state.EpochHistory)
		}
	}

	pm.mu.Lock()
	ps.index = idx
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

// UpdateFollowerProgress records the latest leader-advertised readable
// high-watermark and flushed offset for a follower partition.
func (pm *PartitionManager) UpdateFollowerProgress(topic string, partitionID int, highWatermark, flushedOffset uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	parts, ok := pm.partitions[topic]
	if !ok {
		return
	}
	ps, ok := parts[partitionID]
	if !ok {
		return
	}

	if highWatermark > ps.followerHW {
		ps.followerHW = highWatermark
	}
	if flushedOffset > ps.flushedOffset {
		ps.flushedOffset = flushedOffset
	}
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

// ScanWALForProducerState reads the unflushed WAL tail for a partition and
// extracts batch-level producer metadata for idempotency state recovery.
// Only committed batches (offset ≤ high watermark) are included — uncommitted
// batches may be truncated on failover and must not advance sequence state.
func (pm *PartitionManager) ScanWALForProducerState(topic string, partitionID int) []idempotency.BatchInfo {
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return nil
	}
	ps, ok := tp[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return nil
	}
	flushedOffset := ps.flushedOffset
	// High watermark: only batches below HW are committed.
	// For rf=1 (no replicaState), all WAL data is committed — use max uint64.
	hw := uint64(math.MaxUint64)
	if ps.replicaState != nil {
		hw = ps.replicaState.HighWatermark()
	}
	pm.mu.RUnlock()

	batches, err := ps.wal.ReadBatchMetasFrom(flushedOffset)
	if err != nil {
		slog.Warn("idempotency_wal_scan_failed",
			"topic", topic, "partition", partitionID, "error", err)
		return nil
	}

	var infos []idempotency.BatchInfo
	for _, b := range batches {
		if b.ProducerID == 0 {
			continue
		}
		// Skip uncommitted batches — their last offset is above HW.
		if b.LastOffset >= hw {
			continue
		}
		infos = append(infos, idempotency.BatchInfo{
			ProducerID: b.ProducerID,
			Sequence:   b.Sequence,
			BatchSize:  b.MessageCount,
			Key:        idempotency.PartitionKey{Topic: topic, Partition: partitionID},
		})
	}
	return infos
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

	// Seal the active chunk first so new appends land in a fresh active chunk
	// while the sealed unflushed chunks are uploaded.
	if err := ps.wal.Seal(); err != nil {
		return fmt.Errorf("seal WAL: %w", err)
	}

	// Read all sealed, not-yet-flushed chunks. Flushed-retained chunks stay in
	// WAL for follower catch-up but are excluded from the flush input.
	msgs, err := ps.wal.ReplaySealed()
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
	if err := log.WriteSegment(&segBuf, msgs, compression, pm.segmentsCfg.RecordBatchTargetSize); err != nil {
		return fmt.Errorf("write segment: %w", err)
	}
	segData := segBuf.Bytes()
	segIndexData, err := log.BuildSegmentOffsetIndex(segData, baseOffset, pm.segmentsCfg.IndexIntervalBytes)
	if err != nil {
		return fmt.Errorf("build segment offset index: %w", err)
	}

	// 2. Upload segment assets to S3.
	// This sequence is intentionally non-atomic: the immutable segment object
	// lands first, followed by its sidecars, and only then does index.json gain
	// a reference to the new SegmentRef. Crashes in between can leave orphaned
	// assets behind, which is acceptable because GC cleans up unreferenced
	// segment, offset-index, and metadata objects.
	segKey := log.FormatSegmentKey(topic, partitionID, baseOffset, endOffset, epoch)
	segRef := log.SegmentRef{
		BaseOffset:     baseOffset,
		EndOffset:      endOffset,
		Epoch:          epoch,
		Key:            segKey,
		OffsetIndexKey: log.SegmentOffsetIndexKey(segKey),
		MetaKey:        log.SegmentMetadataKey(segKey),
		CreatedAt:      time.Now(),
	}
	segIndexKey := segRef.OffsetIndexObjectKey()
	segMetaData, err := log.BuildSegmentMetadata(segRef, len(msgs), int64(len(segData)), compression)
	if err != nil {
		return fmt.Errorf("build segment metadata: %w", err)
	}
	ctx := context.Background()
	if err := pm.s3Client.Put(ctx, segKey, segData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload segment: %w", err)
	}
	if err := pm.s3Client.Put(ctx, segIndexKey, segIndexData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload segment offset index: %w", err)
	}
	if err := pm.s3Client.Put(ctx, segRef.MetaObjectKey(), segMetaData, storage.PutOpts{ContentType: "application/json"}); err != nil {
		return fmt.Errorf("upload segment metadata: %w", err)
	}

	// 3. Write segment to disk cache.
	if err := pm.diskCache.Put(segKey, segData); err != nil {
		// Non-fatal: log but don't fail the flush.
		_ = err
	}
	if err := pm.diskCache.Put(segIndexKey, segIndexData); err != nil {
		_ = err
	}
	if err := pm.diskCache.Put(segRef.MetaObjectKey(), segMetaData); err != nil {
		_ = err
	}

	// 4. Update partition state (HW + epoch history) — simple PUT, no CAS needed.
	hw := endOffset + 1
	if ps.replicaState != nil {
		hw = ps.replicaState.HighWatermark()
	}
	partState := log.PartitionState{
		HighWatermark: hw,
	}
	if ps.epochHistory != nil {
		for _, e := range ps.epochHistory.Entries {
			partState.EpochHistory = append(partState.EpochHistory, log.EpochEntry{
				Epoch:       e.Epoch,
				StartOffset: e.StartOffset,
			})
		}
	}
	stateData, err := partState.Marshal()
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	stateKey := log.StateKey(topic, partitionID)
	if err := pm.s3Client.Put(ctx, stateKey, stateData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload state.json: %w", err)
	}

	// Update in-memory segment index.
	pm.mu.Lock()
	ps.index.Add(segRef)
	ps.index.SetHighWatermark(hw)
	pm.mu.Unlock()

	slog.Info("segment_flushed",
		"topic", topic,
		"partition", partitionID,
		"base_offset", baseOffset,
		"end_offset", endOffset,
		"size_bytes", len(segData),
	)

	// Upload per-partition producer idempotency checkpoint.
	if pm.idempotencyMgr != nil {
		pkey := idempotency.PartitionKey{Topic: topic, Partition: partitionID}
		checkpointData, err := pm.idempotencyMgr.CheckpointPartition(pkey)
		if err != nil {
			slog.Warn("flush_idempotency_checkpoint_failed", "topic", topic, "partition", partitionID, "error", err)
		} else if len(checkpointData) > 0 {
			checkpointKey := fmt.Sprintf("%s/%d/producers.checkpoint", topic, partitionID)
			if err := pm.s3Client.Put(ctx, checkpointKey, checkpointData, storage.PutOpts{}); err != nil {
				slog.Warn("flush_idempotency_checkpoint_upload_failed", "topic", topic, "partition", partitionID, "error", err)
			}
		}
	}

	// 5. Mark flushed chunks as retained for replica catch-up. They remain
	// readable via WAL until all followers move past them.
	if err := ps.wal.MarkFlushed(endOffset + 1); err != nil {
		return fmt.Errorf("mark WAL flushed: %w", err)
	}

	retainBefore := endOffset + 1
	if ps.replicaState != nil {
		if minFollowerOffset, ok := ps.replicaState.MinFollowerOffset(); ok && minFollowerOffset < retainBefore {
			retainBefore = minFollowerOffset
		}
	}
	if retainBefore > 0 {
		if err := ps.wal.TruncateBefore(retainBefore); err != nil {
			return fmt.Errorf("truncate WAL retained chunks: %w", err)
		}
	}
	slog.Info("wal_retention_updated", "topic", topic, "partition", partitionID,
		"retain_before", retainBefore, "flushed_end", endOffset,
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
