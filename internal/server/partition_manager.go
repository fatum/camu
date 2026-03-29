package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

// producerPartitionState tracks idempotency sequence for one producer on this partition.
type producerPartitionState struct {
	NextSeq      uint64    `json:"next_seq"`
	LastOffset   uint64    `json:"last_offset"`
	LastActiveAt time.Time `json:"-"` // not persisted; set on each produce
}

// producerCheckpointEntry is a single line in the NDJSON checkpoint file.
type producerCheckpointEntry struct {
	ProducerID uint64 `json:"producer_id"`
	NextSeq    uint64 `json:"next_seq"`
	LastOffset uint64 `json:"last_offset"`
}

// partitionState holds per-partition runtime state.
type partitionState struct {
	mu            sync.RWMutex // unified per-partition lock for all read/write access
	wal           *log.WAL
	index         *log.Index
	nextOffset    uint64
	epoch         uint64                    // always 0 in single-instance mode
	replicaState  *replication.ReplicaState // nil for rf=1
	isLeader      bool
	leaderID      string // current leader for follower fetch state; empty when local leader
	flushedOffset uint64 // highest offset flushed to S3
	followerHW    uint64 // leader-advertised readable HW for follower reads
	epochHistory  *replication.EpochHistory
	fetchCancel   context.CancelFunc                 // cancel follower fetch goroutine
	fetchDone     chan struct{}                      // closed when fetch goroutine exits
	globalID      int                                // cached batcher partition ID, set on first append
	globalIDSet   bool                               // true once globalID has been resolved
	producerSeqs  map[uint64]*producerPartitionState // producerID -> sequence state
}

func (ps *partitionState) checkAndAdvanceSeq(producerID, sequence uint64, batchSize int) error {
	now := time.Now()
	state, ok := ps.producerSeqs[producerID]
	if !ok {
		if sequence != 0 {
			return idempotency.ErrUnknownProducer
		}
		ps.producerSeqs[producerID] = &producerPartitionState{
			NextSeq:      uint64(batchSize),
			LastActiveAt: now,
		}
		return nil
	}
	if sequence < state.NextSeq {
		return idempotency.ErrDuplicateSequence
	}
	if sequence > state.NextSeq {
		return idempotency.ErrSequenceGap
	}
	state.NextSeq = sequence + uint64(batchSize)
	state.LastActiveAt = now
	return nil
}

func (ps *partitionState) rollbackSeq(producerID, sequence uint64) {
	if state, ok := ps.producerSeqs[producerID]; ok {
		state.NextSeq = sequence
	}
}

func (ps *partitionState) recordLastOffset(producerID, offset uint64) {
	if state, ok := ps.producerSeqs[producerID]; ok {
		state.LastOffset = offset
	}
}

func (ps *partitionState) getLastOffset(producerID uint64) (uint64, bool) {
	state, ok := ps.producerSeqs[producerID]
	if !ok {
		return 0, false
	}
	return state.LastOffset, true
}

// snapshotProducerSeqs returns a shallow copy of producerSeqs for checkpoint serialization.
func (ps *partitionState) snapshotProducerSeqs() map[uint64]*producerPartitionState {
	cp := make(map[uint64]*producerPartitionState, len(ps.producerSeqs))
	for k, v := range ps.producerSeqs {
		dup := *v
		cp[k] = &dup
	}
	return cp
}

// rebuildProducerSeqsFromBatches replays WAL batch metadata to advance sequence
// counters past whatever the checkpoint contained.
func (ps *partitionState) rebuildProducerSeqsFromBatches(batches []log.BatchMeta) {
	now := time.Now()
	for _, b := range batches {
		if b.ProducerID == 0 {
			continue
		}
		state, ok := ps.producerSeqs[b.ProducerID]
		if !ok {
			state = &producerPartitionState{}
			ps.producerSeqs[b.ProducerID] = state
		}
		end := b.Sequence + uint64(b.MessageCount)
		if end > state.NextSeq {
			state.NextSeq = end
		}
		state.LastActiveAt = now
	}
}

// loadProducerCheckpoint restores producer sequence state from NDJSON bytes
// produced during flush. It merges into existing state.
func (ps *partitionState) loadProducerCheckpoint(data []byte) {
	now := time.Now()
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var ce producerCheckpointEntry
		if err := json.Unmarshal(scanner.Bytes(), &ce); err != nil {
			continue
		}
		ps.producerSeqs[ce.ProducerID] = &producerPartitionState{
			NextSeq:      ce.NextSeq,
			LastOffset:   ce.LastOffset,
			LastActiveAt: now,
		}
	}
}

// evictStaleProducers removes producers that have been idle for longer than ttl.
func (ps *partitionState) evictStaleProducers(ttl time.Duration) int {
	cutoff := time.Now().Add(-ttl)
	var n int
	for id, state := range ps.producerSeqs {
		if state.LastActiveAt.IsZero() {
			continue
		}
		if state.LastActiveAt.Before(cutoff) {
			delete(ps.producerSeqs, id)
			n++
		}
	}
	return n
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
		wal:          wal,
		index:        idx,
		nextOffset:   nextOffset,
		epoch:        epoch,
		producerSeqs: make(map[uint64]*producerPartitionState),
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
	ps.mu.Lock()

	offset := ps.nextOffset
	ps.nextOffset++

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

	if err := ps.wal.AppendBatchLocked([]log.Message{msg}); err != nil {
		ps.mu.Unlock()
		return 0, fmt.Errorf("WAL append: %w", err)
	}
	pm.postAppendLocked(ps, topic, partitionID, []uint64{offset})
	ps.mu.Unlock()

	// Batcher outside lock (may trigger onFlush which takes ps.mu).
	msgSize := int64(len(msg.Key) + len(msg.Value) + 40)
	if err := pm.batcher.Append(ps.globalID, msgSize); err != nil {
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
	// Phase 1: locked — assign offsets, WAL write, replica notify.
	ps.mu.Lock()
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
	if err := ps.wal.AppendBatchLocked(msgs); err != nil {
		ps.mu.Unlock()
		return nil, fmt.Errorf("WAL append batch: %w", err)
	}
	pm.postAppendLocked(ps, topic, partitionID, offsets)
	ps.mu.Unlock()

	// Phase 2: unlocked — batcher notify (may trigger onFlush which takes ps.mu).
	return offsets, pm.notifyBatcher(ps, msgs)
}

// IdempotencyOpts carries idempotency parameters for AppendBatchWithMeta.
// When non-nil, the idempotency check, WAL write, and offset recording all
// happen atomically under ps.mu — preventing sequence advance without
// data write and ensuring LastOffset is set before the lock releases.
type IdempotencyOpts struct {
	Sequence uint64
}

// AppendBatchWithMeta writes messages with producer metadata to the WAL.
// If idem is non-nil, the idempotency check is performed atomically with
// the WAL write under ps.mu. Returns ErrDuplicateSequence if duplicate.
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
	// Phase 1: locked — idempotency + offsets + WAL + replica notify.
	ps.mu.Lock()

	if idem != nil {
		if err := ps.checkAndAdvanceSeq(batch.ProducerID, idem.Sequence, len(batch.Messages)); err != nil {
			ps.mu.Unlock()
			return nil, err
		}
	}

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

	if err := ps.wal.AppendBatchWithMetaLocked(batch); err != nil {
		if idem != nil {
			ps.rollbackSeq(batch.ProducerID, idem.Sequence)
		}
		ps.mu.Unlock()
		return nil, fmt.Errorf("WAL append batch: %w", err)
	}

	if idem != nil {
		ps.recordLastOffset(batch.ProducerID, offsets[len(offsets)-1])
	}

	pm.postAppendLocked(ps, topic, partitionID, offsets)
	ps.mu.Unlock()

	// Phase 2: unlocked — batcher notify (may trigger onFlush which takes ps.mu).
	return offsets, pm.notifyBatcher(ps, batch.Messages)
}

// postAppendLocked performs the replica-notify part of post-append.
// Must be called under ps.mu.Lock.
func (pm *PartitionManager) postAppendLocked(ps *partitionState, topic string, partitionID int, offsets []uint64) {
	if ps.replicaState != nil {
		ps.replicaState.SetLeaderOffset(offsets[len(offsets)-1] + 1)
		ps.replicaState.NotifyNewData()
	}
	// Cache globalID lazily under ps.mu.
	if !ps.globalIDSet {
		ps.globalID = pm.getGlobalID(topic, partitionID)
		ps.globalIDSet = true
	}
}

// notifyBatcher sends size update to the batcher. Must be called OUTSIDE ps.mu
// because batcher.Append may synchronously trigger onFlush which takes ps.mu.
func (pm *PartitionManager) notifyBatcher(ps *partitionState, msgs []log.Message) error {
	totalBatchSize := int64(0)
	for _, msg := range msgs {
		totalBatchSize += int64(len(msg.Key) + len(msg.Value) + 40)
	}
	if err := pm.batcher.Append(ps.globalID, totalBatchSize); err != nil {
		return fmt.Errorf("batcher append: %w", err)
	}
	return nil
}

// AppendReplicatedBatches writes batches to the given partition preserving their
// existing offsets (set by the leader) and producer metadata. It does NOT reassign offsets.
func (pm *PartitionManager) AppendReplicatedBatchFrames(ctx context.Context, topic string, partitionID int, frames []log.BatchFrame) error {
	pm.mu.RLock()
	ps, ok := pm.partitions[topic][partitionID]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("partition %s/%d not found", topic, partitionID)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, frame := range frames {
		if frame.Meta.MessageCount == 0 || len(frame.Data) == 0 {
			continue
		}
		if err := ps.wal.AppendBatchFrameLocked(frame.Data, frame.Meta); err != nil {
			return err
		}
		if frame.Meta.LastOffset+1 > ps.nextOffset {
			ps.nextOffset = frame.Meta.LastOffset + 1
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

	ps.mu.Lock()
	ps.index = idx
	ps.mu.Unlock()
}

// GetDiskCache returns the disk cache used by the partition manager.
func (pm *PartitionManager) GetDiskCache() *log.DiskCache {
	return pm.diskCache
}

// GetIndex returns the partition index.
func (pm *PartitionManager) GetIndex(topic string, partitionID int) *log.Index {
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
	pm.mu.RUnlock()

	ps.mu.RLock()
	idx := ps.index
	ps.mu.RUnlock()
	return idx
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
	pm.mu.RLock()
	parts, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return
	}
	ps, ok := parts[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return
	}
	pm.mu.RUnlock()

	ps.mu.Lock()
	if highWatermark > ps.followerHW {
		ps.followerHW = highWatermark
	}
	if flushedOffset > ps.flushedOffset {
		ps.flushedOffset = flushedOffset
	}
	ps.mu.Unlock()
}

// TruncateWAL removes WAL entries before the given offset for a partition.
func (pm *PartitionManager) TruncateWAL(topic string, pid int, beforeOffset uint64) error {
	ps := pm.GetPartitionState(topic, pid)
	if ps == nil {
		return fmt.Errorf("partition %s/%d not found", topic, pid)
	}
	ps.mu.Lock()
	err := ps.wal.TruncateBeforeLocked(beforeOffset)
	ps.mu.Unlock()
	return err
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

// ScanAndRebuildProducerState reads the unflushed WAL tail for a partition and
// rebuilds ps.producerSeqs from batch-level producer metadata.
// Only committed batches (offset ≤ high watermark) are included — uncommitted
// batches may be truncated on failover and must not advance sequence state.
func (pm *PartitionManager) ScanAndRebuildProducerState(topic string, partitionID int) int {
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return 0
	}
	ps, ok := tp[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return 0
	}
	ps.mu.RLock()
	flushedOffset := ps.flushedOffset
	// High watermark: only batches below HW are committed.
	// For rf=1 (no replicaState), all WAL data is committed — use max uint64.
	hw := uint64(math.MaxUint64)
	if ps.replicaState != nil {
		hw = ps.replicaState.HighWatermark()
	}
	ps.mu.RUnlock()
	pm.mu.RUnlock()

	batches, err := ps.wal.ReadBatchMetasFrom(flushedOffset)
	if err != nil {
		slog.Warn("idempotency_wal_scan_failed",
			"topic", topic, "partition", partitionID, "error", err)
		return 0
	}

	// Filter to committed batches only.
	var committed []log.BatchMeta
	for _, b := range batches {
		if b.ProducerID == 0 {
			continue
		}
		if b.LastOffset >= hw {
			continue
		}
		committed = append(committed, b)
	}
	if len(committed) > 0 {
		ps.mu.Lock()
		ps.rebuildProducerSeqsFromBatches(committed)
		ps.mu.Unlock()
	}
	return len(committed)
}

// EvictStaleProducers iterates over all partitions and evicts producers idle for
// longer than ttl. Returns total evictions across all partitions.
func (pm *PartitionManager) EvictStaleProducers(ttl time.Duration) int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var total int
	for _, tp := range pm.partitions {
		for _, ps := range tp {
			ps.mu.Lock()
			total += ps.evictStaleProducers(ttl)
			ps.mu.Unlock()
		}
	}
	return total
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
	ps.mu.Lock()
	if err := ps.wal.SealLocked(); err != nil {
		ps.mu.Unlock()
		return fmt.Errorf("seal WAL: %w", err)
	}

	// Read all sealed, not-yet-flushed chunks. Flushed-retained chunks stay in
	// WAL for follower catch-up but are excluded from the flush input.
	batches, err := ps.wal.ReplaySealedBatchesLocked()
	if err != nil {
		ps.mu.Unlock()
		return fmt.Errorf("WAL replay: %w", err)
	}
	if len(batches) == 0 {
		slog.Debug("flush_skipped_empty_wal",
			"topic", topic,
			"partition", partitionID,
			"epoch", ps.epoch,
			"next_offset", ps.nextOffset,
			"flushed_offset", ps.flushedOffset,
			"index_hw", ps.index.HighWatermark(),
		)
		ps.mu.Unlock()
		return nil
	}

	messageCount := 0
	for _, batch := range batches {
		messageCount += len(batch.Messages)
	}
	baseOffset := batches[0].Messages[0].Offset
	lastBatch := batches[len(batches)-1]
	endOffset := lastBatch.Messages[len(lastBatch.Messages)-1].Offset
	epoch := ps.epoch
	ps.mu.Unlock()

	ps.mu.RLock()
	flushNextOffset := ps.nextOffset
	flushFlushedOffset := ps.flushedOffset
	flushHW := ps.index.HighWatermark()
	if ps.replicaState != nil {
		flushHW = ps.replicaState.HighWatermark()
	}
	flushIsLeader := ps.isLeader
	ps.mu.RUnlock()

	slog.Info("flush_begin",
		"topic", topic,
		"partition", partitionID,
		"epoch", epoch,
		"message_count", messageCount,
		"base_offset", baseOffset,
		"end_offset", endOffset,
		"next_offset", flushNextOffset,
		"flushed_offset", flushFlushedOffset,
		"hw", flushHW,
		"is_leader", flushIsLeader,
	)

	// 1. Serialize messages into segment binary format.
	var segBuf bytes.Buffer
	compression := pm.segmentsCfg.Compression
	if compression == "" {
		compression = log.CompressionNone
	}
	if err := log.WriteSegmentBatches(&segBuf, batches, compression, pm.segmentsCfg.RecordBatchTargetSize); err != nil {
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
	segMetaData, err := log.BuildSegmentMetadata(segRef, messageCount, int64(len(segData)), compression)
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
	ps.mu.RLock()
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
	ps.mu.RUnlock()
	stateData, err := partState.Marshal()
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	stateKey := log.StateKey(topic, partitionID)
	if err := pm.s3Client.Put(ctx, stateKey, stateData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload state.json: %w", err)
	}

	// Update in-memory segment index.
	ps.mu.Lock()
	ps.index.Add(segRef)
	ps.index.SetHighWatermark(hw)
	ps.mu.Unlock()

	slog.Info("segment_flushed",
		"topic", topic,
		"partition", partitionID,
		"base_offset", baseOffset,
		"end_offset", endOffset,
		"size_bytes", len(segData),
	)

	// Upload per-partition producer idempotency checkpoint from ps.producerSeqs.
	ps.mu.RLock()
	snap := ps.snapshotProducerSeqs()
	ps.mu.RUnlock()
	if len(snap) > 0 {
		var cpBuf bytes.Buffer
		for pid, st := range snap {
			line, err := json.Marshal(producerCheckpointEntry{
				ProducerID: pid,
				NextSeq:    st.NextSeq,
				LastOffset: st.LastOffset,
			})
			if err != nil {
				slog.Warn("flush_idempotency_checkpoint_failed", "topic", topic, "partition", partitionID, "error", err)
				break
			}
			cpBuf.Write(line)
			cpBuf.WriteByte('\n')
		}
		if cpBuf.Len() > 0 {
			checkpointKey := fmt.Sprintf("%s/%d/producers.checkpoint", topic, partitionID)
			if err := pm.s3Client.Put(ctx, checkpointKey, cpBuf.Bytes(), storage.PutOpts{}); err != nil {
				slog.Warn("flush_idempotency_checkpoint_upload_failed", "topic", topic, "partition", partitionID, "error", err)
			}
		}
	}

	// 5. Mark flushed chunks as retained for replica catch-up. They remain
	// readable via WAL until all followers move past them.
	ps.mu.Lock()
	if err := ps.wal.MarkFlushedLocked(endOffset + 1); err != nil {
		ps.mu.Unlock()
		return fmt.Errorf("mark WAL flushed: %w", err)
	}

	retainBefore := endOffset + 1
	if ps.replicaState != nil {
		if minFollowerOffset, ok := ps.replicaState.MinFollowerOffset(); ok && minFollowerOffset < retainBefore {
			retainBefore = minFollowerOffset
		}
	}
	if retainBefore > 0 {
		if err := ps.wal.TruncateBeforeLocked(retainBefore); err != nil {
			ps.mu.Unlock()
			return fmt.Errorf("truncate WAL retained chunks: %w", err)
		}
	}
	retentionHW := uint64(0)
	if ps.replicaState != nil {
		retentionHW = ps.replicaState.HighWatermark()
	}

	// 6. Track highest offset successfully flushed to S3.
	if endOffset > ps.flushedOffset {
		ps.flushedOffset = endOffset
	}
	ps.mu.Unlock()

	slog.Info("wal_retention_updated", "topic", topic, "partition", partitionID,
		"retain_before", retainBefore, "flushed_end", endOffset,
		"hw", retentionHW)

	return nil
}
