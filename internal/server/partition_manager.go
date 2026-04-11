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
	"sort"
	"strconv"
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

func (pm *PartitionManager) ensureActiveSegment(topic string, partitionID int) error {
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return fmt.Errorf("topic %q not initialized", topic)
	}
	ps, ok := tp[partitionID]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("partition %d not initialized for topic %q", partitionID, topic)
	}

	ps.mu.RLock()
	if ps.activeSegment != nil {
		ps.mu.RUnlock()
		return nil
	}
	nextOffset := ps.nextOffset
	ps.mu.RUnlock()

	dir := pm.activeSegmentDir(topic, partitionID)
	baseOffset := int64(nextOffset)
	if matches, err := filepath.Glob(filepath.Join(dir, "*.log")); err == nil && len(matches) > 0 {
		name := filepath.Base(matches[0])
		if len(name) > len(".log") {
			if parsed, err := strconv.ParseInt(name[:len(name)-len(".log")], 10, 64); err == nil {
				baseOffset = parsed
			}
		}
	}

	seg, err := log.OpenActiveSegment(dir, baseOffset)
	if err != nil {
		return fmt.Errorf("open active segment for %s/%d: %w", topic, partitionID, err)
	}
	if err := seg.Recover(); err != nil {
		_ = seg.Close()
		return fmt.Errorf("recover active segment for %s/%d: %w", topic, partitionID, err)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.activeSegment != nil {
		_ = seg.Close()
		return nil
	}
	ps.activeSegment = seg
	if segNext := seg.NextOffset(); uint64(segNext) > ps.nextOffset {
		ps.nextOffset = uint64(segNext)
	}
	return nil
}

func (pm *PartitionManager) activeSegmentLogEnd(topic string, partitionID int) (uint64, bool) {
	ps := pm.GetPartitionState(topic, partitionID)
	if ps == nil {
		return 0, false
	}
	ps.mu.RLock()
	seg := ps.activeSegment
	ps.mu.RUnlock()
	if seg == nil {
		return 0, false
	}
	return uint64(seg.NextOffset()), true
}

func (pm *PartitionManager) localPartitionDir(topic string, partitionID int) string {
	return filepath.Join(pm.localDir, topic, fmt.Sprintf("%d", partitionID))
}

func (pm *PartitionManager) activeSegmentDir(topic string, partitionID int) string {
	return filepath.Join(pm.localPartitionDir(topic, partitionID), "active")
}

func (pm *PartitionManager) EpochHistoryPath(topic string, partitionID int) string {
	return filepath.Join(pm.localPartitionDir(topic, partitionID), "epochs.json")
}

func (pm *PartitionManager) hasNativeRecoveryData(topic string, partitionID int) bool {
	ps := pm.GetPartitionState(topic, partitionID)
	if ps == nil {
		return false
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.index != nil && ps.index.NextOffset() > 0 {
		return true
	}
	return ps.activeSegment != nil && len(ps.activeSegment.OffsetIndex()) > 0
}

func (pm *PartitionManager) recoverLocalLogEnd(topic string, partitionID int) uint64 {
	ps := pm.GetPartitionState(topic, partitionID)
	if ps == nil {
		return 0
	}

	ps.mu.RLock()
	logEnd := ps.nextOffset
	if ps.activeSegment != nil {
		if segNext := uint64(ps.activeSegment.NextOffset()); segNext > logEnd {
			logEnd = segNext
		}
	}
	ps.mu.RUnlock()
	return logEnd
}

func (pm *PartitionManager) flushRecoveredTail(topic string, partitionID int) error {
	ps := pm.GetPartitionState(topic, partitionID)
	if ps == nil {
		return fmt.Errorf("partition %s/%d not found", topic, partitionID)
	}
	ps.mu.RLock()
	hasActiveData := ps.activeSegment != nil && len(ps.activeSegment.OffsetIndex()) > 0
	ps.mu.RUnlock()
	if hasActiveData {
		return pm.onFlushActiveSegment(topic, partitionID)
	}
	return nil
}

func (pm *PartitionManager) RebuildProducerStateFromLocalTail(topic string, partitionID int) (string, int) {
	return "active_segment", pm.ScanAndRebuildProducerStateFromActiveSegment(topic, partitionID)
}

// producerCheckpointEntry is a single line in the NDJSON checkpoint file.
type producerCheckpointEntry struct {
	ProducerID uint64 `json:"producer_id"`
	NextSeq    uint64 `json:"next_seq"`
	LastOffset uint64 `json:"last_offset"`
}

// partitionState holds per-partition runtime state.
type partitionState struct {
	mu            sync.RWMutex       // unified per-partition lock for all read/write access
	activeSegment *log.ActiveSegment // zero-copy RecordBatch storage (nil until wired)
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

// rebuildProducerSeqsFromBatches replays batch metadata to advance sequence
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

// PartitionManager manages per-partition native log state, indexes, and batching.
type PartitionManager struct {
	mu          sync.RWMutex
	s3Client    *storage.S3Client
	diskCache   *log.DiskCache
	partitions  map[string]map[int]*partitionState // topic -> partitionID -> state
	routers     map[string]*producer.Router
	batcher     *producer.Batcher
	localDir    string
	segmentsCfg config.SegmentsConfig

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

	localDir := filepath.Join(cacheDir, "local")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return nil, fmt.Errorf("partition manager: create local dir: %w", err)
	}

	pm := &PartitionManager{
		s3Client:    s3Client,
		diskCache:   diskCache,
		partitions:  make(map[string]map[int]*partitionState),
		routers:     make(map[string]*producer.Router),
		localDir:    localDir,
		segmentsCfg: cfg.Segments,
		globalIDMap: make(map[int]topicPartition),
		reverseMap:  make(map[topicPartition]int),
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
		OnFlush: pm.onFlushDispatch,
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
	return pm.initTopicLocked(ctx, tc, epochs)
}

func (pm *PartitionManager) AddTopicPartitions(ctx context.Context, tc meta.TopicConfig, epochs map[int]uint64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[tc.Name]; !exists {
		return pm.initTopicLocked(ctx, tc, epochs)
	}

	topicPartitions := pm.partitions[tc.Name]
	for pid := 0; pid < tc.Partitions; pid++ {
		if _, exists := topicPartitions[pid]; exists {
			continue
		}
		epoch := epochs[pid]
		ps, err := pm.initPartition(ctx, tc.Name, pid, epoch)
		if err != nil {
			return fmt.Errorf("add topic %q partition %d: %w", tc.Name, pid, err)
		}
		topicPartitions[pid] = ps
		pm.getGlobalID(tc.Name, pid)
	}

	pm.routers[tc.Name] = producer.NewRouter(tc.Partitions)
	slog.Info("topic_partitions_added", "topic", tc.Name, "partitions", tc.Partitions)
	return nil
}

func (pm *PartitionManager) initTopicLocked(ctx context.Context, tc meta.TopicConfig, epochs map[int]uint64) error {
	if _, exists := pm.partitions[tc.Name]; exists {
		return nil
	}

	topicPartitions := make(map[int]*partitionState)

	for pid := 0; pid < tc.Partitions; pid++ {
		epoch := epochs[pid]
		ps, err := pm.initPartition(ctx, tc.Name, pid, epoch)
		if err != nil {
			for _, p := range topicPartitions {
				if p.activeSegment != nil {
					_ = p.activeSegment.Close()
				}
			}
			return fmt.Errorf("init topic %q partition %d: %w", tc.Name, pid, err)
		}
		topicPartitions[pid] = ps
		pm.getGlobalID(tc.Name, pid)
	}

	pm.partitions[tc.Name] = topicPartitions
	pm.routers[tc.Name] = producer.NewRouter(tc.Partitions)
	slog.Info("topic_initialized", "topic", tc.Name, "partitions", tc.Partitions)
	return nil
}

// initPartition initializes a single partition from native storage state.
// The epoch comes from the acquired lease — if a previous epoch's local active
// segment exists, it is discarded for fencing.
func (pm *PartitionManager) initPartition(ctx context.Context, topic string, partitionID int, epoch uint64) (*partitionState, error) {
	// 1. Load segment list from S3 by listing objects.
	prefix := log.ListSegmentPrefix(topic, partitionID)
	keys, err := pm.s3Client.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list segments for %s/%d: %w", topic, partitionID, err)
	}
	idx := log.NewIndex()
	for _, ref := range pm.loadSegmentRefs(ctx, log.SegmentRefsFromKeys(keys)) {
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

	// 2. Check epoch fencing for local active-segment state.
	partitionDir := pm.localPartitionDir(topic, partitionID)
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		return nil, fmt.Errorf("create local partition dir: %w", err)
	}
	epochFile := filepath.Join(partitionDir, "epoch")
	var prevEpoch uint64
	if epochData, err := os.ReadFile(epochFile); err == nil {
		_, _ = fmt.Sscanf(string(epochData), "%d", &prevEpoch)
	}

	if epoch > prevEpoch && prevEpoch > 0 {
		slog.Warn("epoch fencing: discarding stale local segment state",
			"topic", topic, "partition", partitionID,
			"previous_epoch", prevEpoch, "lease_epoch", epoch)
		_ = os.RemoveAll(pm.activeSegmentDir(topic, partitionID))
	}

	// 3. Write current epoch to sidecar file.
	if err := fsutil.AtomicWriteFile(epochFile, []byte(fmt.Sprintf("%d", epoch)), 0o644); err != nil {
		return nil, fmt.Errorf("write epoch sidecar: %w", err)
	}

	// 4. Native storage is authoritative for partition recovery.
	nextOffset := idx.NextOffset()

	slog.Info("partition_state_loaded",
		"topic", topic,
		"partition", partitionID,
		"epoch", epoch,
		"index_next_offset", idx.NextOffset(),
		"index_hw", idx.HighWatermark(),
		"next_offset", nextOffset,
	)

	flushedOffset := uint64(0)
	if idx.NextOffset() > 0 {
		flushedOffset = idx.NextOffset() - 1
	}

	return &partitionState{
		index:         idx,
		nextOffset:    nextOffset,
		epoch:         epoch,
		flushedOffset: flushedOffset,
		producerSeqs:  make(map[uint64]*producerPartitionState),
	}, nil
}

// Append adds a message to the given topic/partition, assigns an offset, writes
// it to the active segment, and enqueues for flushing. Returns the assigned offset.
func (pm *PartitionManager) Append(ctx context.Context, topic string, partitionID int, msg log.Message) (uint64, error) {
	offsets, err := pm.AppendBatch(ctx, topic, partitionID, []log.Message{msg})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

// AppendBatch writes multiple messages to the same partition as one native
// batch. Returns the assigned offsets for each message.
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
	batch := log.Batch{Messages: msgs}
	return pm.appendNativeBatchToPS(ps, topic, partitionID, batch, nil)
}

// IdempotencyOpts carries idempotency parameters for AppendBatchWithMeta.
// When non-nil, the idempotency check, append, and offset recording all
// happen atomically under ps.mu — preventing sequence advance without
// data write and ensuring LastOffset is set before the lock releases.
type IdempotencyOpts struct {
	Sequence uint64
}

// AppendBatchWithMeta writes messages with producer metadata to the active
// segment. Returns ErrDuplicateSequence if duplicate.
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
	return pm.appendNativeBatchToPS(ps, topic, partitionID, batch, idem)
}

func (pm *PartitionManager) appendNativeBatchToPS(ps *partitionState, topic string, partitionID int, batch log.Batch, idem *IdempotencyOpts) ([]uint64, error) {
	if len(batch.Messages) == 0 {
		return nil, nil
	}
	if err := pm.ensureActiveSegment(topic, partitionID); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	for i := range batch.Messages {
		if batch.Messages[i].Timestamp == 0 {
			batch.Messages[i].Timestamp = now
		}
	}

	rawBatch := log.EncodeRecordBatchWithMeta(0, batch)
	baseOffset, err := pm.appendRawBatchToPS(ps, topic, partitionID, rawBatch, idem, false)
	if err != nil {
		return nil, err
	}

	offsets := make([]uint64, len(batch.Messages))
	for i := range offsets {
		offsets[i] = uint64(baseOffset) + uint64(i)
	}
	return offsets, nil
}

// AppendRawBatch writes a raw Kafka v2 RecordBatch to the active segment,
// patching offsets and leader epoch in place. This is the zero-copy produce
// path — no record-level decoding or re-serialization.
func (pm *PartitionManager) AppendRawBatch(ctx context.Context, topic string, partitionID int, batch []byte) (int64, error) {
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

	return pm.appendRawBatchToPS(ps, topic, partitionID, batch, nil, true)
}

func (pm *PartitionManager) appendRawBatchToPS(ps *partitionState, topic string, partitionID int, batch []byte, idem *IdempotencyOpts, enforceLeader bool) (int64, error) {
	h, err := log.ReadRecordBatchHeader(batch)
	if err != nil {
		return 0, fmt.Errorf("read record batch header: %w", err)
	}

	numRecords := int64(h.LastOffsetDelta) + 1

	ps.mu.Lock()

	if enforceLeader && !ps.isLeader {
		ps.mu.Unlock()
		return 0, fmt.Errorf("%w: partition %d", errKafkaNotLeader, partitionID)
	}

	if ps.activeSegment == nil {
		ps.mu.Unlock()
		return 0, fmt.Errorf("active segment not initialized for %s/%d", topic, partitionID)
	}

	// Idempotency check BEFORE append.
	if h.ProducerID >= 0 {
		seq := uint64(h.FirstSequence)
		if idem != nil {
			seq = idem.Sequence
		}
		if err := ps.checkAndAdvanceSeq(uint64(h.ProducerID), seq, int(numRecords)); err != nil {
			ps.mu.Unlock()
			return 0, err
		}
	}

	// Assign offsets.
	baseOffset := int64(ps.nextOffset)
	if err := log.PatchRecordBatchFirstOffset(batch, baseOffset); err != nil {
		if h.ProducerID >= 0 {
			ps.rollbackSeq(uint64(h.ProducerID), uint64(h.FirstSequence))
		}
		ps.mu.Unlock()
		return 0, fmt.Errorf("patch first offset: %w", err)
	}
	if err := log.PatchRecordBatchLeaderEpoch(batch, int32(ps.epoch)); err != nil {
		if h.ProducerID >= 0 {
			ps.rollbackSeq(uint64(h.ProducerID), uint64(h.FirstSequence))
		}
		ps.mu.Unlock()
		return 0, fmt.Errorf("patch leader epoch: %w", err)
	}

	// Write to active segment.
	if err := ps.activeSegment.Append(batch); err != nil {
		if h.ProducerID >= 0 {
			ps.rollbackSeq(uint64(h.ProducerID), uint64(h.FirstSequence))
		}
		ps.mu.Unlock()
		return 0, fmt.Errorf("active segment append: %w", err)
	}

	// Update producer state AFTER successful append.
	if h.ProducerID >= 0 {
		lastOffset := uint64(baseOffset) + uint64(numRecords) - 1
		ps.recordLastOffset(uint64(h.ProducerID), lastOffset)
	}

	ps.nextOffset += uint64(numRecords)

	// Replica notify.
	offsets := make([]uint64, numRecords)
	for i := range offsets {
		offsets[i] = uint64(baseOffset) + uint64(i)
	}
	pm.postAppendLocked(ps, topic, partitionID, offsets)
	ps.mu.Unlock()

	// Phase 2: unlocked — batcher notify.
	batchSize := int64(len(batch))
	if !ps.globalIDSet {
		// globalID should have been set by postAppendLocked, but guard anyway.
		ps.mu.Lock()
		if !ps.globalIDSet {
			ps.globalID = pm.getGlobalID(topic, partitionID)
			ps.globalIDSet = true
		}
		ps.mu.Unlock()
	}
	if err := pm.batcher.Append(ps.globalID, batchSize); err != nil {
		return baseOffset, fmt.Errorf("batcher append: %w", err)
	}

	return baseOffset, nil
}

// postAppendLocked performs the replica-notify part of post-append.
// Must be called under ps.mu.Lock.
func (pm *PartitionManager) postAppendLocked(ps *partitionState, topic string, partitionID int, offsets []uint64) {
	if ps.replicaState != nil {
		ps.replicaState.SetLeaderOffset(offsets[len(offsets)-1] + 1)
		if hw := ps.replicaState.HighWatermark(); hw > ps.index.HighWatermark() {
			ps.index.SetHighWatermark(hw)
		}
		ps.replicaState.NotifyNewData()
	} else if next := offsets[len(offsets)-1] + 1; next > ps.index.HighWatermark() {
		ps.index.SetHighWatermark(next)
	}
	// Cache globalID lazily under ps.mu.
	if !ps.globalIDSet {
		ps.globalID = pm.getGlobalID(topic, partitionID)
		ps.globalIDSet = true
	}
}

// AppendReplicatedRawBatches writes raw Kafka v2 RecordBatch bytes to the
// active segment for the given partition. This is the new replication path for
// followers once the leader writes RecordBatch to sealed segments. Offsets are
// already assigned by the leader; no offset patching or idempotency check is
// performed.
func (pm *PartitionManager) AppendReplicatedRawBatches(ctx context.Context, topic string, partitionID int, batches [][]byte) error {
	pm.mu.RLock()
	ps, ok := pm.partitions[topic][partitionID]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("partition %s/%d not found", topic, partitionID)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, batch := range batches {
		h, err := log.ReadRecordBatchHeader(batch)
		if err != nil {
			return fmt.Errorf("AppendReplicatedRawBatches: read header: %w", err)
		}
		if ps.activeSegment == nil {
			return fmt.Errorf("AppendReplicatedRawBatches: partition %s/%d has no active segment", topic, partitionID)
		}
		if err := ps.activeSegment.Append(batch); err != nil {
			return fmt.Errorf("AppendReplicatedRawBatches: append: %w", err)
		}
		end := uint64(h.FirstOffset+int64(h.LastOffsetDelta)) + 1
		if end > ps.nextOffset {
			ps.nextOffset = end
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
	for _, ref := range pm.loadSegmentRefs(ctx, log.SegmentRefsFromKeys(keys)) {
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
	if nextOffset := idx.NextOffset(); nextOffset > 0 {
		refreshedFlushedOffset := nextOffset - 1
		if refreshedFlushedOffset > ps.flushedOffset {
			ps.flushedOffset = refreshedFlushedOffset
		}
	}
	ps.mu.Unlock()
}

func (pm *PartitionManager) loadSegmentRefs(ctx context.Context, refs []log.SegmentRef) []log.SegmentRef {
	for i := range refs {
		data, err := pm.s3Client.Get(ctx, refs[i].MetaObjectKey())
		if err != nil {
			continue
		}
		var meta log.SegmentMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		refs[i].MinTimestamp = meta.MinTimestamp
		refs[i].MaxTimestamp = meta.MaxTimestamp
	}
	return refs
}

// GetDiskCache returns the disk cache used by the partition manager.
func (pm *PartitionManager) GetDiskCache() *log.DiskCache {
	return pm.diskCache
}

// RemoveSealedSegmentObjects removes sealed-segment refs for the given object
// keys from the local partition index and evicts matching disk-cache entries.
func (pm *PartitionManager) RemoveSealedSegmentObjects(topic string, partitionID int, keys ...string) {
	ps := pm.GetPartitionState(topic, partitionID)
	if ps != nil {
		ps.mu.Lock()
		if ps.index != nil {
			ps.index.RemoveObjectKeys(keys...)
		}
		ps.mu.Unlock()
	}
	if pm.diskCache != nil {
		for _, key := range keys {
			if key == "" {
				continue
			}
			pm.diskCache.Delete(key)
		}
	}
}

// InstallSealedSegment swaps old sealed-segment refs out of the local index,
// adds ref, and updates the disk cache with the merged artifact.
func (pm *PartitionManager) InstallSealedSegment(topic string, partitionID int, ref log.SegmentRef, segData, sidecarData, metaData []byte, removeKeys ...string) {
	ps := pm.GetPartitionState(topic, partitionID)
	if ps != nil {
		ps.mu.Lock()
		if ps.index != nil {
			ps.index.RemoveObjectKeys(removeKeys...)
			ps.index.Add(ref)
		}
		ps.mu.Unlock()
	}
	if pm.diskCache != nil {
		for _, key := range removeKeys {
			if key == "" {
				continue
			}
			pm.diskCache.Delete(key)
		}
		if len(segData) > 0 {
			_ = pm.diskCache.Put(ref.Key, segData)
		}
		if len(sidecarData) > 0 {
			_ = pm.diskCache.Put(ref.OffsetIndexObjectKey(), sidecarData)
		}
		if len(metaData) > 0 {
			_ = pm.diskCache.Put(ref.MetaObjectKey(), metaData)
		}
	}
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

func (pm *PartitionManager) RemoveTopic(topic string) {
	pm.mu.Lock()
	parts := pm.partitions[topic]
	delete(pm.partitions, topic)
	delete(pm.routers, topic)
	pm.mu.Unlock()

	var doneChans []chan struct{}
	for _, ps := range parts {
		if ps.fetchCancel != nil {
			ps.fetchCancel()
			if ps.fetchDone != nil {
				doneChans = append(doneChans, ps.fetchDone)
			}
		}
	}
	for _, ch := range doneChans {
		<-ch
	}
	for _, ps := range parts {
		ps.mu.Lock()
		if ps.activeSegment != nil {
			_ = ps.activeSegment.Close()
			ps.activeSegment = nil
		}
		ps.index = nil
		ps.mu.Unlock()
	}

	pm.globalIDMu.Lock()
	for tp, id := range pm.reverseMap {
		if tp.topic != topic {
			continue
		}
		delete(pm.reverseMap, tp)
		delete(pm.globalIDMap, id)
	}
	pm.globalIDMu.Unlock()

	_ = os.RemoveAll(filepath.Join(pm.localDir, topic))
}

// UpdateFollowerProgress records the latest leader-advertised epoch, readable
// high-watermark, and flushed offset for a follower partition.
func (pm *PartitionManager) UpdateFollowerProgress(topic string, partitionID int, leaderEpoch, highWatermark, flushedOffset uint64) {
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
	if leaderEpoch > ps.epoch {
		ps.epoch = leaderEpoch
	}
	if highWatermark > ps.followerHW {
		ps.followerHW = highWatermark
	}
	if flushedOffset > ps.flushedOffset {
		ps.flushedOffset = flushedOffset
	}
	ps.mu.Unlock()
}

// TruncateLogFrom removes local active-segment data at and above the given
// offset for a partition.
func (pm *PartitionManager) TruncateLogFrom(topic string, pid int, offset uint64) error {
	ps := pm.GetPartitionState(topic, pid)
	if ps == nil {
		return fmt.Errorf("partition %s/%d not found", topic, pid)
	}
	ps.mu.Lock()
	var err error
	if ps.activeSegment != nil {
		err = ps.activeSegment.TruncateFrom(int64(offset))
	}
	if ps.nextOffset > offset {
		ps.nextOffset = offset
	}
	if ps.followerHW > ps.nextOffset {
		ps.followerHW = ps.nextOffset
	}
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

func (pm *PartitionManager) ScanAndRebuildProducerStateFromActiveSegment(topic string, partitionID int) int {
	ps := pm.GetPartitionState(topic, partitionID)
	if ps == nil {
		return 0
	}

	ps.mu.RLock()
	seg := ps.activeSegment
	flushedOffset := ps.flushedOffset
	hw := uint64(math.MaxUint64)
	if ps.replicaState != nil {
		hw = ps.replicaState.HighWatermark()
	}
	ps.mu.RUnlock()
	if seg == nil {
		return 0
	}

	offsetIdx := seg.OffsetIndex()
	if len(offsetIdx) == 0 {
		return 0
	}

	committed := make([]log.BatchMeta, 0, len(offsetIdx))
	for _, entry := range offsetIdx {
		if entry.BatchSize <= 0 || entry.Position < 0 {
			continue
		}
		buf := make([]byte, entry.BatchSize)
		n, err := seg.ReadAt(buf, entry.Position)
		if err != nil && n < int(entry.BatchSize) {
			continue
		}
		h, err := log.ReadRecordBatchHeader(buf[:n])
		if err != nil || h.ProducerID < 0 {
			continue
		}
		meta := log.BatchMeta{
			ProducerID:   uint64(h.ProducerID),
			Sequence:     uint64(h.FirstSequence),
			MessageCount: int(h.LastOffsetDelta) + 1,
			FirstOffset:  uint64(h.FirstOffset),
			LastOffset:   uint64(h.LastOffset()),
		}
		if meta.LastOffset <= flushedOffset || meta.LastOffset >= hw {
			continue
		}
		committed = append(committed, meta)
	}
	if len(committed) == 0 {
		return 0
	}

	ps.mu.Lock()
	ps.rebuildProducerSeqsFromBatches(committed)
	ps.mu.Unlock()
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

// Shutdown stops the batcher and closes all active segments.
func (pm *PartitionManager) Shutdown(ctx context.Context) error {
	pm.batcher.Stop()

	pm.mu.Lock()
	defer pm.mu.Unlock()

	var firstErr error
	for _, topicPartitions := range pm.partitions {
		for _, ps := range topicPartitions {
			if ps.activeSegment != nil {
				if err := ps.activeSegment.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	return firstErr
}

// ReadRawBatches reads raw RecordBatch bytes starting at startOffset, up to
// maxBytes. It reads from both sealed segments (via the index) and the active
// segment. Returns the concatenated raw bytes, the high watermark, and any error.
func (pm *PartitionManager) ReadRawBatches(ctx context.Context, topic string, pid int, startOffset int64, maxBytes int) ([]byte, int64, error) {
	return pm.readRawBatchesWithUpperBound(ctx, topic, pid, startOffset, maxBytes, false)
}

// ReadReplicaRawBatches reads raw RecordBatch bytes for follower replication.
// Unlike ReadRawBatches, it is bounded by the partition log end rather than the
// readable high watermark, so followers can catch up on uncommitted tail data.
func (pm *PartitionManager) ReadReplicaRawBatches(ctx context.Context, topic string, pid int, startOffset int64, maxBytes int) ([]byte, int64, error) {
	return pm.readRawBatchesWithUpperBound(ctx, topic, pid, startOffset, maxBytes, true)
}

func (pm *PartitionManager) readRawBatchesWithUpperBound(ctx context.Context, topic string, pid int, startOffset int64, maxBytes int, useLogEnd bool) ([]byte, int64, error) {
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return nil, 0, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}
	ps, ok := tp[pid]
	if !ok {
		pm.mu.RUnlock()
		return nil, 0, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, pid, topic)
	}
	pm.mu.RUnlock()

	ps.mu.RLock()
	hw, hwOK := readableHighWatermark(ps)
	nextOff := ps.nextOffset
	activeSeg := ps.activeSegment
	index := ps.index
	ps.mu.RUnlock()

	upperBound := int64(nextOff)
	if !useLogEnd && hwOK {
		upperBound = int64(hw)
	}

	// Nothing to read if startOffset is at or beyond the readable bound.
	if startOffset >= upperBound {
		return nil, upperBound, nil
	}

	if maxBytes <= 0 {
		maxBytes = 1 << 20 // 1 MiB default
	}

	var out []byte
	currentOffset := startOffset
	remaining := maxBytes

	// Phase 1: Sealed segments (if startOffset < active segment base).
	activeBase := int64(0)
	if activeSeg != nil {
		activeBase = activeSeg.BaseOffset()
	}

	if index != nil && currentOffset < activeBase {
		segments := index.SegmentsFrom(uint64(currentOffset), 0)
		for _, ref := range segments {
			if remaining <= 0 || currentOffset >= activeBase {
				break
			}

			// Read sealed segment data from S3/disk cache.
			segData, err := pm.readSealedSegmentData(ctx, ref)
			if err != nil {
				slog.Warn("ReadRawBatches: failed to read sealed segment",
					"topic", topic, "partition", pid,
					"segment_key", ref.Key, "error", err)
				break
			}

			// Use the sidecar offset index to seek directly to the
			// approximate position instead of scanning from byte 0.
			startPos := 0
			if sidecarData, err := pm.readSealedSegmentSidecar(ctx, ref); err == nil {
				if entries, _, err := log.ReadSidecar(sidecarData); err == nil {
					if pos, ok := log.LookupSidecarPosition(entries, currentOffset); ok {
						startPos = int(pos)
					}
				}
			}

			// Sealed segment data IS raw RecordBatch bytes back-to-back.
			// Slice out batches directly — no decoding or re-encoding needed.
			rawBatches, err := log.ReadSegmentBatchesFromPosition(segData, startPos, uint64(currentOffset), 0)
			if err != nil {
				slog.Warn("ReadRawBatches: failed to parse sealed segment batches",
					"topic", topic, "partition", pid,
					"segment_key", ref.Key, "error", err)
				break
			}

			for _, batch := range rawBatches {
				// Always include at least one batch even if it exceeds remaining.
				if len(out) > 0 && len(out)+len(batch) > maxBytes {
					goto done
				}
				out = append(out, batch...)
				remaining = maxBytes - len(out)
				// Parse the header to advance currentOffset past this batch.
				hdr, hErr := log.ReadRecordBatchHeader(batch)
				if hErr == nil {
					currentOffset = hdr.LastOffset() + 1
				}
			}
		}
	}

	// Phase 2: Active segment.
	if activeSeg != nil && remaining > 0 {
		offsetIdx := activeSeg.OffsetIndex()
		if len(offsetIdx) > 0 {
			// Binary search for the first batch containing currentOffset.
			startIdx := sort.Search(len(offsetIdx), func(i int) bool {
				return offsetIdx[i].LastOffset >= currentOffset
			})

			for i := startIdx; i < len(offsetIdx); i++ {
				entry := offsetIdx[i]

				// Skip batches entirely above HW.
				if entry.BaseOffset >= upperBound {
					break
				}

				// Bounds check.
				if entry.BatchSize <= 0 || entry.Position < 0 {
					continue
				}

				buf := make([]byte, entry.BatchSize)
				n, err := activeSeg.ReadAt(buf, entry.Position)
				if err != nil && n < int(entry.BatchSize) {
					break
				}
				buf = buf[:n]

				// Always include at least one batch even if it exceeds remaining.
				if len(out) > 0 && len(out)+len(buf) > maxBytes {
					break
				}
				out = append(out, buf...)
				remaining = maxBytes - len(out)
				if remaining <= 0 {
					break
				}
			}
		}
	}

done:
	return out, upperBound, nil
}

// readSealedSegmentData reads a sealed segment's data from disk cache or S3.
func (pm *PartitionManager) readSealedSegmentData(ctx context.Context, ref log.SegmentRef) ([]byte, error) {
	// Try disk cache first.
	if pm.diskCache != nil {
		data, err := pm.diskCache.Get(ref.Key)
		if err == nil && len(data) > 0 {
			return data, nil
		}
	}

	// Fall back to S3.
	if pm.s3Client != nil {
		data, err := pm.s3Client.Get(ctx, ref.Key)
		if err != nil {
			return nil, fmt.Errorf("s3 get %s: %w", ref.Key, err)
		}
		// Populate disk cache for next time.
		if pm.diskCache != nil {
			_ = pm.diskCache.Put(ref.Key, data)
		}
		return data, nil
	}

	return nil, fmt.Errorf("no storage backend available for segment %s", ref.Key)
}

// readSealedSegmentSidecar reads the CIDX sidecar for a sealed segment from
// disk cache or S3. Returns an error if unavailable (callers should fall back
// to scanning from byte 0).
func (pm *PartitionManager) readSealedSegmentSidecar(ctx context.Context, ref log.SegmentRef) ([]byte, error) {
	key := ref.OffsetIndexObjectKey()
	if key == "" {
		return nil, fmt.Errorf("no sidecar key for segment %s", ref.Key)
	}
	if pm.diskCache != nil {
		data, err := pm.diskCache.Get(key)
		if err == nil && len(data) > 0 {
			return data, nil
		}
	}
	if pm.s3Client != nil {
		data, err := pm.s3Client.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("s3 get %s: %w", key, err)
		}
		if pm.diskCache != nil {
			_ = pm.diskCache.Put(key, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("no storage backend available for sidecar %s", key)
}

// readSealedSegmentOffsetIndex reads a sealed segment's offset index from disk cache or S3.
func (pm *PartitionManager) readSealedSegmentOffsetIndex(ctx context.Context, ref log.SegmentRef) ([]byte, error) {
	key := ref.OffsetIndexObjectKey()

	// Try disk cache first.
	if pm.diskCache != nil {
		data, err := pm.diskCache.Get(key)
		if err == nil && len(data) > 0 {
			return data, nil
		}
	}

	// Fall back to S3.
	if pm.s3Client != nil {
		data, err := pm.s3Client.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("s3 get %s: %w", key, err)
		}
		if pm.diskCache != nil {
			_ = pm.diskCache.Put(key, data)
		}
		return data, nil
	}

	return nil, fmt.Errorf("no storage backend available for offset index %s", key)
}

// SetLeaseChecker sets a callback to verify partition ownership before flushing.
// The server sets this after creating the partition manager.
func (pm *PartitionManager) SetLeaseChecker(fn func(topic string, partitionID int) bool) {
	pm.leaseChecker = fn
}

// onFlushDispatch is the batcher's flush callback.
func (pm *PartitionManager) onFlushDispatch(globalPartitionID int) error {
	topic, partitionID, ok := pm.resolveGlobalID(globalPartitionID)
	if !ok {
		return fmt.Errorf("unknown global partition ID %d", globalPartitionID)
	}
	return pm.onFlushActiveSegment(topic, partitionID)
}

// onFlushActiveSegment seals the active segment, opens a new one, uploads
// the sealed segment and its sidecar to S3, and updates the partition index.
func (pm *PartitionManager) onFlushActiveSegment(topic string, partitionID int) error {
	// Check lease validity before flushing.
	if pm.leaseChecker != nil && !pm.leaseChecker(topic, partitionID) {
		slog.Warn("onFlushActiveSegment: lease expired, skipping flush",
			"topic", topic, "partition", partitionID)
		return nil
	}

	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return fmt.Errorf("topic %q not found during active segment flush", topic)
	}
	ps, ok := tp[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return fmt.Errorf("partition %d not found for topic %q during active segment flush", partitionID, topic)
	}
	pm.mu.RUnlock()

	ps.mu.Lock()
	oldSeg := ps.activeSegment
	if oldSeg == nil {
		ps.mu.Unlock()
		return nil // nothing to flush
	}
	offsetIdx := oldSeg.OffsetIndex()
	if len(offsetIdx) == 0 {
		ps.mu.Unlock()
		return nil // empty segment, nothing to seal
	}

	// Seal the current active segment (sync + close + write sidecar).
	segmentPath, sidecarPath, err := oldSeg.Seal()
	if err != nil {
		ps.mu.Unlock()
		return fmt.Errorf("seal active segment: %w", err)
	}

	// Open a fresh active segment starting at nextOffset.
	newSeg, err := log.OpenActiveSegment(oldSeg.Dir(), int64(ps.nextOffset))
	if err != nil {
		ps.mu.Unlock()
		return fmt.Errorf("open new active segment: %w", err)
	}
	ps.activeSegment = newSeg
	epoch := ps.epoch
	hw := ps.index.HighWatermark()
	if ps.replicaState != nil {
		hw = ps.replicaState.HighWatermark()
	}
	snap := ps.snapshotProducerSeqs()
	ps.mu.Unlock()

	// --- Everything below is outside the lock ---

	// Read sealed segment file and sidecar from disk.
	segData, err := os.ReadFile(segmentPath)
	if err != nil {
		return fmt.Errorf("read sealed segment: %w", err)
	}
	sidecarData, err := os.ReadFile(sidecarPath)
	if err != nil {
		return fmt.Errorf("read sealed sidecar: %w", err)
	}

	baseOffset := oldSeg.BaseOffset()
	endOffset := offsetIdx[len(offsetIdx)-1].LastOffset
	minTimestamp := offsetIdx[0].FirstTimestamp
	maxTimestamp := offsetIdx[0].MaxTimestamp
	for _, entry := range offsetIdx {
		if entry.FirstTimestamp < minTimestamp {
			minTimestamp = entry.FirstTimestamp
		}
		if entry.MaxTimestamp > maxTimestamp {
			maxTimestamp = entry.MaxTimestamp
		}
	}

	segKey := log.FormatSegmentKey(topic, partitionID, uint64(baseOffset), uint64(endOffset), epoch)
	sidecarKey := log.SegmentOffsetIndexKey(segKey)
	metaKey := log.SegmentMetadataKey(segKey)

	segRef := log.SegmentRef{
		BaseOffset:     uint64(baseOffset),
		EndOffset:      uint64(endOffset),
		MinTimestamp:   minTimestamp,
		MaxTimestamp:   maxTimestamp,
		Epoch:          epoch,
		Key:            segKey,
		OffsetIndexKey: sidecarKey,
		MetaKey:        metaKey,
		CreatedAt:      time.Now(),
	}
	metaData, err := log.BuildSegmentMetadata(segRef, int(endOffset-baseOffset+1), int64(len(segData)), pm.segmentsCfg.Compression)
	if err != nil {
		return fmt.Errorf("build segment metadata: %w", err)
	}

	ctx := context.Background()
	if err := pm.s3Client.Put(ctx, segKey, segData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload sealed segment: %w", err)
	}
	if err := pm.s3Client.Put(ctx, sidecarKey, sidecarData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload sealed sidecar: %w", err)
	}
	if err := pm.s3Client.Put(ctx, metaKey, metaData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload segment metadata: %w", err)
	}

	if pm.diskCache != nil {
		_ = pm.diskCache.Put(segKey, segData)
		_ = pm.diskCache.Put(sidecarKey, sidecarData)
		_ = pm.diskCache.Put(metaKey, metaData)
	}

	partState := log.PartitionState{HighWatermark: hw}
	ps.mu.RLock()
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
	if err := pm.s3Client.Put(ctx, log.StateKey(topic, partitionID), stateData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload state.json: %w", err)
	}

	if len(snap) > 0 {
		var cpBuf bytes.Buffer
		for producerID, st := range snap {
			line, err := json.Marshal(producerCheckpointEntry{
				ProducerID: producerID,
				NextSeq:    st.NextSeq,
				LastOffset: st.LastOffset,
			})
			if err != nil {
				return fmt.Errorf("marshal producers checkpoint: %w", err)
			}
			cpBuf.Write(line)
			cpBuf.WriteByte('\n')
		}
		if err := pm.s3Client.Put(ctx, fmt.Sprintf("%s/%d/producers.checkpoint", topic, partitionID), cpBuf.Bytes(), storage.PutOpts{}); err != nil {
			return fmt.Errorf("upload producers checkpoint: %w", err)
		}
	}

	ps.mu.Lock()
	ps.index.Add(segRef)
	ps.index.SetHighWatermark(hw)
	ps.flushedOffset = uint64(endOffset)
	ps.mu.Unlock()

	// Clean up local files now that they are safely in S3.
	_ = os.Remove(segmentPath)
	_ = os.Remove(sidecarPath)

	slog.Info("active_segment_flushed",
		"topic", topic,
		"partition", partitionID,
		"base_offset", baseOffset,
		"end_offset", endOffset,
		"size_bytes", len(segData),
	)

	return nil
}
