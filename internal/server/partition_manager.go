package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// producerBatchMeta records the exact position of a producer's most recently
// appended batch. A duplicate retry is confirmed only when it exactly matches
// this batch, so it returns the retried batch's own base offset rather than
// deriving one from the producer's latest offset (which would acknowledge an
// earlier sequence range with the later batch's offsets).
type producerBatchMeta struct {
	FirstSequence uint64 `json:"first_sequence"`
	NumRecords    int64  `json:"num_records"`
	BaseOffset    int64  `json:"base_offset"`
	LastOffset    uint64 `json:"last_offset"`
}

// producerPartitionState tracks idempotency sequence for one producer on this partition.
type producerPartitionState struct {
	NextSeq      uint64             `json:"next_seq"`
	LastOffset   uint64             `json:"last_offset"`
	LastActiveAt time.Time          `json:"-"` // not persisted; set on each produce
	LastBatch    *producerBatchMeta `json:"-"` // most recent appended batch; rebuilt on load
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
	baseOffset = newestActiveSegmentBaseOffset(dir, baseOffset)

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

// newestActiveSegmentBaseOffset selects the newest active tail after an
// interrupted local compaction. During compaction both the old prefix-bearing
// file and the newly installed tail can briefly exist; the latter has the
// higher base offset and is the only one that should be reopened.
func newestActiveSegmentBaseOffset(dir string, fallback int64) int64 {
	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return fallback
	}
	newest := fallback
	found := false
	for _, match := range matches {
		name := filepath.Base(match)
		if len(name) <= len(".log") {
			continue
		}
		parsed, err := strconv.ParseInt(name[:len(name)-len(".log")], 10, 64)
		if err == nil && (!found || parsed > newest) {
			newest = parsed
			found = true
		}
	}
	return newest
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
	mu                   sync.RWMutex       // unified per-partition lock for all read/write access
	activeSegment        *log.ActiveSegment // zero-copy RecordBatch storage (nil until wired)
	index                *log.Index
	nextOffset           uint64
	epoch                uint64                    // always 0 in single-instance mode
	replicaState         *replication.ReplicaState // nil for rf=1
	isLeader             bool
	leaderID             string // current leader for follower fetch state; empty when local leader
	flushedOffset        uint64 // highest offset flushed to S3
	followerHW           uint64 // leader-advertised readable HW for follower reads
	epochHistory         *replication.EpochHistory
	fetchCancel          context.CancelFunc                 // cancel follower fetch goroutine
	fetchDone            chan struct{}                      // closed when fetch goroutine exits
	fetchGeneration      uint64                             // fences concurrent follower reconfigurations
	fetchAssignmentEpoch uint64                             // assignment epoch the active fetch is configured to follow
	globalID             int                                // cached batcher partition ID, set on first append
	globalIDSet          bool                               // true once globalID has been resolved
	producerSeqs         map[uint64]*producerPartitionState // producerID -> sequence state
	pendingFlush         *sealedSegment                     // sealed locally; retry this exact segment until uploaded
}

// sealedSegment is immutable upload work. Once a segment is sealed, retries
// must publish this object rather than sealing whichever segment is active next.
type sealedSegment struct {
	topic, segmentPath, sidecarPath string
	partitionID                     int
	ref                             log.SegmentRef
	highWatermark                   uint64
	stateData                       []byte
	producerCheckpoint              []byte
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

// recordAppendedBatch records the exact position of the most recently appended
// batch for a producer. Callers must hold ps.mu.
func (ps *partitionState) recordAppendedBatch(producerID, firstSeq uint64, numRecords int64, baseOffset int64, lastOffset uint64) {
	state, ok := ps.producerSeqs[producerID]
	if !ok {
		return
	}
	state.LastOffset = lastOffset
	state.LastBatch = &producerBatchMeta{
		FirstSequence: firstSeq,
		NumRecords:    numRecords,
		BaseOffset:    baseOffset,
		LastOffset:    lastOffset,
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
		if b.LastOffset > state.LastOffset {
			state.LastOffset = b.LastOffset
			// Batches replay in offset order, so the highest LastOffset is the
			// producer's most recent batch — the one a duplicate retry matches.
			state.LastBatch = &producerBatchMeta{
				FirstSequence: b.Sequence,
				NumRecords:    int64(b.MessageCount),
				BaseOffset:    int64(b.FirstOffset),
				LastOffset:    b.LastOffset,
			}
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

	highWaterMark := maxSize64 * 8
	pm.batcher = producer.NewBatcher(producer.BatcherConfig{
		MaxSize:       maxSize64,
		MaxAge:        maxAge,
		OnFlush:       pm.onFlushDispatch,
		HighWaterMark: highWaterMark,
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

	// Followers do not own a lease, so their requested epoch is zero. Preserve
	// the epoch recorded with a recovered active tail: it is the epoch the
	// follower must report to its new leader for a precise divergence check.
	// Resetting it to zero fences the entire unflushed tail on every restart.
	localEpoch := epoch
	if localEpoch == 0 && prevEpoch > 0 {
		localEpoch = prevEpoch
	}

	// 3. Write the effective local epoch to the sidecar file.
	if err := fsutil.AtomicWriteFile(epochFile, []byte(fmt.Sprintf("%d", localEpoch)), 0o644); err != nil {
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
		epoch:         localEpoch,
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
		return nil, fmt.Errorf("%w: topic %q not initialized", errKafkaUnknownTopicPartition, topic)
	}
	ps, ok := topicPartitions[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return nil, fmt.Errorf("%w: partition %d not found for topic %q", errKafkaUnknownTopicPartition, partitionID, topic)
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
		return nil, fmt.Errorf("%w: topic %q not initialized", errKafkaUnknownTopicPartition, topic)
	}
	ps, ok := topicPartitions[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return nil, fmt.Errorf("%w: partition %d not found for topic %q", errKafkaUnknownTopicPartition, partitionID, topic)
	}
	pm.mu.RUnlock()

	offsets, err := pm.appendBatchWithMetaToPS(ps, topic, partitionID, batch, idem)
	if errors.Is(err, idempotency.ErrDuplicateSequence) {
		if prior, ok := pm.duplicateBaseOffset(ps, batch.ProducerID, batch.Sequence, int64(len(batch.Messages))); ok {
			offsets = make([]uint64, len(batch.Messages))
			for i := range offsets {
				offsets[i] = uint64(prior) + uint64(i)
			}
			return offsets, nil
		}
	}
	return offsets, err
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

// duplicateBaseOffset returns the exact base offset of an already-appended
// idempotent batch so that an exact retried duplicate can be confirmed with
// success instead of an error, matching Kafka's idempotent produce semantics.
// It only confirms when the retried batch exactly matches the producer's most
// recently appended batch (same first sequence and record count). Anything else
// is an overlapping or non-identical request for an earlier sequence range and
// must not be acknowledged with a later batch's offsets.
func (pm *PartitionManager) duplicateBaseOffset(ps *partitionState, producerID, firstSeq uint64, numRecords int64) (int64, bool) {
	// Snapshot the batch metadata under the lock. A concurrent append can
	// replace LastBatch between releasing the read lock and reading its fields;
	// reading it without the lock is also a data race. EvictStaleProducerStates
	// may delete the map entry concurrently, so check ok before dereferencing
	// state to avoid a nil-pointer panic.
	ps.mu.RLock()
	state, ok := ps.producerSeqs[producerID]
	var lastBatch *producerBatchMeta
	if ok {
		lastBatch = state.LastBatch
	}
	ps.mu.RUnlock()
	if !ok || lastBatch == nil {
		return 0, false
	}
	if lastBatch.FirstSequence != firstSeq || lastBatch.NumRecords != numRecords {
		return 0, false
	}
	return lastBatch.BaseOffset, true
}

// AppendRawBatch writes a raw Kafka v2 RecordBatch to the active segment,
// patching offsets and leader epoch in place. This is the zero-copy produce
// path — no record-level decoding or re-serialization.
func (pm *PartitionManager) AppendRawBatch(ctx context.Context, topic string, partitionID int, batch []byte) (int64, error) {
	pm.mu.RLock()
	topicPartitions, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return 0, fmt.Errorf("%w: topic %q not initialized", errKafkaUnknownTopicPartition, topic)
	}
	ps, ok := topicPartitions[partitionID]
	if !ok {
		pm.mu.RUnlock()
		return 0, fmt.Errorf("%w: partition %d not found for topic %q", errKafkaUnknownTopicPartition, partitionID, topic)
	}
	pm.mu.RUnlock()

	baseOffset, err := pm.appendRawBatchToPS(ps, topic, partitionID, batch, nil, true)
	if errors.Is(err, idempotency.ErrDuplicateSequence) {
		if h, hErr := log.ReadRecordBatchHeader(batch); hErr == nil {
			numRecords := int64(h.LastOffsetDelta) + 1
			if prior, ok := pm.duplicateBaseOffset(ps, uint64(h.ProducerID), uint64(h.FirstSequence), numRecords); ok {
				return prior, nil
			}
		}
	}
	return baseOffset, err
}

func (pm *PartitionManager) appendRawBatchToPS(ps *partitionState, topic string, partitionID int, batch []byte, idem *IdempotencyOpts, enforceLeader bool) (int64, error) {
	h, err := log.ReadRecordBatchHeader(batch)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", errKafkaInvalidRecordBatch, err)
	}

	numRecords := int64(h.LastOffsetDelta) + 1

	ps.mu.Lock()

	if enforceLeader && !ps.isLeader {
		ps.mu.Unlock()
		return 0, fmt.Errorf("%w: partition %d", errKafkaNotLeader, partitionID)
	}

	if ps.activeSegment == nil {
		ps.mu.Unlock()
		return 0, fmt.Errorf("%w: active segment not initialized for %s/%d", errKafkaSegmentNotReady, topic, partitionID)
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
		return 0, fmt.Errorf("%w: %v", errKafkaInvalidRecordBatch, err)
	}
	if err := log.PatchRecordBatchLeaderEpoch(batch, int32(ps.epoch)); err != nil {
		if h.ProducerID >= 0 {
			ps.rollbackSeq(uint64(h.ProducerID), uint64(h.FirstSequence))
		}
		ps.mu.Unlock()
		return 0, fmt.Errorf("%w: %v", errKafkaInvalidRecordBatch, err)
	}

	// Write to active segment.
	if err := ps.activeSegment.Append(batch); err != nil {
		if h.ProducerID >= 0 {
			ps.rollbackSeq(uint64(h.ProducerID), uint64(h.FirstSequence))
		}
		ps.mu.Unlock()
		// A closed segment means the segment was retired (or a seal failed) —
		// retryable, not a fatal unknown error. Other IO failures are returned
		// as-is and mapped to UNKNOWN_SERVER_ERROR.
		if errors.Is(err, os.ErrClosed) {
			return 0, fmt.Errorf("%w: active segment append: %v", errKafkaSegmentNotReady, err)
		}
		return 0, fmt.Errorf("active segment append: %w", err)
	}

	// Update producer state AFTER successful append.
	if h.ProducerID >= 0 {
		lastOffset := uint64(baseOffset) + uint64(numRecords) - 1
		ps.recordAppendedBatch(uint64(h.ProducerID), uint64(h.FirstSequence), numRecords, baseOffset, lastOffset)
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
		if err := appendReplicatedRawBatchLocked(ps, batch); err != nil {
			return fmt.Errorf("AppendReplicatedRawBatches: %w", err)
		}
	}
	return nil
}

// AppendReplicatedRawBatch appends one RecordBatch received from the leader.
// The fetcher calls this directly while streaming the HTTP response, avoiding
// a one-element slice allocation for every replicated batch.
func (pm *PartitionManager) AppendReplicatedRawBatch(ctx context.Context, topic string, partitionID int, batch []byte) error {
	pm.mu.RLock()
	ps, ok := pm.partitions[topic][partitionID]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("partition %s/%d not found", topic, partitionID)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := appendReplicatedRawBatchLocked(ps, batch); err != nil {
		return fmt.Errorf("AppendReplicatedRawBatch: %w", err)
	}
	return nil
}

// AppendReplicatedBatchStream appends a single RecordBatch by streaming its
// body from r, avoiding materializing the full batch in memory. The 61-byte
// header has already been read and parsed by the caller; bodySize is the
// remaining bytes after the header.
func (pm *PartitionManager) AppendReplicatedBatchStream(topic string, partitionID int, hdr log.RecordBatchHeader, headerBytes []byte, body io.Reader, bodySize int64) error {
	pm.mu.RLock()
	ps, ok := pm.partitions[topic][partitionID]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("partition %s/%d not found", topic, partitionID)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.activeSegment == nil {
		return fmt.Errorf("no active segment")
	}
	if err := ps.activeSegment.AppendFromReader(hdr, headerBytes, body, bodySize); err != nil {
		return fmt.Errorf("AppendReplicatedBatchStream: %w", err)
	}
	end := uint64(hdr.LastOffset()) + 1
	if end > ps.nextOffset {
		ps.nextOffset = end
	}
	return nil
}

func appendReplicatedRawBatchLocked(ps *partitionState, batch []byte) error {
	h, err := log.ReadRecordBatchHeader(batch)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if ps.activeSegment == nil {
		return fmt.Errorf("no active segment")
	}
	if err := ps.activeSegment.Append(batch); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	end := uint64(h.LastOffset()) + 1
	if end > ps.nextOffset {
		ps.nextOffset = end
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
			// state.json is published with each sealed segment. It must not make
			// an S3-backed reader believe records are readable beyond the segment
			// objects currently present in the refreshed index.
			highWatermark := state.HighWatermark
			if indexedEnd := idx.NextOffset(); indexedEnd < highWatermark {
				highWatermark = indexedEnd
			}
			idx.SetHighWatermark(highWatermark)
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
	epochChanged := false
	if leaderEpoch != ps.epoch {
		ps.epoch = leaderEpoch
		epochChanged = true
	}
	// A leader can advertise a high watermark beyond the batches carried by
	// this fetch response. Never expose that remote position to local readers:
	// they may otherwise receive an empty response at an offset which this
	// follower has not replicated yet.
	localReadable := highWatermark
	if localReadable > ps.nextOffset {
		localReadable = ps.nextOffset
	}
	if localReadable > ps.followerHW {
		ps.followerHW = localReadable
	}
	if flushedOffset > ps.flushedOffset {
		ps.flushedOffset = flushedOffset
	}
	// Followers retain only the replicated tail which is not yet durable in the
	// local S3 index. A leader's flushed offset is not sufficient evidence: a
	// follower may observe the header before its index refresh sees the segment.
	// Never drop the local prefix until the object is locally readable.
	durablePrefix := ps.index != nil && ps.index.NextOffset() > ps.flushedOffset
	if !ps.isLeader && ps.activeSegment != nil && ps.flushedOffset > 0 && durablePrefix {
		compacted, changed, err := ps.activeSegment.CompactThrough(int64(ps.flushedOffset))
		if err != nil {
			slog.Warn("follower_active_segment_compaction_failed", "topic", topic, "partition", partitionID, "flushed_offset", ps.flushedOffset, "error", err)
		} else if changed {
			ps.activeSegment = compacted
			slog.Info("follower_active_segment_compacted", "topic", topic, "partition", partitionID, "flushed_offset", ps.flushedOffset, "active_base_offset", compacted.BaseOffset(), "active_size_bytes", compacted.Size())
		}
	}
	ps.mu.Unlock()

	if epochChanged {
		epochFile := filepath.Join(pm.localPartitionDir(topic, partitionID), "epoch")
		_ = fsutil.AtomicWriteFile(epochFile, []byte(fmt.Sprintf("%d", leaderEpoch)), 0o644)
	}
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
			ps.mu.RLock()
			cancel := ps.fetchCancel
			done := ps.fetchDone
			ps.mu.RUnlock()
			if cancel != nil {
				cancel()
				if done != nil {
					doneChans = append(doneChans, done)
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

// ReplicaBatchRange describes a contiguous byte range in the leader's active
// segment file that contains the batches to replicate. The caller can use
// File with io.NewSectionReader to sendfile the range directly to a socket,
// avoiding all user-space copies.
type ReplicaBatchRange struct {
	File       *os.File
	FileOffset int64
	Length     int64
	UpperBound int64
}

// ReadReplicaBatchRange returns the file, byte offset, and length of the
// contiguous batch range starting at startOffset, up to maxBytes. It replaces
// ReadReplicaRawBatches for the zero-copy replication path: no batch data is
// read into user space. Returns Length=0 when no data is available.
func (pm *PartitionManager) ReadReplicaBatchRange(topic string, pid int, startOffset int64, maxBytes int) (ReplicaBatchRange, error) {
	pm.mu.RLock()
	tp, ok := pm.partitions[topic]
	if !ok {
		pm.mu.RUnlock()
		return ReplicaBatchRange{}, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}
	ps, ok := tp[pid]
	pm.mu.RUnlock()
	if !ok {
		return ReplicaBatchRange{}, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, pid, topic)
	}

	ps.mu.RLock()
	upperBound := int64(ps.nextOffset)
	activeSeg := ps.activeSegment
	ps.mu.RUnlock()
	if activeSeg == nil || startOffset < activeSeg.BaseOffset() || startOffset >= upperBound {
		return ReplicaBatchRange{UpperBound: upperBound}, nil
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}

	var result ReplicaBatchRange
	activeSeg.WithOffsetIndex(func(idx []log.IndexEntry) {
		startIdx := sort.Search(len(idx), func(i int) bool {
			return idx[i].LastOffset >= startOffset
		})
		if startIdx >= len(idx) {
			return
		}
		startPos := idx[startIdx].Position
		endIdx := startIdx
		accumulated := 0
		for i := startIdx; i < len(idx); i++ {
			entry := idx[i]
			if entry.BaseOffset >= upperBound {
				break
			}
			if entry.BatchSize <= 0 || entry.Position < 0 {
				continue
			}
			if accumulated > 0 && accumulated+int(entry.BatchSize) > maxBytes {
				break
			}
			accumulated += int(entry.BatchSize)
			endIdx = i + 1
		}
		if endIdx <= startIdx {
			return
		}
		endEntry := idx[endIdx-1]
		result = ReplicaBatchRange{
			File:       activeSeg.File(),
			FileOffset: startPos,
			Length:     endEntry.Position + int64(endEntry.BatchSize) - startPos,
			UpperBound: upperBound,
		}
	})
	return result, nil
}

// SyncFollowerSealedPrefix refreshes a follower's index when the leader has
// sealed data before its active segment. Sealed data is shared through S3;
// copying it through the replication connection is unnecessary.
func (pm *PartitionManager) SyncFollowerSealedPrefix(ctx context.Context, topic string, pid int, activeBase uint64) uint64 {
	ps := pm.GetPartitionState(topic, pid)
	if ps == nil {
		return 0
	}
	ps.mu.RLock()
	indexedNext := uint64(0)
	if ps.index != nil {
		indexedNext = ps.index.NextOffset()
	}
	ps.mu.RUnlock()
	if indexedNext < activeBase {
		pm.RefreshIndex(ctx, topic, pid)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	// activeBase is the first offset in the leader's active tail. The S3
	// prefix may advance a follower only through a range that is both present
	// in its index and published as durable in state.json. A segment object can
	// contain an uncommitted tail from a previous leader, so its end offset is
	// not itself a replication checkpoint.
	if ps.index != nil {
		durableEnd := ps.index.HighWatermark()
		if durableEnd > activeBase {
			durableEnd = activeBase
		}
		if ps.nextOffset < durableEnd && ps.index.NextOffset() >= durableEnd {
			ps.nextOffset = durableEnd
		}
	}
	return ps.nextOffset
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
	index := ps.index.Clone()
	pendingFlush := ps.pendingFlush
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
			// A hole in the index must never serve a later segment's records:
			// that would silently relabel a distant offset range as the
			// requested one. Stop at the first missing segment and return only
			// the contiguous prefix.
			if currentOffset < int64(ref.BaseOffset) {
				break
			}

			for currentOffset <= int64(ref.EndOffset) && remaining > 0 {
				nextOffset, err := pm.appendSealedRawBatches(ctx, ref, currentOffset, upperBound, maxBytes, &out)
				if err != nil {
					return nil, upperBound, fmt.Errorf("read sealed segment %s: %w", ref.Key, err)
				}
				if nextOffset <= currentOffset {
					break
				}
				currentOffset = nextOffset
				remaining = maxBytes - len(out)
			}
		}
	}

	// A segment is sealed before it is published to S3. Keep it readable from
	// its local files while that asynchronous publish is in flight so followers
	// can still replicate the leader's tail.
	if pendingFlush != nil && remaining > 0 && currentOffset >= int64(pendingFlush.ref.BaseOffset) && currentOffset <= int64(pendingFlush.ref.EndOffset) {
		ps.mu.RLock()
		if ps.pendingFlush == pendingFlush {
			segData, err := os.ReadFile(pendingFlush.segmentPath)
			if err == nil {
				startPos := 0
				if sidecarData, sidecarErr := os.ReadFile(pendingFlush.sidecarPath); sidecarErr == nil {
					if entries, _, readErr := log.ReadSidecar(sidecarData); readErr == nil {
						if pos, ok := log.LookupSidecarPosition(entries, currentOffset); ok {
							startPos = int(pos)
						}
					}
				}
				rawBatches, readErr := log.ReadSegmentBatchesFromPosition(segData, startPos, uint64(currentOffset), 0)
				if readErr == nil {
					for _, batch := range rawBatches {
						hdr, headerErr := log.ReadRecordBatchHeader(batch)
						if headerErr != nil || hdr.FirstOffset >= upperBound {
							break
						}
						if len(out) > 0 && len(out)+len(batch) > maxBytes {
							break
						}
						out = append(out, batch...)
						remaining = maxBytes - len(out)
						currentOffset = hdr.LastOffset() + 1
					}
				}
			}
		}
		ps.mu.RUnlock()
	}

	// Phase 2: Active segment.
	// A page may stop within the sealed prefix. Do not append the active tail
	// unless the requested range reached its base offset: Kafka records must be
	// contiguous, never a sealed page followed by a distant active batch.
	if activeSeg != nil && remaining > 0 && currentOffset >= activeBase {
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

	return out, upperBound, nil
}

const maxSealedSegmentRangeReadBytes = 4 << 20

const (
	sealedSegmentRangeReadAttempts  = 5
	sealedSegmentRangeRetryInterval = 50 * time.Millisecond
)

// appendSealedRawBatches appends complete Kafka RecordBatches from one sealed
// segment. The sidecar lets us fetch only the requested contiguous range rather
// than loading the full (up to 64 MiB) object into memory.
func (pm *PartitionManager) appendSealedRawBatches(ctx context.Context, ref log.SegmentRef, startOffset, upperBound int64, maxBytes int, out *[]byte) (int64, error) {
	sidecarData, err := pm.readSealedSegmentSidecar(ctx, ref)
	if err != nil {
		return startOffset, err
	}
	entries, _, err := log.ReadSidecar(sidecarData)
	if err != nil {
		return startOffset, err
	}
	start := sort.Search(len(entries), func(i int) bool { return entries[i].LastOffset >= startOffset })
	if start == len(entries) {
		return int64(ref.EndOffset) + 1, nil
	}

	end := start
	rangeBytes := int64(0)
	for end < len(entries) {
		entry := entries[end]
		if entry.BaseOffset >= upperBound || entry.BatchSize <= 0 {
			break
		}
		batchBytes := int64(entry.BatchSize)
		if end > start && (rangeBytes+batchBytes > maxSealedSegmentRangeReadBytes || len(*out)+int(rangeBytes+batchBytes) > maxBytes) {
			break
		}
		rangeBytes += batchBytes
		end++
	}
	if end == start {
		return startOffset, nil
	}

	data, err := pm.readSealedSegmentRange(ctx, ref, entries[start].Position, rangeBytes)
	if err != nil {
		return startOffset, err
	}
	position := int64(0)
	nextOffset := startOffset
	for _, entry := range entries[start:end] {
		batchSize := int64(entry.BatchSize)
		if position+batchSize > int64(len(data)) {
			return startOffset, fmt.Errorf("short segment range: got %d bytes, need %d", len(data), position+batchSize)
		}
		batch := data[position : position+batchSize]
		position += batchSize
		hdr, err := log.ReadRecordBatchHeader(batch)
		if err != nil {
			return startOffset, err
		}
		if hdr.LastOffset() < startOffset {
			continue
		}
		if hdr.FirstOffset >= upperBound {
			break
		}
		if len(*out) > 0 && len(*out)+len(batch) > maxBytes {
			break
		}
		*out = append(*out, batch...)
		nextOffset = hdr.LastOffset() + 1
	}
	return nextOffset, nil
}

func (pm *PartitionManager) readSealedSegmentRange(ctx context.Context, ref log.SegmentRef, offset, length int64) ([]byte, error) {
	if pm.diskCache != nil {
		data, err := pm.diskCache.ReadRange(ref.Key, offset, length)
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, log.ErrCacheMiss) {
			return nil, err
		}
	}
	if pm.s3Client == nil {
		return nil, fmt.Errorf("no storage backend available for segment %s", ref.Key)
	}
	data, err := pm.getS3WithRetry(ctx, func() ([]byte, error) {
		return pm.s3Client.GetRange(ctx, ref.Key, offset, length)
	})
	if err != nil {
		return nil, fmt.Errorf("s3 range get %s: %w", ref.Key, err)
	}
	return data, nil
}

func (pm *PartitionManager) getS3WithRetry(ctx context.Context, get func() ([]byte, error)) ([]byte, error) {
	var err error
	for attempt := 0; attempt < sealedSegmentRangeReadAttempts; attempt++ {
		data, getErr := get()
		if getErr == nil {
			return data, nil
		}
		err = getErr
		if errors.Is(err, storage.ErrNotFound) || attempt == sealedSegmentRangeReadAttempts-1 {
			break
		}
		timer := time.NewTimer(sealedSegmentRangeRetryInterval << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, err
}

// readSealedSegmentData reads a full sealed segment for maintenance work. The
// normal consume path uses bounded range reads and never caches segment data.
func (pm *PartitionManager) readSealedSegmentData(ctx context.Context, ref log.SegmentRef) ([]byte, error) {
	if pm.s3Client == nil {
		return nil, fmt.Errorf("no storage backend available for segment %s", ref.Key)
	}
	data, err := pm.s3Client.Get(ctx, ref.Key)
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", ref.Key, err)
	}
	return data, nil
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
		data, err := pm.getS3WithRetry(ctx, func() ([]byte, error) {
			return pm.s3Client.Get(ctx, key)
		})
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

// onFlushActiveSegment seals the active segment then publishes it outside the
// producer path. A failed publish is retried using the same sealed files.
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
	task := ps.pendingFlush
	if task == nil {
		var err error
		task, err = pm.sealActiveSegmentLocked(ps, topic, partitionID)
		if err != nil {
			ps.mu.Unlock()
			return err
		}
		if task == nil {
			ps.mu.Unlock()
			return nil
		}
		ps.pendingFlush = task
	}
	ps.mu.Unlock()

	return pm.publishSealedSegment(ps, task)
}

// sealActiveSegmentLocked seals the current segment and records all immutable
// metadata required to retry publishing it. ps.mu must be held.
func (pm *PartitionManager) sealActiveSegmentLocked(ps *partitionState, topic string, partitionID int) (*sealedSegment, error) {
	oldSeg := ps.activeSegment
	if oldSeg == nil {
		return nil, nil
	}
	offsetIdx := oldSeg.OffsetIndex()
	if len(offsetIdx) == 0 {
		return nil, nil
	}

	// Open the replacement segment before sealing the old one. If this fails
	// the old segment is still open and writable, so concurrent produces are
	// unaffected and the flush can be retried.
	newSeg, err := log.OpenActiveSegment(oldSeg.Dir(), int64(ps.nextOffset))
	if err != nil {
		return nil, fmt.Errorf("open new active segment: %w", err)
	}

	segmentPath, sidecarPath, err := oldSeg.Seal()
	if err != nil {
		// The replacement was opened before the seal attempt; a failed seal
		// must not leak its file descriptor or leave an empty orphan .log
		// behind. Close and remove it so retried flushes don't accumulate
		// descriptors and empty files at the next-offset path.
		_ = newSeg.Close()
		_ = os.Remove(newSeg.Path())
		// Seal syncs and closes the segment file before writing the sidecar, so
		// on a mid-seal failure the file may already be closed. Reopen the old
		// segment from its log file so a concurrent produce never appends to a
		// closed file and the next flush attempt can re-seal it.
		if reopened, rerr := log.OpenActiveSegment(oldSeg.Dir(), oldSeg.BaseOffset()); rerr == nil {
			if rerr := reopened.Recover(); rerr == nil {
				ps.activeSegment = reopened
			} else {
				_ = reopened.Close()
			}
		}
		return nil, fmt.Errorf("seal active segment: %w", err)
	}
	ps.activeSegment = newSeg

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
	endOffset := offsetIdx[len(offsetIdx)-1].LastOffset
	baseOffset := oldSeg.BaseOffset()
	segKey := log.FormatSegmentKey(topic, partitionID, uint64(baseOffset), uint64(endOffset), ps.epoch)
	ref := log.SegmentRef{
		BaseOffset: uint64(baseOffset), EndOffset: uint64(endOffset),
		MinTimestamp: minTimestamp, MaxTimestamp: maxTimestamp, Epoch: ps.epoch,
		Key: segKey, OffsetIndexKey: log.SegmentOffsetIndexKey(segKey), MetaKey: log.SegmentMetadataKey(segKey), CreatedAt: time.Now(),
	}
	hw := ps.index.HighWatermark()
	if ps.replicaState != nil {
		hw = ps.replicaState.HighWatermark()
	}
	// This state is published with ref. Do not advertise records from a
	// later active or pending segment before their objects are durable in S3.
	if durableEnd := ref.EndOffset + 1; hw > durableEnd {
		hw = durableEnd
	}
	partState := log.PartitionState{HighWatermark: hw}
	if ps.epochHistory != nil {
		for _, e := range ps.epochHistory.Entries {
			partState.EpochHistory = append(partState.EpochHistory, log.EpochEntry{Epoch: e.Epoch, StartOffset: e.StartOffset})
		}
	}
	stateData, err := partState.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal state: %w", err)
	}
	var cpBuf bytes.Buffer
	for producerID, st := range ps.snapshotProducerSeqs() {
		line, err := json.Marshal(producerCheckpointEntry{ProducerID: producerID, NextSeq: st.NextSeq, LastOffset: st.LastOffset})
		if err != nil {
			return nil, fmt.Errorf("marshal producers checkpoint: %w", err)
		}
		cpBuf.Write(line)
		cpBuf.WriteByte('\n')
	}
	return &sealedSegment{topic: topic, partitionID: partitionID, segmentPath: segmentPath, sidecarPath: sidecarPath, ref: ref, highWatermark: hw, stateData: stateData, producerCheckpoint: cpBuf.Bytes()}, nil
}

func (pm *PartitionManager) publishSealedSegment(ps *partitionState, task *sealedSegment) error {
	segData, err := os.ReadFile(task.segmentPath)
	if err != nil {
		return fmt.Errorf("read sealed segment: %w", err)
	}
	sidecarData, err := os.ReadFile(task.sidecarPath)
	if err != nil {
		return fmt.Errorf("read sealed sidecar: %w", err)
	}
	metaData, err := log.BuildSegmentMetadata(task.ref, int(task.ref.EndOffset-task.ref.BaseOffset+1), int64(len(segData)), pm.segmentsCfg.Compression)
	if err != nil {
		return fmt.Errorf("build segment metadata: %w", err)
	}
	ctx := context.Background()
	if err := pm.s3Client.Put(ctx, task.ref.Key, segData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload sealed segment: %w", err)
	}
	if err := pm.s3Client.Put(ctx, task.ref.OffsetIndexKey, sidecarData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload sealed sidecar: %w", err)
	}
	if err := pm.s3Client.Put(ctx, task.ref.MetaKey, metaData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload segment metadata: %w", err)
	}
	if err := pm.s3Client.Put(ctx, log.StateKey(task.topic, task.partitionID), task.stateData, storage.PutOpts{}); err != nil {
		return fmt.Errorf("upload state.json: %w", err)
	}
	if len(task.producerCheckpoint) > 0 {
		if err := pm.s3Client.Put(ctx, fmt.Sprintf("%s/%d/producers.checkpoint", task.topic, task.partitionID), task.producerCheckpoint, storage.PutOpts{}); err != nil {
			return fmt.Errorf("upload producers checkpoint: %w", err)
		}
	}
	if pm.diskCache != nil {
		_ = pm.diskCache.Put(task.ref.Key, segData)
		_ = pm.diskCache.Put(task.ref.OffsetIndexKey, sidecarData)
		_ = pm.diskCache.Put(task.ref.MetaKey, metaData)
	}
	ps.mu.Lock()
	if ps.pendingFlush != task {
		ps.mu.Unlock()
		return fmt.Errorf("sealed segment changed while publishing %s", task.ref.Key)
	}
	ps.index.Add(task.ref)
	ps.index.SetHighWatermark(task.highWatermark)
	ps.flushedOffset = task.ref.EndOffset
	ps.pendingFlush = nil
	ps.mu.Unlock()
	_ = os.Remove(task.segmentPath)
	_ = os.Remove(task.sidecarPath)
	slog.Info("active_segment_flushed", "topic", task.topic, "partition", task.partitionID, "base_offset", task.ref.BaseOffset, "end_offset", task.ref.EndOffset, "size_bytes", len(segData))
	return nil
}
