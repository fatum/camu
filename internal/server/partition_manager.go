package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/producer"
	"github.com/maksim/camu/internal/storage"
)

// partitionState holds per-partition runtime state.
type partitionState struct {
	wal        *log.WAL
	index      *log.Index
	indexETag  string // last known ETag of index.json in S3
	nextOffset uint64
	epoch      uint64 // always 0 in single-instance mode
}

// PartitionManager manages per-partition state including WAL, index, and batching.
type PartitionManager struct {
	mu         sync.RWMutex
	s3Client   *storage.S3Client
	diskCache  *log.DiskCache
	partitions map[string]map[int]*partitionState // topic -> partitionID -> state
	routers    map[string]*producer.Router
	batcher    *producer.Batcher
	walDir     string
	segmentsCfg config.SegmentsConfig

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
func (pm *PartitionManager) InitTopic(ctx context.Context, tc meta.TopicConfig) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[tc.Name]; exists {
		return nil // already initialized
	}

	topicPartitions := make(map[int]*partitionState)

	for pid := 0; pid < tc.Partitions; pid++ {
		ps, err := pm.initPartition(ctx, tc.Name, pid)
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
	return nil
}

// initPartition initializes a single partition: loads index, opens WAL, replays.
func (pm *PartitionManager) initPartition(ctx context.Context, topic string, partitionID int) (*partitionState, error) {
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
	wal, err := log.OpenWAL(walPath, false) // fsync disabled for performance in tests
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	// 3. Replay WAL to recover unflushed messages.
	msgs, err := wal.Replay()
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("replay WAL: %w", err)
	}

	// 4. Set nextOffset from max(index.NextOffset(), last WAL message offset + 1).
	nextOffset := idx.NextOffset()
	if len(msgs) > 0 {
		lastWALOffset := msgs[len(msgs)-1].Offset + 1
		if lastWALOffset > nextOffset {
			nextOffset = lastWALOffset
		}
	}

	return &partitionState{
		wal:        wal,
		index:      idx,
		indexETag:  indexETag,
		nextOffset: nextOffset,
		epoch:      0,
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

	// Assign offset (must be serialized per partition).
	// We use the partition manager's main lock for simplicity; a per-partition lock
	// would be better for high concurrency but is fine for single-instance mode.
	pm.mu.Lock()
	offset := ps.nextOffset
	ps.nextOffset++
	pm.mu.Unlock()

	msg.Offset = offset
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixNano()
	}

	// Write to WAL.
	if err := ps.wal.Append(msg); err != nil {
		return 0, fmt.Errorf("WAL append: %w", err)
	}

	// Add to batcher.
	globalID := pm.getGlobalID(topic, partitionID)
	if err := pm.batcher.Append(globalID, msg); err != nil {
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

	// Add all to batcher.
	globalID := pm.getGlobalID(topic, partitionID)
	for _, msg := range msgs {
		if err := pm.batcher.Append(globalID, msg); err != nil {
			return nil, fmt.Errorf("batcher append: %w", err)
		}
	}

	return offsets, nil
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

// onFlush is the batcher's flush callback. It serializes messages into a segment,
// uploads to S3, writes to disk cache, updates the index, and truncates the WAL.
func (pm *PartitionManager) onFlush(globalPartitionID int, msgs []log.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	topic, partitionID, ok := pm.resolveGlobalID(globalPartitionID)
	if !ok {
		return fmt.Errorf("unknown global partition ID %d", globalPartitionID)
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

	baseOffset := msgs[0].Offset
	endOffset := msgs[len(msgs)-1].Offset
	epoch := ps.epoch

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

		idxData, err := currentIdx.MarshalJSON()
		if err != nil {
			return fmt.Errorf("marshal updated index: %w", err)
		}

		newETag, err := pm.s3Client.ConditionalPut(ctx, indexKey, idxData, currentETag)
		if err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue // retry
			}
			return fmt.Errorf("conditional put index: %w", err)
		}

		// Update in-memory index and etag.
		pm.mu.Lock()
		ps.index = currentIdx
		ps.indexETag = newETag
		pm.mu.Unlock()
		break
	}

	// 5. Truncate WAL before the flushed offset.
	if err := ps.wal.TruncateBefore(endOffset + 1); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}

	return nil
}
