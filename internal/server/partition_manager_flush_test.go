package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

// newTestPM creates a PartitionManager backed by an in-memory S3 client.
func newTestPM(t *testing.T) (*PartitionManager, *storage.S3Client) {
	t.Helper()

	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	require.NoError(t, err)

	cfg := &config.Config{}
	cfg.Cache.Directory = filepath.Join(t.TempDir(), "cache")
	cfg.Segments.MaxSize = 8 * 1024 * 1024

	pm, err := NewPartitionManager(cfg, s3Client)
	require.NoError(t, err)
	return pm, s3Client
}

func TestOnFlushActiveSegment(t *testing.T) {
	pm, s3Client := newTestPM(t)

	ctx := context.Background()
	topic := "test-topic"
	pid := 0

	// Create and initialise the partition.
	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
	}
	require.NoError(t, pm.InitTopic(ctx, tc, map[int]uint64{}))

	// Wire an active segment onto the partition state.
	ps := pm.GetPartitionState(topic, pid)
	require.NotNil(t, ps)

	segDir := filepath.Join(t.TempDir(), "segments", topic, fmt.Sprintf("%d", pid))
	seg, err := log.OpenActiveSegment(segDir, 0)
	require.NoError(t, err)

	ps.mu.Lock()
	ps.activeSegment = seg
	ps.isLeader = true
	ps.mu.Unlock()

	// Register a global ID for the batcher so notifyBatcher works.
	ps.mu.Lock()
	ps.globalID = pm.getGlobalID(topic, pid)
	ps.globalIDSet = true
	ps.mu.Unlock()

	// Append two raw RecordBatches via AppendRawBatch.
	batch1 := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Now().UnixMilli()},
	})
	batch2 := log.EncodeRecordBatch(0, []log.Message{
		{Key: []byte("k2"), Value: []byte("v2"), Timestamp: time.Now().UnixMilli()},
		{Key: []byte("k3"), Value: []byte("v3"), Timestamp: time.Now().UnixMilli()},
	})

	_, err = pm.AppendRawBatch(ctx, topic, pid, batch1)
	require.NoError(t, err)
	_, err = pm.AppendRawBatch(ctx, topic, pid, batch2)
	require.NoError(t, err)

	// Flush the active segment directly.
	require.NoError(t, pm.onFlushActiveSegment(topic, pid))

	// --- Verify: segment + sidecar uploaded to S3 ---
	segKey := log.FormatSegmentKey(topic, pid, 0, 2, 0)
	sidecarKey := log.SegmentOffsetIndexKey(segKey)
	metaKey := log.SegmentMetadataKey(segKey)

	segData, err := s3Client.Get(ctx, segKey)
	require.NoError(t, err, "sealed segment must be in S3")
	require.NotEmpty(t, segData)

	sidecarData, err := s3Client.Get(ctx, sidecarKey)
	require.NoError(t, err, "sidecar must be in S3")
	require.NotEmpty(t, sidecarData)

	metaData, err := s3Client.Get(ctx, metaKey)
	require.NoError(t, err, "segment metadata must be in S3")
	require.NotEmpty(t, metaData)

	// --- Verify: new active segment opened ---
	ps.mu.RLock()
	newSeg := ps.activeSegment
	ps.mu.RUnlock()
	require.NotNil(t, newSeg, "new active segment must be set")
	require.Equal(t, int64(3), newSeg.BaseOffset(), "new segment base offset must equal nextOffset (3 records)")

	// --- Verify: partition index updated with a SegmentRef ---
	ps.mu.RLock()
	refs := ps.index.SegmentsFrom(0, 100)
	ps.mu.RUnlock()
	require.Len(t, refs, 1, "index must contain one segment ref")
	ref := refs[0]
	require.Equal(t, uint64(0), ref.BaseOffset)
	require.Equal(t, uint64(2), ref.EndOffset) // offsets 0, 1, 2
	require.Equal(t, segKey, ref.Key)
	require.Equal(t, sidecarKey, ref.OffsetIndexKey)
}
