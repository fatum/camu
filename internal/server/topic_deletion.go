package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

const topicDeletionPrefix = "_coordination/topic_deletions/"

type topicDeletionRecord struct {
	Topic     meta.TopicConfig `json:"topic"`
	StartedAt time.Time        `json:"started_at"`
}

func topicDeletionKey(topic string) string {
	return topicDeletionPrefix + topic + ".json"
}

func (s *Server) getTopicDeletion(ctx context.Context, topic string) error {
	data, err := s.s3Client.Get(ctx, topicDeletionKey(topic))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return storage.ErrNotFound
		}
		return err
	}
	var rec topicDeletionRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return fmt.Errorf("unmarshal topic deletion %q: %w", topic, err)
	}
	return nil
}

func (s *Server) putTopicDeletion(ctx context.Context, rec topicDeletionRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal topic deletion %q: %w", rec.Topic.Name, err)
	}
	if err := s.s3Client.Put(ctx, topicDeletionKey(rec.Topic.Name), data, storage.PutOpts{
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("put topic deletion %q: %w", rec.Topic.Name, err)
	}
	return nil
}

func (s *Server) listTopicDeletions(ctx context.Context) ([]topicDeletionRecord, error) {
	keys, err := s.s3Client.List(ctx, topicDeletionPrefix)
	if err != nil {
		return nil, err
	}
	recs := make([]topicDeletionRecord, 0, len(keys))
	for _, key := range keys {
		data, err := s.s3Client.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, err
		}
		var rec topicDeletionRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, fmt.Errorf("unmarshal topic deletion %q: %w", key, err)
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

func (s *Server) topicDeletionPending(ctx context.Context, topic string) bool {
	return s.getTopicDeletion(ctx, topic) == nil
}

func (s *Server) enqueueTopicDeletion(ctx context.Context, tc meta.TopicConfig) error {
	if err := s.putTopicDeletion(ctx, topicDeletionRecord{
		Topic:     tc,
		StartedAt: time.Now(),
	}); err != nil {
		return err
	}
	if err := s.topicStore.Delete(ctx, tc.Name); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	s.dropTopicRuntime(tc.Name)
	return nil
}

func (s *Server) dropTopicRuntime(topic string) {
	s.assignmentsMu.Lock()
	delete(s.myPartitions, topic)
	delete(s.disklessTopics, topic)
	s.assignmentsMu.Unlock()
	s.partitionManager.RemoveTopic(topic)
}

// topicDeleteBatchSize bounds one batch of key deletions (S3 DeleteObjects
// accepts up to 1000 objects per call).
const topicDeleteBatchSize = 1000

func (s *Server) deleteTopicS3Data(ctx context.Context, topic string) error {
	// Stream the topic's keys and delete them in batches, so a huge topic never
	// holds the full key list in memory and issues far fewer Delete calls.
	var batch []string
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.s3Client.DeleteMany(ctx, batch); err != nil {
			return fmt.Errorf("delete topic data: %w", err)
		}
		batch = batch[:0]
		return nil
	}
	if err := s.s3Client.ListEach(ctx, topic+"/", func(key string) error {
		batch = append(batch, key)
		if len(batch) >= topicDeleteBatchSize {
			return flush()
		}
		return nil
	}); err != nil {
		return fmt.Errorf("list topic data: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}

	assignmentKey := fmt.Sprintf("_coordination/assignments/%s.json", topic)
	if err := s.s3Client.Delete(ctx, assignmentKey); err != nil {
		return fmt.Errorf("delete topic assignment: %w", err)
	}

	epochPrefix := fmt.Sprintf("_coordination/epochs/%s/", topic)
	keys, err := s.s3Client.List(ctx, epochPrefix)
	if err != nil {
		return fmt.Errorf("list topic epochs: %w", err)
	}
	for _, key := range keys {
		if err := s.s3Client.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete topic epoch %q: %w", key, err)
		}
	}
	return nil
}

func (s *Server) gcPendingTopicDeletions(ctx context.Context) {
	recs, err := s.listTopicDeletions(ctx)
	if err != nil {
		slog.Warn("coordinationGC: list topic deletions", "error", err)
		return
	}

	for _, rec := range recs {
		if err := s.processTopicDeletion(ctx, rec); err != nil {
			slog.Warn("topic_delete_cleanup_failed", "topic", rec.Topic.Name, "error", err)
		}
	}
}

// processTopicDeletion performs the full cleanup for one pending topic deletion
// and removes its marker. It is idempotent and crash-safe: any step that fails
// returns the error and leaves the marker in place so the next pass (or a
// restart) resumes from the marker.
func (s *Server) processTopicDeletion(ctx context.Context, rec topicDeletionRecord) error {
	// Remove the registry entry first: enqueueTopicDeletion normally deletes
	// it, but a crash between writing the marker and that delete would
	// otherwise leave a registered topic whose data this GC is erasing.
	if err := s.topicStore.Delete(ctx, rec.Topic.Name); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("delete topic registry entry: %w", err)
	}
	if err := s.icebergTableStoreFor().DeleteTable(ctx, rec.Topic.Name); err != nil {
		return fmt.Errorf("clean iceberg table: %w", err)
	}
	if err := s.schemaRegistry.DeleteTopicSchemas(ctx, rec.Topic.Name); err != nil {
		return fmt.Errorf("delete schema registry: %w", err)
	}
	if err := s.deleteTopicS3Data(ctx, rec.Topic.Name); err != nil {
		return fmt.Errorf("delete topic s3 data: %w", err)
	}
	if rec.Topic.StorageMode == meta.StorageModeDiskless && s.disklessMeta != nil {
		if err := s.disklessMeta.DeleteTopic(ctx, rec.Topic.Name); err != nil {
			return fmt.Errorf("delete diskless metadata: %w", err)
		}
	}
	s.dropTopicRuntime(rec.Topic.Name)
	if err := s.s3Client.Delete(ctx, topicDeletionKey(rec.Topic.Name)); err != nil {
		return fmt.Errorf("delete topic deletion marker: %w", err)
	}
	slog.Info("topic_delete_completed", "topic", rec.Topic.Name)
	return nil
}

// enqueueTopicDeletions hands every pending deletion marker to the bounded
// topic-deletion worker pool. It returns immediately so the leader's GC tick
// never blocks on a long cleanup; a marker whose cleanup is still in flight is
// skipped (the in-flight worker removes it when done), and any other failure
// keeps the marker so a later tick retries it.
func (s *Server) enqueueTopicDeletions(ctx context.Context) {
	recs, err := s.listTopicDeletions(ctx)
	if err != nil {
		slog.Warn("coordinationGC: list topic deletions", "error", err)
		return
	}
	for _, rec := range recs {
		s.spawnTopicDeletion(rec)
	}
}

// spawnTopicDeletion enqueues one topic's cleanup unless it is already in
// flight. A full queue is dropped so the caller (the leader GC tick) is never
// blocked; the marker persists and a later tick retries.
func (s *Server) spawnTopicDeletion(rec topicDeletionRecord) {
	s.topicDeletionMu.Lock()
	if s.topicDeletionCh == nil {
		s.topicDeletionMu.Unlock()
		slog.Warn("topic_delete_spawn_without_workers", "topic", rec.Topic.Name)
		return
	}
	if _, inFlight := s.topicDeletionInflight[rec.Topic.Name]; inFlight {
		s.topicDeletionMu.Unlock()
		return
	}
	s.topicDeletionInflight[rec.Topic.Name] = struct{}{}
	s.topicDeletionMu.Unlock()

	select {
	case s.topicDeletionCh <- rec:
	default:
		s.topicDeletionMu.Lock()
		delete(s.topicDeletionInflight, rec.Topic.Name)
		s.topicDeletionMu.Unlock()
	}
}

func (s *Server) topicDeletionWorker() {
	for rec := range s.topicDeletionCh {
		if err := s.processTopicDeletion(s.topicDeletionCtx, rec); err != nil {
			if s.topicDeletionCtx.Err() != nil {
				slog.Warn("topic_delete_worker_aborted", "topic", rec.Topic.Name, "error", err)
			} else {
				slog.Warn("topic_delete_worker_failed", "topic", rec.Topic.Name, "error", err)
			}
		}
		s.topicDeletionMu.Lock()
		delete(s.topicDeletionInflight, rec.Topic.Name)
		s.topicDeletionMu.Unlock()
	}
}

const (
	// topicDeletionWorkerCount bounds concurrent topic cleanups.
	topicDeletionWorkerCount = 2
	// topicDeletionQueueDepth bounds how many pending cleanups wait for a worker.
	topicDeletionQueueDepth = 8
)

// startTopicDeletionWorkers starts the bounded async cleanup workers. A long
// topic cleanup (streaming + batched deletes) then runs off the leader GC tick,
// which only enqueues markers.
func (s *Server) startTopicDeletionWorkers() {
	s.topicDeletionMu.Lock()
	if s.topicDeletionCh != nil {
		s.topicDeletionMu.Unlock()
		return
	}
	s.topicDeletionCh = make(chan topicDeletionRecord, topicDeletionQueueDepth)
	s.topicDeletionInflight = make(map[string]struct{})
	s.topicDeletionCtx, s.topicDeletionCancel = context.WithCancel(context.Background())
	s.topicDeletionMu.Unlock()
	for i := 0; i < topicDeletionWorkerCount; i++ {
		s.topicDeletionWG.Add(1)
		go func() {
			defer s.topicDeletionWG.Done()
			s.topicDeletionWorker()
		}()
	}
}

// stopTopicDeletionWorkers cancels in-flight cleanups (they abort at the next
// page boundary; the marker persists and a later pass resumes) and waits for
// the workers to drain the queue.
func (s *Server) stopTopicDeletionWorkers() {
	s.topicDeletionMu.Lock()
	ch := s.topicDeletionCh
	s.topicDeletionMu.Unlock()
	if s.topicDeletionCancel != nil {
		s.topicDeletionCancel()
	}
	if ch != nil {
		close(ch)
	}
	s.topicDeletionWG.Wait()
}
