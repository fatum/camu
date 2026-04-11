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

func (s *Server) getTopicDeletion(ctx context.Context, topic string) (topicDeletionRecord, error) {
	data, err := s.s3Client.Get(ctx, topicDeletionKey(topic))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return topicDeletionRecord{}, storage.ErrNotFound
		}
		return topicDeletionRecord{}, err
	}
	var rec topicDeletionRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return topicDeletionRecord{}, fmt.Errorf("unmarshal topic deletion %q: %w", topic, err)
	}
	return rec, nil
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
	_, err := s.getTopicDeletion(ctx, topic)
	return err == nil
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

func (s *Server) deleteTopicS3Data(ctx context.Context, topic string) error {
	keys, err := s.s3Client.List(ctx, topic+"/")
	if err != nil {
		return fmt.Errorf("list topic data: %w", err)
	}
	for _, key := range keys {
		if err := s.s3Client.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete topic object %q: %w", key, err)
		}
	}

	assignmentKey := fmt.Sprintf("_coordination/assignments/%s.json", topic)
	if err := s.s3Client.Delete(ctx, assignmentKey); err != nil {
		return fmt.Errorf("delete topic assignment: %w", err)
	}

	epochPrefix := fmt.Sprintf("_coordination/epochs/%s/", topic)
	keys, err = s.s3Client.List(ctx, epochPrefix)
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
		if err := s.deleteTopicS3Data(ctx, rec.Topic.Name); err != nil {
			slog.Warn("topic_delete_cleanup_s3_failed", "topic", rec.Topic.Name, "error", err)
			continue
		}
		if rec.Topic.StorageMode == meta.StorageModeDiskless && s.disklessMeta != nil {
			if err := s.disklessMeta.DeleteTopic(ctx, rec.Topic.Name); err != nil {
				slog.Warn("topic_delete_cleanup_diskless_meta_failed", "topic", rec.Topic.Name, "error", err)
				continue
			}
		}
		s.dropTopicRuntime(rec.Topic.Name)
		if err := s.s3Client.Delete(ctx, topicDeletionKey(rec.Topic.Name)); err != nil {
			slog.Warn("topic_delete_cleanup_marker_failed", "topic", rec.Topic.Name, "error", err)
			continue
		}
		slog.Info("topic_delete_completed", "topic", rec.Topic.Name)
	}
}
