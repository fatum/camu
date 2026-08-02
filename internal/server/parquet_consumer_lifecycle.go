package server

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"time"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

const parquetConsumerPollInterval = 200 * time.Millisecond

func parquetConsumerKey(topic string, partition int) string {
	return topic + "\x00" + strconv.Itoa(partition)
}

// ensureParquetConsumer starts (or replaces) the consumer owned by the local
// partition leader. The consumer uses the partition's local committed log;
// Camu assignment epochs provide ownership/fencing, so no Kafka group is used.
func (s *Server) ensureParquetConsumer(tc meta.TopicConfig, identity PartitionIdentity) {
	if !tc.ExportEnabled || tc.StorageMode == meta.StorageModeDiskless || tc.UncleanLeaderElection || identity.Role != PartitionRoleLeader {
		s.stopParquetConsumer(tc.Name, identity.Partition)
		return
	}
	key := parquetConsumerKey(tc.Name, identity.Partition)
	for {
		s.parquetConsumersMu.Lock()
		if s.parquetConsumers == nil {
			s.parquetConsumers = make(map[string]parquetConsumer)
		}
		current, ok := s.parquetConsumers[key]
		if ok {
			if !current.stopping && current.epoch == identity.LeaderEpoch && reflect.DeepEqual(current.topicConfig, tc) {
				s.parquetConsumersMu.Unlock()
				return
			}
			if current.stopping {
				done := current.done
				s.parquetConsumersMu.Unlock()
				if !waitParquetConsumer(done) {
					return
				}
				continue
			}
			current.stopping = true
			s.parquetConsumers[key] = current
			s.parquetConsumersMu.Unlock()
			current.cancel()
			if !waitParquetConsumer(current.done) {
				return
			}
			s.parquetConsumersMu.Lock()
			if existing, stillCurrent := s.parquetConsumers[key]; stillCurrent && existing.done == current.done {
				delete(s.parquetConsumers, key)
			}
			s.parquetConsumersMu.Unlock()
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		s.parquetConsumers[key] = parquetConsumer{topicConfig: tc, epoch: identity.LeaderEpoch, cancel: cancel, done: done}
		s.parquetConsumersMu.Unlock()

		go s.runParquetConsumer(ctx, done, tc, identity)
		slog.Info("parquet_export_consumer_started", "topic", tc.Name, "partition", identity.Partition, "epoch", identity.LeaderEpoch)
		return
	}
}

func (s *Server) runParquetConsumer(ctx context.Context, done chan struct{}, tc meta.TopicConfig, identity PartitionIdentity) {
	defer close(done)
	var cp pipeline.Checkpoint
	for {
		var err error
		cp, err = s.loadParquetCheckpoint(ctx, tc.Name, identity.Partition)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		slog.Warn("parquet_pipeline_checkpoint_load_failed", "topic", tc.Name, "partition", identity.Partition, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(parquetConsumerPollInterval):
		}
	}
	ticker := time.NewTicker(parquetConsumerPollInterval)
	defer ticker.Stop()
	for {
		s.runParquetExportPass(ctx, tc, identity, &cp)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) loadParquetCheckpoint(ctx context.Context, topic string, partition int) (pipeline.Checkpoint, error) {
	store := pipeline.NewCheckpointStore(s.s3Client, serverPipelineFence{server: s})
	cp, err := store.Load(ctx, parquetPipelineName, topic, partition)
	if errors.Is(err, storage.ErrNotFound) {
		return pipeline.Checkpoint{SourceTopic: topic, Partition: partition, Sink: parquetPipelineName, SinkVersion: parquetPipelineVersion}, nil
	}
	if err == nil && (cp.Sink != parquetPipelineName || cp.SinkVersion != parquetPipelineVersion) {
		return pipeline.Checkpoint{}, errors.New("parquet pipeline checkpoint has incompatible sink version")
	}
	return cp, err
}

func waitParquetConsumer(done chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

func (s *Server) stopParquetConsumer(topic string, partition int) {
	key := parquetConsumerKey(topic, partition)
	s.parquetConsumersMu.Lock()
	current, ok := s.parquetConsumers[key]
	if !ok || current.stopping {
		s.parquetConsumersMu.Unlock()
		return
	}
	current.stopping = true
	s.parquetConsumers[key] = current
	s.parquetConsumersMu.Unlock()
	current.cancel()
	if !waitParquetConsumer(current.done) {
		return
	}
	s.parquetConsumersMu.Lock()
	if existing, stillCurrent := s.parquetConsumers[key]; stillCurrent && existing.done == current.done {
		delete(s.parquetConsumers, key)
	}
	s.parquetConsumersMu.Unlock()
	slog.Info("parquet_export_consumer_stopped", "topic", topic, "partition", partition)
}

func (s *Server) stopAllParquetConsumers() {
	s.parquetConsumersMu.Lock()
	consumers := make(map[string]parquetConsumer, len(s.parquetConsumers))
	for key, current := range s.parquetConsumers {
		current.stopping = true
		s.parquetConsumers[key] = current
		consumers[key] = current
	}
	s.parquetConsumersMu.Unlock()
	for _, current := range consumers {
		current.cancel()
	}
	finished := make(map[string]struct{}, len(consumers))
	for key, current := range consumers {
		// Wait before closing shared S3 and partition-manager resources.
		<-current.done
		finished[key] = struct{}{}
	}
	s.parquetConsumersMu.Lock()
	for key := range finished {
		delete(s.parquetConsumers, key)
	}
	s.parquetConsumersMu.Unlock()
}
