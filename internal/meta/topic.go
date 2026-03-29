package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/maksim/camu/internal/storage"
)

const topicPrefix = "_meta/topics/"

// topicConfigJSON is the on-disk representation of a TopicConfig.
// Retention is stored as a nanosecond integer so it round-trips correctly.
type topicConfigJSON struct {
	Name                  string    `json:"name"`
	Partitions            int       `json:"partitions"`
	RetentionNs           int64     `json:"retention_ns"`
	CreatedAt             time.Time `json:"created_at"`
	ReplicationFactor     int       `json:"replication_factor"`
	MinInsyncReplicas     int       `json:"min_insync_replicas"`
	UncleanLeaderElection bool      `json:"unclean_leader_election"`
}

// TopicConfig holds the configuration for a single topic.
type TopicConfig struct {
	Name                  string
	Partitions            int
	Retention             time.Duration
	CreatedAt             time.Time
	ReplicationFactor     int
	MinInsyncReplicas     int
	UncleanLeaderElection bool
}

func (tc TopicConfig) toJSON() topicConfigJSON {
	return topicConfigJSON{
		Name:                  tc.Name,
		Partitions:            tc.Partitions,
		RetentionNs:           int64(tc.Retention),
		CreatedAt:             tc.CreatedAt,
		ReplicationFactor:     tc.ReplicationFactor,
		MinInsyncReplicas:     tc.MinInsyncReplicas,
		UncleanLeaderElection: tc.UncleanLeaderElection,
	}
}

func fromJSON(j topicConfigJSON) TopicConfig {
	cfg := TopicConfig{
		Name:                  j.Name,
		Partitions:            j.Partitions,
		Retention:             time.Duration(j.RetentionNs),
		CreatedAt:             j.CreatedAt,
		ReplicationFactor:     j.ReplicationFactor,
		MinInsyncReplicas:     j.MinInsyncReplicas,
		UncleanLeaderElection: j.UncleanLeaderElection,
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 1
	}
	if cfg.MinInsyncReplicas == 0 {
		cfg.MinInsyncReplicas = 1
	}
	return cfg
}

// TopicStore manages topic metadata stored in S3.
type TopicStore struct {
	s3Client *storage.S3Client
	cache    sync.Map // name -> TopicConfig
}

// NewTopicStore creates a new TopicStore backed by the given S3 client.
func NewTopicStore(s3 *storage.S3Client) *TopicStore {
	return &TopicStore{s3Client: s3}
}

func topicKey(name string) string {
	return topicPrefix + name + ".json"
}

// Create stores a new topic configuration. Returns an error if the topic already exists.
func (ts *TopicStore) Create(ctx context.Context, cfg TopicConfig) error {
	_, err := ts.Get(ctx, cfg.Name)
	if err == nil {
		return fmt.Errorf("topic %q already exists", cfg.Name)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("Create: checking existence of %q: %w", cfg.Name, err)
	}

	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 1
	}
	if cfg.MinInsyncReplicas == 0 {
		cfg.MinInsyncReplicas = 1
	}

	data, err := json.Marshal(cfg.toJSON())
	if err != nil {
		return fmt.Errorf("Create: marshal %q: %w", cfg.Name, err)
	}

	if err := ts.s3Client.Put(ctx, topicKey(cfg.Name), data, storage.PutOpts{
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("Create: put %q: %w", cfg.Name, err)
	}
	ts.cache.Store(cfg.Name, cfg)
	return nil
}

// Get retrieves a topic configuration by name. Returns a wrapped storage.ErrNotFound if missing.
func (ts *TopicStore) Get(ctx context.Context, name string) (TopicConfig, error) {
	// Check cache first.
	if v, ok := ts.cache.Load(name); ok {
		return v.(TopicConfig), nil
	}

	// Cache miss — fetch from S3.
	data, err := ts.s3Client.Get(ctx, topicKey(name))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return TopicConfig{}, fmt.Errorf("topic %q: %w", name, storage.ErrNotFound)
		}
		return TopicConfig{}, fmt.Errorf("Get %q: %w", name, err)
	}

	var j topicConfigJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return TopicConfig{}, fmt.Errorf("Get %q: unmarshal: %w", name, err)
	}
	cfg := fromJSON(j)
	ts.cache.Store(name, cfg)
	return cfg, nil
}

// List returns all topic configurations stored in S3.
func (ts *TopicStore) List(ctx context.Context) ([]TopicConfig, error) {
	keys, err := ts.s3Client.List(ctx, topicPrefix)
	if err != nil {
		return nil, fmt.Errorf("List: list prefix: %w", err)
	}

	topics := make([]TopicConfig, 0, len(keys))
	for _, key := range keys {
		data, err := ts.s3Client.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("List: get %q: %w", key, err)
		}
		var j topicConfigJSON
		if err := json.Unmarshal(data, &j); err != nil {
			return nil, fmt.Errorf("List: unmarshal %q: %w", key, err)
		}
		topics = append(topics, fromJSON(j))
	}

	// Sync cache: replace with fresh S3 state. This evicts topics deleted
	// by other instances and populates topics created by other instances.
	live := make(map[string]struct{}, len(topics))
	for _, tc := range topics {
		ts.cache.Store(tc.Name, tc)
		live[tc.Name] = struct{}{}
	}
	ts.cache.Range(func(key, _ any) bool {
		if _, ok := live[key.(string)]; !ok {
			ts.cache.Delete(key)
		}
		return true
	})

	return topics, nil
}

// Delete removes a topic configuration from S3.
func (ts *TopicStore) Delete(ctx context.Context, name string) error {
	if err := ts.s3Client.Delete(ctx, topicKey(name)); err != nil {
		return fmt.Errorf("Delete %q: %w", name, err)
	}
	ts.cache.Delete(name)
	return nil
}
