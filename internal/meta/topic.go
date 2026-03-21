package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/storage"
)

const topicPrefix = "_meta/topics/"

// topicConfigJSON is the on-disk representation of a TopicConfig.
// Retention is stored as a nanosecond integer so it round-trips correctly.
type topicConfigJSON struct {
	Name         string    `json:"name"`
	Partitions   int       `json:"partitions"`
	RetentionNs  int64     `json:"retention_ns"`
	CreatedAt    time.Time `json:"created_at"`
}

// TopicConfig holds the configuration for a single topic.
type TopicConfig struct {
	Name       string
	Partitions int
	Retention  time.Duration
	CreatedAt  time.Time
}

func (tc TopicConfig) toJSON() topicConfigJSON {
	return topicConfigJSON{
		Name:        tc.Name,
		Partitions:  tc.Partitions,
		RetentionNs: int64(tc.Retention),
		CreatedAt:   tc.CreatedAt,
	}
}

func fromJSON(j topicConfigJSON) TopicConfig {
	return TopicConfig{
		Name:       j.Name,
		Partitions: j.Partitions,
		Retention:  time.Duration(j.RetentionNs),
		CreatedAt:  j.CreatedAt,
	}
}

// TopicStore manages topic metadata stored in S3.
type TopicStore struct {
	s3Client *storage.S3Client
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

	data, err := json.Marshal(cfg.toJSON())
	if err != nil {
		return fmt.Errorf("Create: marshal %q: %w", cfg.Name, err)
	}

	if err := ts.s3Client.Put(ctx, topicKey(cfg.Name), data, storage.PutOpts{
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("Create: put %q: %w", cfg.Name, err)
	}
	return nil
}

// Get retrieves a topic configuration by name. Returns a wrapped storage.ErrNotFound if missing.
func (ts *TopicStore) Get(ctx context.Context, name string) (TopicConfig, error) {
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
	return fromJSON(j), nil
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
	return topics, nil
}

// Delete removes a topic configuration from S3.
func (ts *TopicStore) Delete(ctx context.Context, name string) error {
	if err := ts.s3Client.Delete(ctx, topicKey(name)); err != nil {
		return fmt.Errorf("Delete %q: %w", name, err)
	}
	return nil
}
