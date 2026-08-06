package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maksim/camu/internal/storage"
)

const topicPrefix = "_meta/topics/"

const (
	StorageModeClassic  = "classic"
	StorageModeDiskless = "diskless"
)

// topicConfigJSON is the on-disk representation of a TopicConfig.
// Retention is stored as a nanosecond integer so it round-trips correctly.
type topicConfigJSON struct {
	Name                  string       `json:"name"`
	Partitions            int          `json:"partitions"`
	RetentionNs           int64        `json:"retention_ns"`
	CreatedAt             time.Time    `json:"created_at"`
	ReplicationFactor     int          `json:"replication_factor"`
	MinInsyncReplicas     int          `json:"min_insync_replicas"`
	UncleanLeaderElection bool         `json:"unclean_leader_election"`
	ExportEnabled         bool         `json:"export_enabled"`
	StorageMode           string       `json:"storage_mode,omitempty"`
	Schema                *TopicSchema `json:"schema,omitempty"`
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
	ExportEnabled         bool
	StorageMode           string
	Schema                *TopicSchema
}

type TopicSchema struct {
	Encoding        string        `json:"encoding"`
	Version         int           `json:"version,omitempty"`
	Fields          []SchemaField `json:"fields"`
	DeadLetterTopic string        `json:"dead_letter_topic,omitempty"`
}

type SchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Path     string `json:"path"`
	Nullable bool   `json:"nullable,omitempty"`
}

// CloneSchema returns a deep copy of the schema so callers can mutate it
// without sharing state with a cached TopicConfig (which holds a *TopicSchema).
func CloneSchema(s *TopicSchema) *TopicSchema {
	if s == nil {
		return nil
	}
	c := *s
	c.Fields = append([]SchemaField(nil), s.Fields...)
	return &c
}

func (s *TopicSchema) Validate() error {
	if s == nil {
		return nil
	}
	switch s.Encoding {
	case "json", "avro", "protobuf":
	default:
		return fmt.Errorf("schema encoding must be json, avro, or protobuf")
	}
	if len(s.Fields) == 0 {
		return fmt.Errorf("schema fields are required")
	}
	seen := map[string]bool{}
	fixed := map[string]bool{"record_offset": true, "record_timestamp": true, "key": true, "value": true, "headers": true, "dt": true, "hour": true}
	for _, f := range s.Fields {
		if f.Name == "" || seen[strings.ToLower(f.Name)] || fixed[strings.ToLower(f.Name)] {
			return fmt.Errorf("schema field names must be unique and non-empty")
		}
		seen[strings.ToLower(f.Name)] = true
		switch f.Type {
		case "string", "int64", "float64", "bool", "timestamp":
		default:
			return fmt.Errorf("unsupported schema field type %q", f.Type)
		}
		if len(f.Path) < 3 || f.Path[:2] != "$." || strings.Contains(f.Path, "[") || strings.Contains(f.Path, "'") || strings.HasSuffix(f.Path, ".") {
			return fmt.Errorf("schema field %q path must start with $.", f.Name)
		}
	}
	return nil
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
		ExportEnabled:         tc.ExportEnabled,
		StorageMode:           tc.StorageMode,
		Schema:                tc.Schema,
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
		ExportEnabled:         j.ExportEnabled,
		StorageMode:           j.StorageMode,
		Schema:                j.Schema,
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
// The write is create-if-not-exists, so a concurrent Create of the same topic
// never silently overwrites the first.
func (ts *TopicStore) Create(ctx context.Context, cfg TopicConfig) error {
	if err := cfg.Schema.Validate(); err != nil {
		return fmt.Errorf("Create: invalid schema: %w", err)
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

	if _, err := ts.s3Client.ConditionalPut(ctx, topicKey(cfg.Name), data, ""); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return fmt.Errorf("topic %q already exists", cfg.Name)
		}
		return fmt.Errorf("Create: put %q: %w", cfg.Name, err)
	}
	ts.cache.Store(cfg.Name, withClonedSchema(cfg))
	return nil
}

// putConfig writes an existing topic's configuration atomically: it reads the
// current object's etag and writes with a conditional put, so a concurrent
// update never silently overwrites a newer configuration. The schema in the
// cache is stored as a private copy.
func (ts *TopicStore) putConfig(ctx context.Context, cfg TopicConfig) error {
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 1
	}
	if cfg.MinInsyncReplicas == 0 {
		cfg.MinInsyncReplicas = 1
	}
	data, err := json.Marshal(cfg.toJSON())
	if err != nil {
		return fmt.Errorf("marshal %q: %w", cfg.Name, err)
	}
	_, eTag, err := ts.s3Client.GetWithETag(ctx, topicKey(cfg.Name))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("topic %q does not exist", cfg.Name)
		}
		return fmt.Errorf("read %q: %w", cfg.Name, err)
	}
	if _, err := ts.s3Client.ConditionalPut(ctx, topicKey(cfg.Name), data, eTag); err != nil {
		return fmt.Errorf("put %q: %w", cfg.Name, err)
	}
	ts.cache.Store(cfg.Name, withClonedSchema(cfg))
	return nil
}

// withClonedSchema returns cfg with a private deep copy of its schema, so the
// cached config never shares a mutable *TopicSchema with callers.
func withClonedSchema(cfg TopicConfig) TopicConfig {
	cfg.Schema = CloneSchema(cfg.Schema)
	return cfg
}

// Update overwrites an existing topic configuration without changing its
// schema (schema changes go through UpdateSchema).
func (ts *TopicStore) Update(ctx context.Context, cfg TopicConfig) error {
	if err := cfg.Schema.Validate(); err != nil {
		return fmt.Errorf("Update: invalid schema: %w", err)
	}
	current, err := ts.Get(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("Update: checking existence of %q: %w", cfg.Name, err)
	}
	oldSchema, _ := json.Marshal(current.Schema)
	newSchema, _ := json.Marshal(cfg.Schema)
	if string(oldSchema) != string(newSchema) {
		return fmt.Errorf("Update: schema is immutable")
	}
	return ts.putConfig(ctx, cfg)
}

// UpdateSchema writes a topic configuration whose schema changed, bypassing
// Update's schema-immutability guard. It is the topic-schema evolution path
// (the new schema is versioned in the registry before the config is written).
func (ts *TopicStore) UpdateSchema(ctx context.Context, cfg TopicConfig) error {
	if err := cfg.Schema.Validate(); err != nil {
		return fmt.Errorf("UpdateSchema: invalid schema: %w", err)
	}
	return ts.putConfig(ctx, cfg)
}

// Get retrieves a topic configuration by name. Returns a wrapped storage.ErrNotFound if missing.
func (ts *TopicStore) Get(ctx context.Context, name string) (TopicConfig, error) {
	// Check cache first.
	if v, ok := ts.cache.Load(name); ok {
		cfg := v.(TopicConfig)
		cfg.Schema = CloneSchema(cfg.Schema)
		return cfg, nil
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
	cfg.Schema = CloneSchema(cfg.Schema)
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
	for i := range topics {
		topics[i].Schema = CloneSchema(topics[i].Schema)
		ts.cache.Store(topics[i].Name, topics[i])
		live[topics[i].Name] = struct{}{}
	}
	ts.cache.Range(func(key, _ any) bool {
		if _, ok := live[key.(string)]; !ok {
			ts.cache.Delete(key)
		}
		return true
	})

	return topics, nil
}

// ListCached returns all topic configurations from the in-memory cache without
// hitting S3. Returns nil if the cache has never been populated (call List first).
// Useful for hot-path handlers like Kafka Metadata that are called frequently.
func (ts *TopicStore) ListCached() []TopicConfig {
	var topics []TopicConfig
	ts.cache.Range(func(_, v any) bool {
		topics = append(topics, withClonedSchema(v.(TopicConfig)))
		return true
	})
	return topics
}

// Delete removes a topic configuration from S3.
func (ts *TopicStore) Delete(ctx context.Context, name string) error {
	if err := ts.s3Client.Delete(ctx, topicKey(name)); err != nil {
		return fmt.Errorf("Delete %q: %w", name, err)
	}
	ts.cache.Delete(name)
	return nil
}
