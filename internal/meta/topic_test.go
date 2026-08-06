package meta

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func newTestStore(t *testing.T) *TopicStore {
	t.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test-bucket",
		Region:   "us-east-1",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewTopicStore(s3Client)
}

func TestTopicStore_CreateAndGet(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	cfg := TopicConfig{
		Name:       "orders",
		Partitions: 4,
		Retention:  7 * 24 * time.Hour,
		CreatedAt:  time.Now().UTC().Truncate(time.Second),
	}

	if err := store.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != cfg.Name {
		t.Errorf("Name: got %q, want %q", got.Name, cfg.Name)
	}
	if got.Partitions != cfg.Partitions {
		t.Errorf("Partitions: got %d, want %d", got.Partitions, cfg.Partitions)
	}
	if got.Retention != cfg.Retention {
		t.Errorf("Retention: got %v, want %v", got.Retention, cfg.Retention)
	}
	if !got.CreatedAt.Equal(cfg.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, cfg.CreatedAt)
	}
}

func TestTopicStore_List(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	topics := []TopicConfig{
		{Name: "orders", Partitions: 4, Retention: 7 * 24 * time.Hour, CreatedAt: time.Now().UTC()},
		{Name: "payments", Partitions: 2, Retention: 30 * 24 * time.Hour, CreatedAt: time.Now().UTC()},
	}

	for _, cfg := range topics {
		if err := store.Create(ctx, cfg); err != nil {
			t.Fatalf("Create %q: %v", cfg.Name, err)
		}
	}

	list, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(list) != 2 {
		t.Errorf("List count: got %d, want 2", len(list))
	}
}

func TestTopicStore_Delete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	cfg := TopicConfig{
		Name:       "orders",
		Partitions: 4,
		Retention:  7 * 24 * time.Hour,
		CreatedAt:  time.Now().UTC(),
	}

	if err := store.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := store.Delete(ctx, "orders"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.Get(ctx, "orders")
	if err == nil {
		t.Fatal("Get after Delete: expected error, got nil")
	}
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get after Delete: expected ErrNotFound, got %v", err)
	}
}

func TestTopicConfig_ReplicationFields(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	cfg := TopicConfig{
		Name:                  "orders",
		Partitions:            4,
		Retention:             7 * 24 * time.Hour,
		CreatedAt:             time.Now().UTC().Truncate(time.Second),
		ReplicationFactor:     3,
		MinInsyncReplicas:     2,
		UncleanLeaderElection: true,
	}

	if err := store.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ReplicationFactor != 3 {
		t.Errorf("ReplicationFactor: got %d, want 3", got.ReplicationFactor)
	}
	if got.MinInsyncReplicas != 2 {
		t.Errorf("MinInsyncReplicas: got %d, want 2", got.MinInsyncReplicas)
	}
	if !got.UncleanLeaderElection {
		t.Errorf("UncleanLeaderElection: got false, want true")
	}
}

func TestTopicConfig_DefaultsToOne(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	cfg := TopicConfig{
		Name:       "events",
		Partitions: 1,
		Retention:  24 * time.Hour,
		CreatedAt:  time.Now().UTC().Truncate(time.Second),
		// ReplicationFactor and MinInsyncReplicas deliberately not set (zero)
	}

	if err := store.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.Get(ctx, "events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ReplicationFactor != 1 {
		t.Errorf("ReplicationFactor: got %d, want 1 (default)", got.ReplicationFactor)
	}
	if got.MinInsyncReplicas != 1 {
		t.Errorf("MinInsyncReplicas: got %d, want 1 (default)", got.MinInsyncReplicas)
	}
}

func TestTopicStore_CreateDuplicate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	cfg := TopicConfig{
		Name:       "orders",
		Partitions: 4,
		Retention:  7 * 24 * time.Hour,
		CreatedAt:  time.Now().UTC(),
	}

	if err := store.Create(ctx, cfg); err != nil {
		t.Fatalf("Create first: %v", err)
	}

	err := store.Create(ctx, cfg)
	if err == nil {
		t.Fatal("Create duplicate: expected error, got nil")
	}
}

func schemaCfg(version int) TopicConfig {
	return TopicConfig{
		Name:              "orders",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now().UTC(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		Schema:            &TopicSchema{Encoding: "json", Version: version, Fields: []SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}},
	}
}

// TestTopicStore_UpdateRejectsSchemaChange and UpdateSchema bypasses it: the
// schema-evolution path must not run into the immutability guard (the HTTP
// schema endpoint previously always failed with 500).
func TestTopicStore_UpdateSchemaBypassesImmutabilityGuard(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	v0 := schemaCfg(0)
	if err := store.Create(ctx, v0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Generic Update with a changed schema is rejected.
	v1 := schemaCfg(1)
	if err := store.Update(ctx, v1); err == nil {
		t.Fatal("Update with changed schema: expected error, got nil")
	}

	// UpdateSchema writes the new version.
	if err := store.UpdateSchema(ctx, v1); err != nil {
		t.Fatalf("UpdateSchema: %v", err)
	}
	got, err := store.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Schema == nil || got.Schema.Version != 1 {
		t.Fatalf("schema version after UpdateSchema = %+v, want version 1", got.Schema)
	}
}

// TestTopicStore_GetReturnsIndependentSchema ensures a caller mutating the
// schema of a returned TopicConfig cannot corrupt the cached copy or the
// schema another reader sees.
func TestTopicStore_GetReturnsIndependentSchema(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	v0 := schemaCfg(0)
	if err := store.Create(ctx, v0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	first, err := store.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	second, err := store.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	first.Schema.Version = 99
	first.Schema.Fields[0].Name = "mutated"
	if second.Schema.Version != 0 || second.Schema.Fields[0].Name != "id" {
		t.Fatalf("mutating one Get's schema leaked into another: %+v", second.Schema)
	}
	if third, _ := store.Get(ctx, "orders"); third.Schema.Version != 0 || third.Schema.Fields[0].Name != "id" {
		t.Fatalf("mutating one Get's schema leaked into a later Get: %+v", third.Schema)
	}
}

// TestTopicStore_CreateIsConditionalAcrossInstances verifies the create-if-not-
// exists write holds even when the second store has a cold cache (no TOCTOU
// window between the existence check and the write).
func TestTopicStore_CreateIsConditionalAcrossInstances(t *testing.T) {
	ctx := context.Background()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test-bucket",
		Region:   "us-east-1",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatal(err)
	}
	store := NewTopicStore(s3Client)
	other := NewTopicStore(s3Client) // same backend, cold cache
	if err := store.Create(ctx, schemaCfg(0)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := other.Create(ctx, schemaCfg(1)); err == nil {
		t.Fatal("Create from cold cache: expected error, got nil")
	}
	got, err := store.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Schema != nil && got.Schema.Version != 0 {
		t.Fatalf("Create overwrote the topic: schema version = %d, want 0", got.Schema.Version)
	}
}
