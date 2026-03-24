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
