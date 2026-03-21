package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func newTestLeaseStore(t *testing.T) *LeaseStore {
	t.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Region:   "us-east-1",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("failed to create s3 client: %v", err)
	}
	return NewLeaseStore(s3Client)
}

func TestLease_AcquireAndRenew(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	lease, err := store.Acquire(ctx, "topic1", 0, "instance-a", "http://a:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if lease.Epoch != 1 {
		t.Errorf("expected epoch=1, got %d", lease.Epoch)
	}
	if lease.InstanceID != "instance-a" {
		t.Errorf("expected instance-a, got %q", lease.InstanceID)
	}
	if lease.ETag == "" {
		t.Error("expected non-empty ETag")
	}

	err = store.Renew(ctx, lease)
	if err != nil {
		t.Errorf("Renew failed: %v", err)
	}
}

func TestLease_ConflictingAcquire(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	_, err := store.Acquire(ctx, "topic1", 0, "instance-a", "http://a:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}

	_, err = store.Acquire(ctx, "topic1", 0, "instance-b", "http://b:8080", 5*time.Second)
	if err == nil {
		t.Fatal("expected error when acquiring held lease, got nil")
	}
	if !errors.Is(err, ErrLeaseHeld) {
		t.Errorf("expected ErrLeaseHeld, got: %v", err)
	}
}

func TestLease_AcquireExpired(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	_, err := store.Acquire(ctx, "topic1", 0, "instance-a", "http://a:8080", 1*time.Millisecond)
	if err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	lease, err := store.Acquire(ctx, "topic1", 0, "instance-b", "http://b:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire after expiry failed: %v", err)
	}
	if lease.Epoch != 2 {
		t.Errorf("expected epoch=2, got %d", lease.Epoch)
	}
	if lease.InstanceID != "instance-b" {
		t.Errorf("expected instance-b, got %q", lease.InstanceID)
	}
}

func TestLease_Release(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	lease, err := store.Acquire(ctx, "topic1", 0, "instance-a", "http://a:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	if err := store.Release(ctx, lease); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	lease2, err := store.Acquire(ctx, "topic1", 0, "instance-b", "http://b:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire after release failed: %v", err)
	}
	if lease2.Epoch != 2 {
		t.Errorf("expected epoch=2, got %d", lease2.Epoch)
	}
	if lease2.InstanceID != "instance-b" {
		t.Errorf("expected instance-b, got %q", lease2.InstanceID)
	}
}

func TestLease_Get(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	acquired, err := store.Acquire(ctx, "topic1", 0, "instance-a", "http://a:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	got, err := store.Get(ctx, "topic1", 0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Epoch != acquired.Epoch {
		t.Errorf("epoch mismatch: want %d, got %d", acquired.Epoch, got.Epoch)
	}
	if got.InstanceID != "instance-a" {
		t.Errorf("expected instance-a, got %q", got.InstanceID)
	}
}

func TestLease_ListForTopic(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := store.Acquire(ctx, "topicX", i, "instance-a", "http://a:8080", 5*time.Second)
		if err != nil {
			t.Fatalf("Acquire partition %d failed: %v", i, err)
		}
	}

	leases, err := store.ListForTopic(ctx, "topicX")
	if err != nil {
		t.Fatalf("ListForTopic failed: %v", err)
	}
	if len(leases) != 3 {
		t.Errorf("expected 3 leases, got %d", len(leases))
	}
}

func TestLease_ReacquireOwnLease(t *testing.T) {
	store := newTestLeaseStore(t)
	ctx := context.Background()

	lease1, err := store.Acquire(ctx, "topic1", 0, "instance-a", "http://a:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}

	// Re-acquire by same instance should succeed and keep epoch.
	lease2, err := store.Acquire(ctx, "topic1", 0, "instance-a", "http://a:8080", 5*time.Second)
	if err != nil {
		t.Fatalf("re-acquire by same instance failed: %v", err)
	}
	if lease2.Epoch != lease1.Epoch {
		t.Errorf("expected same epoch %d, got %d", lease1.Epoch, lease2.Epoch)
	}
}
