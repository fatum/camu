package coordination

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func newTestS3Client(t *testing.T) *storage.S3Client {
	t.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Region:   "us-east-1",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("failed to create s3 client: %v", err)
	}
	return s3Client
}

func TestLeaderElection_Acquire(t *testing.T) {
	s3 := newTestS3Client(t)
	le1 := NewLeaderElection(s3, "instance-1", 5*time.Second)
	le2 := NewLeaderElection(s3, "instance-2", 5*time.Second)
	ctx := context.Background()

	// First instance acquires leadership.
	lease1, acquired, err := le1.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire instance-1: %v", err)
	}
	if !acquired {
		t.Fatal("expected instance-1 to acquire leadership")
	}
	if lease1.InstanceID != "instance-1" {
		t.Errorf("expected instance-1, got %q", lease1.InstanceID)
	}

	// Second instance fails to acquire.
	lease2, acquired, err := le2.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire instance-2: %v", err)
	}
	if acquired {
		t.Fatal("expected instance-2 to NOT acquire leadership")
	}
	if lease2.InstanceID != "instance-1" {
		t.Errorf("expected leader to be instance-1, got %q", lease2.InstanceID)
	}
}

func TestLeaderElection_Renew(t *testing.T) {
	s3 := newTestS3Client(t)
	le := NewLeaderElection(s3, "instance-1", 5*time.Second)
	ctx := context.Background()

	lease, acquired, err := le.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire leadership")
	}

	renewed, err := le.Renew(ctx, lease)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if renewed.InstanceID != "instance-1" {
		t.Errorf("expected instance-1, got %q", renewed.InstanceID)
	}
	if renewed.ETag == "" {
		t.Error("expected non-empty ETag after renewal")
	}
	if renewed.ETag == lease.ETag {
		t.Error("expected ETag to change after renewal")
	}
}

func TestLeaderElection_Failover(t *testing.T) {
	s3 := newTestS3Client(t)
	le1 := NewLeaderElection(s3, "instance-1", 1*time.Millisecond)
	le2 := NewLeaderElection(s3, "instance-2", 5*time.Second)
	ctx := context.Background()

	// Instance 1 acquires with very short TTL.
	_, acquired, err := le1.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire instance-1: %v", err)
	}
	if !acquired {
		t.Fatal("expected instance-1 to acquire leadership")
	}

	// Wait for lease to expire.
	time.Sleep(10 * time.Millisecond)

	// Instance 2 takes over.
	lease2, acquired, err := le2.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire instance-2: %v", err)
	}
	if !acquired {
		t.Fatal("expected instance-2 to acquire leadership after expiry")
	}
	if lease2.InstanceID != "instance-2" {
		t.Errorf("expected instance-2, got %q", lease2.InstanceID)
	}
}

func TestLeaderElection_GetLeader(t *testing.T) {
	s3 := newTestS3Client(t)
	le := NewLeaderElection(s3, "instance-1", 5*time.Second)
	ctx := context.Background()

	_, _, err := le.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}

	got, err := le.GetLeader(ctx)
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	if got.InstanceID != "instance-1" {
		t.Errorf("expected instance-1, got %q", got.InstanceID)
	}
}
