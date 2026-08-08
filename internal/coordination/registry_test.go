package coordination

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func newRegistryTestS3(t *testing.T) *storage.S3Client {
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

func TestRegistry_ActiveInstancesFiltersStale(t *testing.T) {
	s3 := newRegistryTestS3(t)
	ctx := context.Background()

	// Register a live instance and a stale one (heartbeat far in the past).
	live := NewRegistry(s3, "live", "a:8080", "a:8081", "a:8082", "", time.Minute)
	if err := live.Register(ctx); err != nil {
		t.Fatalf("register live: %v", err)
	}
	staleReg := NewRegistry(s3, "stale", "b:8080", "b:8081", "b:8082", "", time.Minute)
	if err := staleReg.Register(ctx); err != nil {
		t.Fatalf("register stale: %v", err)
	}
	// Age the stale registration's heartbeat past the TTL by rewriting it.
	stale := InstanceInfo{InstanceID: "stale", Address: "b:8080", HeartbeatAt: time.Now().Add(-2 * time.Minute)}
	data, _ := jsonMarshalInfo(stale)
	if err := s3.Put(ctx, registryKey("stale"), data, storage.PutOpts{}); err != nil {
		t.Fatalf("age stale registration: %v", err)
	}

	// A fresh reader (empty cache) must see only the live instance.
	reader := NewRegistry(s3, "reader", "c:8080", "c:8081", "c:8082", "", time.Minute)
	active, err := reader.ActiveInstances(ctx)
	if err != nil {
		t.Fatalf("ActiveInstances: %v", err)
	}
	if len(active) != 1 || active[0] != "live" {
		t.Fatalf("active = %v, want [live] (stale excluded)", active)
	}
}

func TestRegistry_GetInstanceInfo(t *testing.T) {
	s3 := newRegistryTestS3(t)
	ctx := context.Background()

	reg := NewRegistry(s3, "node", "n:8080", "n:8081", "n:8082", "k:9092", time.Minute)
	if err := reg.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}

	info, err := reg.GetInstanceInfo(ctx, "node")
	if err != nil {
		t.Fatalf("GetInstanceInfo: %v", err)
	}
	if info.Address != "n:8080" || info.KafkaAddress != "k:9092" {
		t.Fatalf("info = %+v", info)
	}
	if _, err := reg.GetInstanceInfo(ctx, "missing"); err == nil {
		t.Fatal("expected error for missing instance")
	}
}

// TestRegistry_ActiveInstancesSeesNewRegistration verifies the membership read
// is never served from a stale snapshot: a node that registers after the reader
// has already looked must be visible immediately. Caching the membership set
// would let the controller under-provision replication on topic creation.
func TestRegistry_ActiveInstancesSeesNewRegistration(t *testing.T) {
	s3 := newRegistryTestS3(t)
	ctx := context.Background()

	reader := NewRegistry(s3, "reader", "r:8080", "r:8081", "r:8082", "", time.Minute)
	if err := reader.Register(ctx); err != nil {
		t.Fatalf("register reader: %v", err)
	}
	// Prime the reader by listing once.
	if _, err := reader.ActiveInstances(ctx); err != nil {
		t.Fatalf("prime ActiveInstances: %v", err)
	}
	// A peer registers afterwards and must appear in the very next read.
	peer := NewRegistry(s3, "peer", "p:8080", "p:8081", "p:8082", "", time.Minute)
	if err := peer.Register(ctx); err != nil {
		t.Fatalf("register peer: %v", err)
	}
	active, err := reader.ActiveInstances(ctx)
	if err != nil {
		t.Fatalf("ActiveInstances: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("active = %v, want both reader and peer (new registration must be visible immediately)", active)
	}
}

func jsonMarshalInfo(info InstanceInfo) ([]byte, error) {
	return json.Marshal(info)
}
