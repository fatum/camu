package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/storage"
)

func newTestS3(t *testing.T) *storage.S3Client {
	t.Helper()
	c, err := storage.NewS3Client(storage.S3Config{Bucket: "test", Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestManager_AllocateProducerID(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id1, err := m.AllocateProducerID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := m.AllocateProducerID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1+1 {
		t.Fatalf("expected sequential IDs, got %d and %d", id1, id2)
	}
}

func TestManager_AllocateProducerID_SharedS3(t *testing.T) {
	s3 := newTestS3(t)
	ctx := context.Background()

	m1 := NewManager(s3)
	old, err := m1.AllocateProducerID(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Second manager sharing the same S3 — should get the next ID.
	m2 := NewManager(s3)
	next, err := m2.AllocateProducerID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next != old+1 {
		t.Fatalf("expected next=%d, got %d (old=%d)", old+1, next, old)
	}
}

func TestManager_CheckAndAdvance_Accept(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m.AllocateProducerID(ctx)
	key := PartitionKey{Topic: "orders", Partition: 0}

	if err := m.CheckAndAdvance(id, key, 0, 3); err != nil {
		t.Fatalf("seq=0 size=3: %v", err)
	}
	if err := m.CheckAndAdvance(id, key, 3, 2); err != nil {
		t.Fatalf("seq=3 size=2: %v", err)
	}
}

func TestManager_CheckAndAdvance_Duplicate(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m.AllocateProducerID(ctx)
	key := PartitionKey{Topic: "orders", Partition: 0}

	_ = m.CheckAndAdvance(id, key, 0, 3)

	if err := m.CheckAndAdvance(id, key, 0, 3); err != ErrDuplicateSequence {
		t.Fatalf("expected ErrDuplicateSequence, got %v", err)
	}
}

func TestManager_CheckAndAdvance_Gap(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m.AllocateProducerID(ctx)
	key := PartitionKey{Topic: "orders", Partition: 0}

	_ = m.CheckAndAdvance(id, key, 0, 1)

	if err := m.CheckAndAdvance(id, key, 5, 1); err != ErrSequenceGap {
		t.Fatalf("expected ErrSequenceGap, got %v", err)
	}
}

func TestManager_CheckAndAdvance_UnknownProducer(t *testing.T) {
	m := NewManager(newTestS3(t))
	key := PartitionKey{Topic: "orders", Partition: 0}

	if err := m.CheckAndAdvance(99999, key, 5, 1); err != ErrUnknownProducer {
		t.Fatalf("expected ErrUnknownProducer, got %v", err)
	}
}

func TestManager_CheckAndAdvance_UnknownProducerSeqZero(t *testing.T) {
	m := NewManager(newTestS3(t))
	key := PartitionKey{Topic: "orders", Partition: 0}

	if err := m.CheckAndAdvance(99999, key, 0, 1); err != nil {
		t.Fatalf("auto-register failed: %v", err)
	}
	if err := m.CheckAndAdvance(99999, key, 1, 1); err != nil {
		t.Fatalf("follow-up after auto-register failed: %v", err)
	}
}

func TestManager_CheckAndAdvance_MultiPartition(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m.AllocateProducerID(ctx)
	k1 := PartitionKey{Topic: "orders", Partition: 0}
	k2 := PartitionKey{Topic: "orders", Partition: 1}

	if err := m.CheckAndAdvance(id, k1, 0, 5); err != nil {
		t.Fatalf("k1 seq=0: %v", err)
	}
	if err := m.CheckAndAdvance(id, k2, 0, 3); err != nil {
		t.Fatalf("k2 seq=0: %v", err)
	}
	if err := m.CheckAndAdvance(id, k1, 5, 1); err != nil {
		t.Fatalf("k1 seq=5: %v", err)
	}
	if err := m.CheckAndAdvance(id, k2, 3, 1); err != nil {
		t.Fatalf("k2 seq=3: %v", err)
	}
}

func TestManager_RecordAndGetResult(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m.AllocateProducerID(ctx)
	key := PartitionKey{Topic: "orders", Partition: 0}
	_ = m.CheckAndAdvance(id, key, 0, 3)

	// Duplicate should be detected.
	if err := m.CheckAndAdvance(id, key, 0, 3); err != ErrDuplicateSequence {
		t.Fatalf("expected ErrDuplicateSequence, got %v", err)
	}
	// Next sequence should work.
	if err := m.CheckAndAdvance(id, key, 3, 1); err != nil {
		t.Fatalf("seq=3: %v", err)
	}
}

func TestManager_CheckpointPartitionAndReload(t *testing.T) {
	m1 := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m1.AllocateProducerID(ctx)
	key := PartitionKey{Topic: "events", Partition: 2}

	_ = m1.CheckAndAdvance(id, key, 0, 5)

	data, err := m1.CheckpointPartition(key)
	if err != nil {
		t.Fatal(err)
	}

	// Load into a fresh manager.
	m2 := NewManager(newTestS3(t))
	m2.LoadCheckpoint(data)

	if err := m2.CheckAndAdvance(id, key, 0, 5); err != ErrDuplicateSequence {
		t.Fatalf("expected ErrDuplicateSequence after reload, got %v", err)
	}
	if err := m2.CheckAndAdvance(id, key, 5, 1); err != nil {
		t.Fatalf("seq=5 after reload: %v", err)
	}
}

func TestManager_EvictStale(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m.AllocateProducerID(ctx)
	key := PartitionKey{Topic: "orders", Partition: 0}
	_ = m.CheckAndAdvance(id, key, 0, 1)

	n := m.EvictStale(0)
	if n != 1 {
		t.Fatalf("expected 1 eviction, got %d", n)
	}

	if err := m.CheckAndAdvance(id, key, 1, 1); err != ErrUnknownProducer {
		t.Fatalf("expected ErrUnknownProducer after eviction, got %v", err)
	}
}

func TestManager_RebuildFromBatches(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id, _ := m.AllocateProducerID(ctx)
	key := PartitionKey{Topic: "tx", Partition: 0}

	_ = m.CheckAndAdvance(id, key, 0, 5)
	data, err := m.CheckpointPartition(key)
	if err != nil {
		t.Fatal(err)
	}

	m2 := NewManager(newTestS3(t))
	m2.LoadCheckpoint(data)

	m2.RebuildFromBatches([]BatchInfo{
		{ProducerID: id, Sequence: 5, BatchSize: 3, Key: key},
		{ProducerID: id, Sequence: 8, BatchSize: 2, Key: key},
	})

	if err := m2.CheckAndAdvance(id, key, 10, 1); err != nil {
		t.Fatalf("seq=10 after rebuild: %v", err)
	}
	if err := m2.CheckAndAdvance(id, key, 5, 3); err != ErrDuplicateSequence {
		t.Fatalf("expected duplicate, got %v", err)
	}
}

func TestManager_EvictStale_RespectsActivity(t *testing.T) {
	m := NewManager(newTestS3(t))
	ctx := context.Background()
	id1, _ := m.AllocateProducerID(ctx)
	id2, _ := m.AllocateProducerID(ctx)

	key := PartitionKey{Topic: "t", Partition: 0}
	_ = m.CheckAndAdvance(id1, key, 0, 1)
	_ = m.CheckAndAdvance(id2, key, 0, 1)

	n := m.EvictStale(time.Hour)
	if n != 0 {
		t.Fatalf("expected 0 evictions with 1h TTL, got %d", n)
	}

	if err := m.CheckAndAdvance(id1, key, 1, 1); err != nil {
		t.Fatalf("id1 should still work: %v", err)
	}
	if err := m.CheckAndAdvance(id2, key, 1, 1); err != nil {
		t.Fatalf("id2 should still work: %v", err)
	}
}
