// parquet-demo is a standalone binary that uses internal/iceberg and
// internal/jobqueue WITHOUT importing internal/server or
// internal/storage. It exists to prove the two packages are actually
// extractable: a consumer only has to implement two small interfaces
// (iceberg.ObjectStore, jobqueue.ObjectStore) and can drive the full
// manifest-publish / compaction / queue-put / queue-list pipeline.
//
// Run with: go run ./cmd/parquet-demo
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/jobqueue"
)

// memStore is an in-memory ObjectStore that satisfies BOTH
// iceberg.ObjectStore and jobqueue.ObjectStore. It has zero dependency
// on storage.S3Client — it is plain Go maps.
type memStore struct {
	mu      sync.Mutex
	objects map[string]memObject
	counter int
}

type memObject struct {
	data []byte
	etag string
}

func newMemStore() *memStore { return &memStore{objects: map[string]memObject{}} }

// --- iceberg.ObjectStore ---

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, iceberg.ErrNotFound
	}
	return append([]byte(nil), obj.data...), nil
}

func (m *memStore) GetWithETag(_ context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, "", iceberg.ErrNotFound
	}
	return append([]byte(nil), obj.data...), obj.etag, nil
}

func (m *memStore) ConditionalPut(_ context.Context, key string, data []byte, etag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, exists := m.objects[key]
	switch {
	case etag == "" && exists:
		return "", iceberg.ErrConflict
	case etag != "" && !exists:
		return "", iceberg.ErrConflict
	case etag != "" && existing.etag != etag:
		return "", iceberg.ErrConflict
	}
	m.counter++
	newETag := fmt.Sprintf("etag-%d", m.counter)
	m.objects[key] = memObject{data: append([]byte(nil), data...), etag: newETag}
	return newETag, nil
}

func (m *memStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []string{}
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memStore) ListEach(_ context.Context, prefix string, fn func(key string) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			if err := fn(k); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *memStore) DeleteMany(_ context.Context, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		delete(m.objects, key)
	}
	return nil
}

// --- jobqueue.ObjectStore: Put only (the other methods above already
// satisfy the jobqueue interface by shape). ---

func (m *memStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counter++
	m.objects[key] = memObject{data: append([]byte(nil), data...), etag: fmt.Sprintf("etag-%d", m.counter)}
	return nil
}

func main() {
	ctx := context.Background()
	store := newMemStore()

	// ---- Parquet Store ----
	pStore := iceberg.NewStore(store, iceberg.NoFencer{})
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)

	manifest := iceberg.Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []iceberg.Entry{
			{ObjectKey: iceberg.ExportObjectKey("events", 0, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1},
			{ObjectKey: iceberg.ExportObjectKey("events", 0, ts, 10, 19, 1, "events/0/10-19-1.segment|epoch=1"), BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "events/0/10-19-1.segment", SourceEpoch: 1},
		},
	}

	published, err := pStore.PublishManifest(ctx, manifest)
	must("publish", err)
	fmt.Printf("[parquet] published gen=%d entries=%d\n", published.Generation, len(published.Entries))

	// Idempotent republish — generation must not bump.
	retry, err := pStore.PublishManifest(ctx, manifest)
	must("publish retry", err)
	if retry.Generation != published.Generation {
		panic(fmt.Sprintf("retry bumped generation: %d -> %d", published.Generation, retry.Generation))
	}
	fmt.Printf("[parquet] idempotent retry gen=%d (unchanged)\n", retry.Generation)

	// Compaction: replace the two smalls with one big.
	big := iceberg.Entry{
		ObjectKey: iceberg.ExportObjectKey("events", 0, ts, 0, 19, 1, "events/0/0-19-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 19, SchemaVersion: 1, SourceKey: "compaction/events/0/0-19", SourceEpoch: 0,
	}
	compacted, err := pStore.CompactBucket(ctx, "events", 0, "2026-04-11", "13",
		[]string{manifest.Entries[0].ObjectKey, manifest.Entries[1].ObjectKey},
		[]iceberg.Entry{big})
	must("compact", err)
	if len(compacted.Entries) != 1 || compacted.Entries[0].ObjectKey != big.ObjectKey {
		panic(fmt.Sprintf("compaction did not converge: %+v", compacted.Entries))
	}
	fmt.Printf("[parquet] compacted gen=%d entries=%d\n", compacted.Generation, len(compacted.Entries))

	// Idempotent compaction retry — generation must not bump.
	retryC, err := pStore.CompactBucket(ctx, "events", 0, "2026-04-11", "13",
		[]string{manifest.Entries[0].ObjectKey, manifest.Entries[1].ObjectKey},
		[]iceberg.Entry{big})
	must("compact retry", err)
	if retryC.Generation != compacted.Generation {
		panic(fmt.Sprintf("compact retry bumped generation: %d -> %d", compacted.Generation, retryC.Generation))
	}
	fmt.Printf("[parquet] idempotent compact retry gen=%d (unchanged)\n", retryC.Generation)

	// Topic-scoped listing with inclusive time range.
	manifests, err := pStore.ListTopicManifests(ctx, "events",
		time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 11, 23, 0, 0, 0, time.UTC))
	must("list", err)
	fmt.Printf("[parquet] listed %d manifests in range\n", len(manifests))

	// ---- Job Queue ----
	q := jobqueue.NewQueue(store, "_coordination/partition_jobs/")

	type job struct {
		ID    string `json:"id"`
		Phase string `json:"phase"`
	}
	j := job{ID: "parquet_export/events-0-0-19", Phase: "pending"}
	data, _ := json.Marshal(j)
	must("put", q.Put(ctx, "events", 0, j.ID, data))

	// Idempotent re-put: same key, different phase, overwrites in place.
	j.Phase = "running"
	data, _ = json.Marshal(j)
	must("put 2", q.Put(ctx, "events", 0, j.ID, data))

	entries, err := q.List(ctx, "events", 0)
	must("list jobs", err)
	if len(entries) != 1 {
		panic(fmt.Sprintf("queue has %d jobs, want 1", len(entries)))
	}
	fmt.Printf("[jobqueue] events/0 has %d job(s)\n", len(entries))

	// Strict per-(topic, partition) isolation.
	if other, _ := q.List(ctx, "events", 1); len(other) != 0 {
		panic(fmt.Sprintf("events/1 leaked %d jobs", len(other)))
	}
	if other, _ := q.List(ctx, "orders", 0); len(other) != 0 {
		panic(fmt.Sprintf("orders/0 leaked %d jobs", len(other)))
	}
	fmt.Printf("[jobqueue] per-(topic, partition) isolation holds\n")

	// Delete and verify gone.
	must("delete", q.Delete(ctx, "events", 0, j.ID))
	if e, _ := q.List(ctx, "events", 0); len(e) != 0 {
		panic("delete did not take")
	}

	fmt.Println()
	fmt.Println("STANDALONE-PROOF-OK")
}

func must(label string, err error) {
	if err != nil {
		panic(label + ": " + err.Error())
	}
}
