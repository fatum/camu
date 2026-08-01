package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// fakeObjectStore is an in-memory ObjectStore for standalone tests. It
// depends on nothing from the rest of Camu, which proves the jobqueue
// package has no hidden coupling.
type fakeObjectStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{data: map[string][]byte{}}
}

func (f *fakeObjectStore) Put(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.data[key] = cp
	return nil
}

func (f *fakeObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (f *fakeObjectStore) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return nil
}

func (f *fakeObjectStore) List(_ context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for k := range f.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

// ---- Tests ----

func TestQueueRoundTrip(t *testing.T) {
	q := NewQueue(newFakeObjectStore(), "_coordination/partition_jobs/")
	ctx := context.Background()

	if err := q.Put(ctx, "events", 0, "parquet_export/0-9", []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("put: %v", err)
	}
	entries, err := q.List(ctx, "events", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len = %d, want 1", len(entries))
	}
	if string(entries[0].Data) != `{"ok":true}` {
		t.Fatalf("data = %s", entries[0].Data)
	}
	if err := q.Delete(ctx, "events", 0, "parquet_export/0-9"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	after, _ := q.List(ctx, "events", 0)
	if len(after) != 0 {
		t.Fatalf("after delete len = %d, want 0", len(after))
	}
}

func TestQueueIsolatedPerTopicAndPartition(t *testing.T) {
	q := NewQueue(newFakeObjectStore(), "_coordination/partition_jobs/")
	ctx := context.Background()

	type row struct {
		topic string
		part  int
		id    string
	}
	for _, r := range []row{
		{"events", 0, "a"}, {"events", 0, "b"},
		{"events", 1, "a"},
		{"orders", 0, "a"},
	} {
		if err := q.Put(ctx, r.topic, r.part, r.id, []byte("{}")); err != nil {
			t.Fatalf("put %+v: %v", r, err)
		}
	}

	must := func(topic string, part, want int) {
		t.Helper()
		entries, err := q.List(ctx, topic, part)
		if err != nil {
			t.Fatalf("list %s/%d: %v", topic, part, err)
		}
		if len(entries) != want {
			t.Fatalf("list %s/%d = %d, want %d", topic, part, len(entries), want)
		}
	}
	must("events", 0, 2)
	must("events", 1, 1)
	must("orders", 0, 1)
	must("orders", 1, 0)
	must("unknown", 0, 0)

	// Deleting on (events, 0) does not touch (events, 1).
	if err := q.Delete(ctx, "events", 0, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	must("events", 0, 1)
	must("events", 1, 1)
}

func TestQueuePutIsIdempotentOverwrite(t *testing.T) {
	q := NewQueue(newFakeObjectStore(), "_coordination/partition_jobs/")
	ctx := context.Background()
	for range 5 {
		if err := q.Put(ctx, "events", 0, "job-1", []byte(`{"phase":"pending"}`)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	entries, _ := q.List(ctx, "events", 0)
	if len(entries) != 1 {
		t.Fatalf("len = %d, want 1", len(entries))
	}
	// Transition phase — overwrites in place.
	if err := q.Put(ctx, "events", 0, "job-1", []byte(`{"phase":"running"}`)); err != nil {
		t.Fatalf("put 2: %v", err)
	}
	entries, _ = q.List(ctx, "events", 0)
	if len(entries) != 1 || string(entries[0].Data) != `{"phase":"running"}` {
		t.Fatalf("after transition: %+v", entries)
	}
}

func TestQueueConcurrentPutsOnDistinctKeys(t *testing.T) {
	q := NewQueue(newFakeObjectStore(), "_coordination/partition_jobs/")
	ctx := context.Background()

	const n = 64
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("job-%d", i)
			if err := q.Put(ctx, "events", 0, id, []byte("{}")); err != nil {
				t.Errorf("put %s: %v", id, err)
			}
		}(i)
	}
	wg.Wait()
	entries, _ := q.List(ctx, "events", 0)
	if len(entries) != n {
		t.Fatalf("len = %d, want %d", len(entries), n)
	}
}

func TestQueueDeleteOfMissingIsNoop(t *testing.T) {
	q := NewQueue(newFakeObjectStore(), "_coordination/partition_jobs/")
	// A crash-retry that calls Delete twice must not error.
	if err := q.Delete(context.Background(), "events", 0, "missing"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
}

func TestQueueGetNotFound(t *testing.T) {
	q := NewQueue(newFakeObjectStore(), "_coordination/partition_jobs/")
	_, err := q.Get(context.Background(), "events", 0, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestQueueKeyEscapingPreservesSlashesInID(t *testing.T) {
	// Job IDs like "parquet_export/events-0" should be legal — the queue
	// URL-escapes them so they don't collide with the (topic, partition)
	// path segments.
	q := NewQueue(newFakeObjectStore(), "_coordination/partition_jobs/")
	ctx := context.Background()
	if err := q.Put(ctx, "events", 0, "parquet_export/events-0-10", []byte("{}")); err != nil {
		t.Fatalf("put: %v", err)
	}
	entries, err := q.List(ctx, "events", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len = %d, want 1", len(entries))
	}
	// The escaped slash must not leak a new directory — the entry key
	// stays under the same (topic, partition) prefix.
	if !strings.HasPrefix(entries[0].Key, "_coordination/partition_jobs/events/0/") {
		t.Fatalf("escaped key leaked: %s", entries[0].Key)
	}
}
