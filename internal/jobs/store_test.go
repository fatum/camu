package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/maksim/camu/internal/jobqueue"
)

type fakeObjectStore struct {
	mu      sync.Mutex
	objects map[string]fakeObject
}

type fakeObject struct {
	data []byte
	etag string
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{objects: map[string]fakeObject{}}
}

func (f *fakeObjectStore) Put(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]byte(nil), data...)
	f.objects[key] = fakeObject{data: cp, etag: uuid.NewString()}
	return nil
}

func (f *fakeObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return nil, jobqueue.ErrNotFound
	}
	return append([]byte(nil), obj.data...), nil
}

func (f *fakeObjectStore) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func (f *fakeObjectStore) List(_ context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0)
	for k := range f.objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func TestStoreRoundTrip(t *testing.T) {
	store := NewStore(newFakeObjectStore(), "_coordination/jobs/")
	ctx := context.Background()

	job := Record{
		ID:            ID(TypeRetention, "events-0-0-9"),
		Topic:         "events",
		Partition:     0,
		Type:          TypeRetention,
		ExpectedOwner: "n1",
		ExpectedEpoch: 3,
		State:         StatePending,
		Phase:         PhasePublishData,
		StartedAt:     time.Now().UTC(),
	}
	if err := store.Put(ctx, job); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, err := store.List(ctx, "events", 0)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(List()) = %d, want 1", len(got))
	}
	if got[0].ID != job.ID || got[0].Type != job.Type || got[0].ExpectedOwner != job.ExpectedOwner {
		t.Fatalf("got %+v, want %+v", got[0], job)
	}
	if err := store.Delete(ctx, "events", 0, job.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	got, err = store.List(ctx, "events", 0)
	if err != nil {
		t.Fatalf("List() after delete error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("len(List()) after delete = %d, want 0", len(got))
	}
}

func TestStoreOrdersByUpdatedAt(t *testing.T) {
	store := NewStore(newFakeObjectStore(), "_coordination/jobs/")
	ctx := context.Background()

	for _, id := range []string{"c", "a", "b"} {
		if err := store.Put(ctx, Record{
			ID:        id,
			Topic:     "events",
			Partition: 0,
			Type:      TypeRetention,
			State:     StatePending,
			StartedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("Put(%s) error = %v", id, err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	got, err := store.List(ctx, "events", 0)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	want := []string{"c", "a", "b"}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("got[%d].ID = %q, want %q", i, got[i].ID, id)
		}
	}
}

func TestIDStableEscaping(t *testing.T) {
	got := ID(TypeRetention, "segment/path/file.json")
	if got != "retention/segment%2Fpath%2Ffile" {
		t.Fatalf("ID() = %q", got)
	}
}

func TestStorePropagatesListErrors(t *testing.T) {
	store := NewStore(errorObjectStore{err: errors.New("boom")}, "_coordination/jobs/")
	_, err := store.List(context.Background(), "events", 0)
	if err == nil {
		t.Fatal("List() error = nil, want error")
	}
}

type errorObjectStore struct{ err error }

func (e errorObjectStore) Put(context.Context, string, []byte) error      { return e.err }
func (e errorObjectStore) Get(context.Context, string) ([]byte, error)    { return nil, e.err }
func (e errorObjectStore) Delete(context.Context, string) error           { return e.err }
func (e errorObjectStore) List(context.Context, string) ([]string, error) { return nil, e.err }
