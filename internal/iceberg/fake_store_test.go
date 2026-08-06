package iceberg

import (
	"context"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// fakeObjectStore is an in-memory ObjectStore used to exercise the metadata
// layer in isolation from real S3 / the server package. It implements the same
// ErrNotFound / ErrConflict semantics defined by this package.
type fakeObjectStore struct {
	mu         sync.Mutex
	objects    map[string]fakeObject
	conflictOn map[string]int
}

type fakeObject struct {
	data []byte
	etag string
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{objects: map[string]fakeObject{}, conflictOn: map[string]int{}}
}

// injectConditionalPutConflict causes the next n ConditionalPut calls on key to
// return ErrConflict before passing through.
func (f *fakeObjectStore) injectConditionalPutConflict(key string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conflictOn[key] = n
}

func (f *fakeObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, len(obj.data))
	copy(out, obj.data)
	return out, nil
}

func (f *fakeObjectStore) GetWithETag(_ context.Context, key string) ([]byte, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	out := make([]byte, len(obj.data))
	copy(out, obj.data)
	return out, obj.etag, nil
}

func (f *fakeObjectStore) ConditionalPut(_ context.Context, key string, data []byte, etag string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := f.conflictOn[key]; n > 0 {
		f.conflictOn[key] = n - 1
		return "", ErrConflict
	}
	existing, exists := f.objects[key]
	switch {
	case etag == "" && exists:
		return "", ErrConflict
	case etag != "" && !exists:
		return "", ErrConflict
	case etag != "" && existing.etag != etag:
		return "", ErrConflict
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	newETag := uuid.NewString()
	f.objects[key] = fakeObject{data: cp, etag: newETag}
	return newETag, nil
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
	out := []string{}
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (f *fakeObjectStore) ListEach(_ context.Context, prefix string, fn func(key string) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			if err := fn(k); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *fakeObjectStore) DeleteMany(_ context.Context, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		delete(f.objects, key)
	}
	return nil
}
