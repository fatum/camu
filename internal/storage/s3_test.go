package storage

import (
	"context"
	"testing"
)

func TestS3Client_PutAndGet(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	err := client.Put(ctx, "test/key.txt", []byte("hello"), PutOpts{})
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	data, err := client.Get(ctx, "test/key.txt")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("Get() = %q, want %q", string(data), "hello")
	}
}

func TestS3Client_Delete(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	client.Put(ctx, "test/del.txt", []byte("bye"), PutOpts{})
	err := client.Delete(ctx, "test/del.txt")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	_, err = client.Get(ctx, "test/del.txt")
	if err == nil {
		t.Fatal("Get() after Delete should error")
	}
}

func TestS3Client_List(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	client.Put(ctx, "list/a.txt", []byte("a"), PutOpts{})
	client.Put(ctx, "list/b.txt", []byte("b"), PutOpts{})
	client.Put(ctx, "other/c.txt", []byte("c"), PutOpts{})

	keys, err := client.List(ctx, "list/")
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("List() returned %d keys, want 2", len(keys))
	}
}

func TestS3Client_ConditionalPut(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	// First put — no etag required
	etag1, err := client.ConditionalPut(ctx, "cond/key.txt", []byte("v1"), "")
	if err != nil {
		t.Fatalf("ConditionalPut() error: %v", err)
	}
	if etag1 == "" {
		t.Fatal("ConditionalPut() should return etag")
	}

	// Update with correct etag
	etag2, err := client.ConditionalPut(ctx, "cond/key.txt", []byte("v2"), etag1)
	if err != nil {
		t.Fatalf("ConditionalPut() with correct etag error: %v", err)
	}

	// Update with stale etag should fail
	_, err = client.ConditionalPut(ctx, "cond/key.txt", []byte("v3"), etag1)
	if err == nil {
		t.Fatal("ConditionalPut() with stale etag should error")
	}

	_ = etag2
}

// newTestClient creates a client backed by an in-memory map for unit tests.
func newTestClient(t *testing.T) *S3Client {
	t.Helper()
	client, err := NewS3Client(S3Config{
		Bucket:   "test-bucket",
		Region:   "us-east-1",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error: %v", err)
	}
	return client
}
