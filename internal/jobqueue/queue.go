// Package jobqueue is a minimal S3-backed per-(topic, partition) job
// queue. It is used by Camu's partition-leader maintenance framework to
// persist durable work items (retention, segment merge, parquet export,
// parquet compaction). The queue itself is payload-agnostic — callers
// marshal their own job struct and pass raw bytes.
//
// Contract:
//
//   - Keys are organized as {root}{topic}/{partition}/{urlescape(id)}.json
//     so one leader per (topic, partition) reads a private slice of the
//     object-store namespace.
//   - Put is a blind upsert; putting the same (topic, partition, id)
//     twice overwrites and is idempotent.
//   - List returns all pending entries for one (topic, partition) slot,
//     sorted lexicographically by key (callers that need payload-order
//     sort after unmarshal).
//   - Delete is a no-op if the key does not exist.
//
// The package has no dependency on storage, server, or parquet —
// callers supply their own ObjectStore adapter.
package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
)

// ErrNotFound is returned by an ObjectStore's Get when the key does not
// exist. The queue recognizes this error via errors.Is.
var ErrNotFound = errors.New("jobqueue: not found")

// ObjectStore is the minimal backend contract the queue depends on.
// Implementations must translate backend-specific "not found" errors
// into jobqueue.ErrNotFound.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// Queue is a per-(topic, partition) job queue rooted at a configurable
// prefix on an ObjectStore.
type Queue struct {
	objects ObjectStore
	root    string
}

// Entry is one item returned by List. Data is the raw payload bytes.
type Entry struct {
	Key  string
	Data []byte
}

// NewQueue constructs a queue with the given root prefix (e.g.
// "_coordination/partition_jobs/"). A trailing "/" is enforced.
func NewQueue(objects ObjectStore, root string) *Queue {
	if root == "" {
		root = "jobs/"
	}
	if root[len(root)-1] != '/' {
		root += "/"
	}
	return &Queue{objects: objects, root: root}
}

func (q *Queue) keyFor(topic string, partition int, id string) string {
	return fmt.Sprintf("%s%s/%d/%s.json", q.root, topic, partition, url.PathEscape(id))
}

func (q *Queue) prefixFor(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d/", q.root, topic, partition)
}

// Put writes a job payload under (topic, partition, id). Overwrites are
// allowed and idempotent — the same id can be re-put to transition a
// job through phases without duplicating entries.
func (q *Queue) Put(ctx context.Context, topic string, partition int, id string, data []byte) error {
	if topic == "" || id == "" {
		return fmt.Errorf("jobqueue: topic and id are required")
	}
	key := q.keyFor(topic, partition, id)
	if err := q.objects.Put(ctx, key, data); err != nil {
		return fmt.Errorf("jobqueue: put %q: %w", key, err)
	}
	return nil
}

// Get returns the raw payload for one job, or ErrNotFound if missing.
func (q *Queue) Get(ctx context.Context, topic string, partition int, id string) ([]byte, error) {
	return q.objects.Get(ctx, q.keyFor(topic, partition, id))
}

// List returns all entries for one (topic, partition) slot, sorted by
// key. A different (topic, partition) is never visible — this is what
// gives one leader per slot its private queue.
func (q *Queue) List(ctx context.Context, topic string, partition int) ([]Entry, error) {
	keys, err := q.objects.List(ctx, q.prefixFor(topic, partition))
	if err != nil {
		return nil, fmt.Errorf("jobqueue: list %s/%d: %w", topic, partition, err)
	}
	slices.Sort(keys)
	out := make([]Entry, 0, len(keys))
	for _, key := range keys {
		data, err := q.objects.Get(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("jobqueue: get %q: %w", key, err)
		}
		out = append(out, Entry{Key: key, Data: data})
	}
	return out, nil
}

// Delete removes one job. A missing key is not an error — the caller
// can retry a completion delete after a crash without special handling.
func (q *Queue) Delete(ctx context.Context, topic string, partition int, id string) error {
	err := q.objects.Delete(ctx, q.keyFor(topic, partition, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("jobqueue: delete: %w", err)
	}
	return nil
}
