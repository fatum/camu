package parquet

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeObjectStore is an in-memory ObjectStore used to exercise Store in
// isolation from real S3 / the server package. It implements the same
// ErrNotFound / ErrConflict semantics defined by this package, so it
// directly proves the package has no hidden backend dependency.
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

// injectConditionalPutConflict causes the next n ConditionalPut calls
// on key to return ErrConflict before passing through.
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
	// Test-only failure injection: fail the first N ConditionalPut
	// calls on this exact key, then pass through. Simulates a CAS race
	// with another writer without needing real concurrency.
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

func TestReplaceOverlappingEntriesReplacesOnlyIntersectingRanges(t *testing.T) {
	ctx := context.Background()
	objects := newFakeObjectStore()
	store := NewStore(objects, nil)
	const (
		topic     = "events"
		partition = 0
		date      = "2026-07-30"
		hour      = "13"
	)

	oldOverlap := Entry{ObjectKey: "old-overlap", BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "classic/events/0/10-19-1.segment", SourceEpoch: 1}
	before := Entry{ObjectKey: "before", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "classic/events/0/0-9-1.segment", SourceEpoch: 1}
	after := Entry{ObjectKey: "after", BaseOffset: 20, EndOffset: 29, SchemaVersion: 1, SourceKey: "classic/events/0/20-29-1.segment", SourceEpoch: 1}
	if _, err := store.PublishManifest(ctx, Manifest{Topic: topic, Partition: partition, Date: date, Hour: hour, SchemaVersion: 1, Entries: []Entry{before, oldOverlap, after}}); err != nil {
		t.Fatalf("seed manifest: %v", err)
	}
	replacement := Entry{ObjectKey: "authoritative", BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "classic/events/0/10-19-2.segment", SourceEpoch: 2}
	got, err := store.ReplaceOverlappingEntries(ctx, topic, partition, date, hour, []Entry{replacement})
	if err != nil {
		t.Fatalf("ReplaceOverlappingEntries() error = %v", err)
	}
	if len(got.Entries) != 3 {
		t.Fatalf("entries = %+v, want three", got.Entries)
	}
	for _, entry := range got.Entries {
		if entry.ObjectKey == oldOverlap.ObjectKey {
			t.Fatalf("obsolete overlapping entry survived: %+v", got.Entries)
		}
	}
	if got.Entries[0].ObjectKey != before.ObjectKey || got.Entries[1].ObjectKey != replacement.ObjectKey || got.Entries[2].ObjectKey != after.ObjectKey {
		t.Fatalf("entries = %+v, want before/replacement/after", got.Entries)
	}

	retry, err := store.ReplaceOverlappingEntries(ctx, topic, partition, date, hour, []Entry{replacement})
	if err != nil {
		t.Fatalf("retry ReplaceOverlappingEntries() error = %v", err)
	}
	if retry.Generation != got.Generation {
		t.Fatalf("retry generation = %d, want %d", retry.Generation, got.Generation)
	}
}

func TestReplaceOverlappingEntriesRejectsInvalidSourceWithoutChangingManifest(t *testing.T) {
	ctx := context.Background()
	objects := newFakeObjectStore()
	store := NewStore(objects, nil)
	const (
		topic     = "events"
		partition = 0
		date      = "2026-07-30"
		hour      = "13"
	)
	bucketTime := time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC)

	initial := Entry{
		ObjectKey: "initial", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1,
		SourceKey: "classic/events/0/0-9-0.segment", SourceEpoch: 0,
	}
	seed, err := store.PublishManifest(ctx, Manifest{
		Topic: topic, Partition: partition, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []Entry{initial},
	})
	if err != nil {
		t.Fatalf("seed manifest with source epoch zero: %v", err)
	}
	before, err := store.GetManifest(ctx, topic, partition, bucketTime)
	if err != nil {
		t.Fatalf("get seeded manifest: %v", err)
	}
	if !reflect.DeepEqual(before, seed) {
		t.Fatalf("seeded manifest = %+v, want %+v", before, seed)
	}

	_, err = store.ReplaceOverlappingEntries(ctx, topic, partition, date, hour, []Entry{{
		ObjectKey: "invalid", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1,
		SourceKey: " \t ", SourceEpoch: 0,
	}})
	if err == nil {
		t.Fatal("ReplaceOverlappingEntries() error = nil, want missing source_key")
	}

	after, err := store.GetManifest(ctx, topic, partition, bucketTime)
	if err != nil {
		t.Fatalf("get manifest after rejected replacement: %v", err)
	}
	if after.Generation != before.Generation {
		t.Fatalf("generation after rejected replacement = %d, want %d", after.Generation, before.Generation)
	}
	if !reflect.DeepEqual(after.Entries, before.Entries) {
		t.Fatalf("entries after rejected replacement = %+v, want %+v", after.Entries, before.Entries)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("manifest after rejected replacement = %+v, want %+v", after, before)
	}
}

// planFence is a Fencer controlled from tests.
type planFence struct{ pending map[string]bool }

func (p *planFence) TopicDeletionPending(_ context.Context, topic string) bool {
	return p.pending[topic]
}

type toggleFence struct{ checks int }

func (f *toggleFence) TopicDeletionPending(_ context.Context, _ string) bool {
	f.checks++
	return f.checks >= 3
}

func newStore() (*Store, *fakeObjectStore, *planFence) {
	fs := newFakeObjectStore()
	fence := &planFence{pending: map[string]bool{}}
	return NewStore(fs, fence), fs, fence
}

// ---- Paths ----

func TestPathHelpers(t *testing.T) {
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)
	got := ExportObjectKey("events", 7, ts, 12000, 12999, 1, "events/7/12000-12999-1.segment|epoch=1")
	if !strings.HasPrefix(got, "parquet/dt=2026-04-11/topic=events/hour=13/") || !strings.HasSuffix(got, ".parquet") {
		t.Fatalf("ExportObjectKey = %q, want analytics path layout", got)
	}
	if again := ExportObjectKey("events", 7, ts, 12000, 12999, 1, "events/7/12000-12999-1.segment|epoch=1"); got != again {
		t.Fatalf("ExportObjectKey not deterministic: %q != %q", got, again)
	}
	if other := ExportObjectKey("events", 8, ts, 12000, 12999, 1, "events/8/12000-12999-1.segment|epoch=1"); got == other {
		t.Fatalf("ExportObjectKey should vary by source identity, got %q", got)
	}
	if got, want := ManifestKey("events", 7, ts),
		"_meta/parquet_manifests/events/dt=2026-04-11/hour=13/part-7.json"; got != want {
		t.Fatalf("ManifestKey = %q, want %q", got, want)
	}
	dt, hour, ok := ParseManifestKey("events", "_meta/parquet_manifests/events/dt=2026-04-11/hour=13/part-7.json")
	if !ok || dt != "2026-04-11" || hour != "13" {
		t.Fatalf("ParseManifestKey = (%q,%q,%v), want (2026-04-11,13,true)", dt, hour, ok)
	}
	if _, _, ok := ParseManifestKey("events", "wrong/prefix.json"); ok {
		t.Fatal("ParseManifestKey accepted non-matching key")
	}
}

// ---- Manifest publish ----

func TestPublishManifestRoundTripAndGenerationBump(t *testing.T) {
	store, _, _ := newStore()
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 45, 0, 0, time.UTC)
	m := Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []Entry{{ObjectKey: ExportObjectKey("events", 0, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1"), BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1}},
	}
	first, err := store.PublishManifest(ctx, m)
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if first.Generation != 1 {
		t.Fatalf("first gen = %d, want 1", first.Generation)
	}
	m.Entries = append(m.Entries, Entry{ObjectKey: ExportObjectKey("events", 0, ts, 10, 19, 1, "events/0/10-19-1.segment|epoch=1"), BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "events/0/10-19-1.segment", SourceEpoch: 1})
	second, err := store.PublishManifest(ctx, m)
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if second.Generation != 2 {
		t.Fatalf("second gen = %d, want 2", second.Generation)
	}
}

func TestPublishManifestIdempotentOnRetry(t *testing.T) {
	store, _, _ := newStore()
	ctx := context.Background()
	m := Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []Entry{
			{ObjectKey: "a.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "test/a", SourceEpoch: 0},
			{ObjectKey: "b.parquet", BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "test/b", SourceEpoch: 0},
		},
	}
	if _, err := store.PublishManifest(ctx, m); err != nil {
		t.Fatalf("first: %v", err)
	}
	retry, err := store.PublishManifest(ctx, m)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.Generation != 1 {
		t.Fatalf("retry gen = %d, want 1 (idempotent)", retry.Generation)
	}
	// Subset also idempotent.
	subset := m
	subset.Entries = m.Entries[:1]
	sub, err := store.PublishManifest(ctx, subset)
	if err != nil {
		t.Fatalf("subset: %v", err)
	}
	if sub.Generation != 1 {
		t.Fatalf("subset gen = %d, want 1", sub.Generation)
	}
}

func TestPublishManifestFenced(t *testing.T) {
	store, _, fence := newStore()
	fence.pending["events"] = true
	_, err := store.PublishManifest(context.Background(), Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
	})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("err = %v, want ErrFenced", err)
	}
}

func TestPublishManifestRequiresBucket(t *testing.T) {
	store, _, _ := newStore()
	if _, err := store.PublishManifest(context.Background(), Manifest{Topic: "events"}); err == nil {
		t.Fatal("missing bucket validation")
	}
}

func TestPublishManifestRejectsMissingSourceKeyWithoutChangingStoredManifest(t *testing.T) {
	store, _, _ := newStore()
	ctx := context.Background()
	valid := Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []Entry{{ObjectKey: "valid.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "classic/events/0/0-9.segment", SourceEpoch: 0}},
	}
	first, err := store.PublishManifest(ctx, valid)
	if err != nil {
		t.Fatalf("seed manifest: %v", err)
	}

	invalid := valid
	invalid.Entries = append(invalid.Entries, Entry{ObjectKey: "invalid.parquet", BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceEpoch: 0})
	if _, err := store.PublishManifest(ctx, invalid); err == nil {
		t.Fatal("PublishManifest accepted missing source_key")
	}
	got, err := store.GetManifest(ctx, "events", 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if got.Generation != first.Generation || !entrySetsEqual(got.Entries, first.Entries) {
		t.Fatalf("invalid publish changed stored manifest: got %+v, want %+v", got, first)
	}
	data, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if !strings.Contains(string(data), `"source_epoch":0`) {
		t.Fatalf("source_epoch=0 was not serialized: %s", data)
	}
}

// ---- Compaction ----

func TestCompactBucketIdempotent(t *testing.T) {
	store, _, _ := newStore()
	ctx := context.Background()
	small := []Entry{
		{ObjectKey: "a.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "test/a", SourceEpoch: 0},
		{ObjectKey: "b.parquet", BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "test/b", SourceEpoch: 0},
		{ObjectKey: "c.parquet", BaseOffset: 20, EndOffset: 29, SchemaVersion: 1, SourceKey: "test/c", SourceEpoch: 0},
	}
	if _, err := store.PublishManifest(ctx, Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1, Entries: small,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	big := Entry{ObjectKey: "big.parquet", BaseOffset: 0, EndOffset: 29, SchemaVersion: 1, SourceKey: "compaction/test/0-29", SourceEpoch: 0}
	removeKeys := []string{"a.parquet", "b.parquet", "c.parquet"}

	first, err := store.CompactBucket(ctx, "events", 0, "2026-04-11", "13", removeKeys, []Entry{big})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if first.Generation != 2 || len(first.Entries) != 1 {
		t.Fatalf("first: gen=%d entries=%d", first.Generation, len(first.Entries))
	}

	retry, err := store.CompactBucket(ctx, "events", 0, "2026-04-11", "13", removeKeys, []Entry{big})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.Generation != 2 {
		t.Fatalf("retry gen = %d, want 2 (idempotent)", retry.Generation)
	}
}

func TestCompactBucketPartialCrash(t *testing.T) {
	store, _, _ := newStore()
	ctx := context.Background()
	a := Entry{ObjectKey: "a.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "test/a", SourceEpoch: 0}
	b := Entry{ObjectKey: "b.parquet", BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "test/b", SourceEpoch: 0}
	big := Entry{ObjectKey: "big.parquet", BaseOffset: 0, EndOffset: 19, SchemaVersion: 1, SourceKey: "compaction/test/0-19", SourceEpoch: 0}

	// Post-crash state: a is gone, b is still there, big is present.
	if _, err := store.PublishManifest(ctx, Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []Entry{b, big},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Retry with original removeKeys.
	final, err := store.CompactBucket(ctx, "events", 0, "2026-04-11", "13",
		[]string{a.ObjectKey, b.ObjectKey}, []Entry{big})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if len(final.Entries) != 1 || final.Entries[0].ObjectKey != big.ObjectKey {
		t.Fatalf("final = %+v, want [big]", final.Entries)
	}
}

func TestCompactBucketSchemaUpgradeCarriesForward(t *testing.T) {
	store, _, _ := newStore()
	ctx := context.Background()
	if _, err := store.PublishManifest(ctx, Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []Entry{{ObjectKey: "v1.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "test/v1", SourceEpoch: 0}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	next, err := store.CompactBucket(ctx, "events", 0, "2026-04-11", "13",
		[]string{"v1.parquet"},
		[]Entry{{ObjectKey: "v2.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 2, SourceKey: "compaction/test/v2", SourceEpoch: 0}})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if next.SchemaVersion != 2 {
		t.Fatalf("SchemaVersion = %d, want 2", next.SchemaVersion)
	}
}

func TestCompactBucketRejectsMissingSourceKeyWithoutChangingStoredManifest(t *testing.T) {
	store, _, _ := newStore()
	ctx := context.Background()
	seed := Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []Entry{{ObjectKey: "small.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "classic/events/0/0-9.segment", SourceEpoch: 0}},
	}
	first, err := store.PublishManifest(ctx, seed)
	if err != nil {
		t.Fatalf("seed manifest: %v", err)
	}
	if _, err := store.CompactBucket(ctx, "events", 0, "2026-04-11", "13", []string{"small.parquet"}, []Entry{{ObjectKey: "big.parquet", BaseOffset: 0, EndOffset: 9, SchemaVersion: 1}}); err == nil {
		t.Fatal("CompactBucket accepted missing source_key")
	}
	got, err := store.GetManifest(ctx, "events", 0, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if got.Generation != first.Generation || !entrySetsEqual(got.Entries, first.Entries) {
		t.Fatalf("invalid compaction changed stored manifest: got %+v, want %+v", got, first)
	}
}

// ---- Topic metadata deletion ----

func TestDeleteTopicMetadataRemovesDataAndMetadata(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)

	// Plant a data object and a manifest that references it.
	dataKey := ExportObjectKey("events", 0, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1")
	if _, err := fs.ConditionalPut(ctx, dataKey, []byte("stub"), ""); err != nil {
		t.Fatalf("plant: %v", err)
	}
	if _, err := store.PublishManifest(ctx, Manifest{
		Topic: "events", Partition: 0, Date: "2026-04-11", Hour: "13", SchemaVersion: 1,
		Entries: []Entry{{ObjectKey: dataKey, BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1}},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := store.DeleteTopicMetadata(ctx, "events"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := fs.Get(ctx, dataKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("data survived: %v", err)
	}
	leftover, _ := fs.List(ctx, DataPrefix)
	for _, key := range leftover {
		if strings.Contains(key, "/topic=events/") {
			t.Fatalf("parquet data leaked: %v", leftover)
		}
	}
}

// ---- Listing ----

func TestListTopicManifestsFiltersByKeyThenFetches(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()
	// Plant 3 buckets across 2 topics.
	buckets := []struct {
		topic, date, hour string
	}{
		{"events", "2026-04-10", "12"},
		{"events", "2026-04-11", "13"},
		{"events", "2026-04-11", "23"},
		{"other", "2026-04-11", "13"},
	}
	for _, b := range buckets {
		if _, err := store.PublishManifest(ctx, Manifest{
			Topic: b.topic, Partition: 0, Date: b.date, Hour: b.hour, SchemaVersion: 1,
		}); err != nil {
			t.Fatalf("seed %+v: %v", b, err)
		}
	}

	// Count how many Gets the store issues by instrumenting the fake.
	// We don't care about the exact count — we care that manifests for
	// (events, 2026-04-10, 12) are NOT fetched when the query is for
	// 2026-04-11T00 onward. Indirect proof: the function should return
	// exactly the right set, and key-pruning means the `other` topic's
	// keys are never fetched either.
	from := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 4, 11, 23, 0, 0, 0, time.UTC)
	manifests, err := store.ListTopicManifests(ctx, "events", from, to)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("got %d manifests, want 2 (hour=13 + hour=23); got=%+v", len(manifests), manifests)
	}
	// hour=23 boundary is inclusive.
	var sawHour23 bool
	for _, m := range manifests {
		if m.Hour == "23" {
			sawHour23 = true
		}
	}
	if !sawHour23 {
		t.Fatal("hour=23 bucket missing — boundary exclusive bug regressed")
	}
	// Sanity: other topic's object still exists but was not returned.
	allKeys, _ := fs.List(ctx, ManifestPrefix+"other/")
	if len(allKeys) != 1 {
		t.Fatalf("other topic data touched: %v", allKeys)
	}
}

// plantDataObject writes a data-prefix object directly into the fake
// store, bypassing the Store API — simulates a parquet upload that
// completed but may not have been followed by a manifest PUT.
func plantDataObject(fs *fakeObjectStore, key string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.objects[key] = fakeObject{data: []byte("parquet-bytes"), etag: uuid.NewString()}
}

// publishSingleEntryManifest is a test helper that publishes a manifest
// for one partition referencing exactly one data object.
func publishSingleEntryManifest(t *testing.T, store *Store, topic string, partition int, ts time.Time, baseOffset, endOffset int64) string {
	t.Helper()
	key := ExportObjectKey(topic, partition, ts, baseOffset, endOffset, 1, "test-source|epoch=1")
	date, hour := BucketDateHour(ts)
	_, err := store.PublishManifest(context.Background(), Manifest{
		Topic: topic, Partition: partition, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []Entry{{ObjectKey: key, BaseOffset: baseOffset, EndOffset: endOffset, SchemaVersion: 1, SourceKey: "test-source", SourceEpoch: 0}},
	})
	if err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	return key
}

func TestReconcileBucketRemovesUnreferencedOrphans(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	now := ts.Add(2 * time.Hour) // bucket ended ~1h ago

	// Two partitions publish valid entries into the bucket.
	refKey0 := publishSingleEntryManifest(t, store, "events", 0, ts, 0, 99)
	refKey1 := publishSingleEntryManifest(t, store, "events", 1, ts, 0, 99)
	// Plant the "real" data objects for those entries.
	plantDataObject(fs, refKey0)
	plantDataObject(fs, refKey1)

	// Plant two orphans in the same bucket — e.g. a CAS loser on the
	// manifest side and an aborted mid-publish upload.
	orphan1 := ExportObjectKey("events", 0, ts, 100, 199, 1, "events/0/100-199-1.segment|epoch=1") // not referenced
	orphan2 := ExportObjectKey("events", 1, ts, 200, 299, 1, "events/1/200-299-1.segment|epoch=1")
	plantDataObject(fs, orphan1)
	plantDataObject(fs, orphan2)

	removed, err := store.ReconcileBucket(ctx, "events", "2026-04-11", "13", now, 15*time.Minute)
	if err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}

	// Referenced objects survive.
	if _, err := fs.Get(ctx, refKey0); err != nil {
		t.Fatalf("ref partition 0 deleted: %v", err)
	}
	if _, err := fs.Get(ctx, refKey1); err != nil {
		t.Fatalf("ref partition 1 deleted: %v", err)
	}
	// Orphans are gone.
	if _, err := fs.Get(ctx, orphan1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan1 survived: %v", err)
	}
	if _, err := fs.Get(ctx, orphan2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan2 survived: %v", err)
	}
}

func TestReconcileBucketRespectsAgeFence(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	// now is only a few minutes after the bucket's hour end (14:00 UTC),
	// inside the grace window — reconcile must refuse to delete.
	now := ts.Add(time.Hour).Add(5 * time.Minute)

	orphan := ExportObjectKey("events", 0, ts, 100, 199, 1, "events/0/100-199-1.segment|epoch=1")
	plantDataObject(fs, orphan)

	removed, err := store.ReconcileBucket(ctx, "events", "2026-04-11", "13", now, 15*time.Minute)
	if err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0 (bucket too fresh)", removed)
	}
	if _, err := fs.Get(ctx, orphan); err != nil {
		t.Fatalf("object prematurely deleted inside grace window: %v", err)
	}
}

func TestReconcileBucketEmptyManifestDeletesAll(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	now := ts.Add(2 * time.Hour)

	// No manifest was ever published for this bucket — every data
	// object in it is orphaned (e.g. a crashed export that uploaded
	// before the first manifest PUT).
	o1 := ExportObjectKey("events", 0, ts, 0, 99, 1, "events/0/0-99-1.segment|epoch=1")
	o2 := ExportObjectKey("events", 1, ts, 0, 99, 1, "events/1/0-99-1.segment|epoch=1")
	plantDataObject(fs, o1)
	plantDataObject(fs, o2)

	removed, err := store.ReconcileBucket(ctx, "events", "2026-04-11", "13", now, 15*time.Minute)
	if err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
}

func TestReconcileBucketFencedDuringDeletion(t *testing.T) {
	store, fs, fence := newStore()
	fence.pending["events"] = true
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	now := ts.Add(2 * time.Hour)

	orphan := ExportObjectKey("events", 0, ts, 0, 99, 1, "events/0/0-99-1.segment|epoch=1")
	plantDataObject(fs, orphan)

	_, err := store.ReconcileBucket(ctx, "events", "2026-04-11", "13", now, 15*time.Minute)
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("ReconcileBucket during deletion err = %v, want ErrFenced", err)
	}
	// Object is untouched — DeleteTopicMetadata is the single
	// authority during cleanup.
	if _, err := fs.Get(ctx, orphan); err != nil {
		t.Fatalf("orphan wrongly deleted by fenced reconcile: %v", err)
	}
}

func TestReconcileBucketRefencesBeforeEachDelete(t *testing.T) {
	fs := newFakeObjectStore()
	fence := &toggleFence{}
	store := NewStore(fs, fence)
	ctx := context.Background()
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	now := ts.Add(2 * time.Hour)
	o1 := ExportObjectKey("events", 0, ts, 0, 9, 1, "a")
	o2 := ExportObjectKey("events", 0, ts, 10, 19, 1, "b")
	plantDataObject(fs, o1)
	plantDataObject(fs, o2)
	_, err := store.ReconcileBucket(ctx, "events", "2026-04-11", "13", now, 15*time.Minute)
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("error = %v, want ErrFenced", err)
	}
	if _, err := fs.Get(ctx, o1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("first orphan not deleted: %v", err)
	}
	if _, err := fs.Get(ctx, o2); err != nil {
		t.Fatalf("second orphan deleted after fence: %v", err)
	}
}

func TestReconcileBucketDoesNotCrossBuckets(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()
	hourA := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	hourB := time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC)
	now := hourB.Add(2 * time.Hour)

	// Orphan in hour=14 — must NOT be affected when reconciling hour=13.
	neighbor := ExportObjectKey("events", 0, hourB, 0, 99, 1, "events/0/0-99-1.segment|epoch=1")
	plantDataObject(fs, neighbor)

	orphanInTarget := ExportObjectKey("events", 0, hourA, 0, 99, 1, "events/0/0-99-1.segment|epoch=1")
	plantDataObject(fs, orphanInTarget)

	removed, err := store.ReconcileBucket(ctx, "events", "2026-04-11", "13", now, 15*time.Minute)
	if err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := fs.Get(ctx, neighbor); err != nil {
		t.Fatalf("neighbor bucket was touched: %v", err)
	}
	if _, err := fs.Get(ctx, orphanInTarget); !errors.Is(err, ErrNotFound) {
		t.Fatalf("target bucket orphan survived: %v", err)
	}
}

// ---- Failure injection: CompactBucket CAS races ----

// TestCompactBucketCASConflictThenCallerRetryConverges covers the
// reviewer's failure-injection case where a compaction's ConditionalPut
// on the manifest loses the CAS race. The caller must retry with the
// same (removeKeys, addEntries) arguments; CompactBucket re-reads the
// fresh manifest at the top of each call and converges to the correct
// target set.
func TestCompactBucketCASConflictThenCallerRetryConverges(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()

	// Initial manifest: {kA, kB} at gen=1.
	topic, partition, date, hour := "events", 0, "2026-04-11", "13"
	kA, kB := "parquet/a.parquet", "parquet/b.parquet"
	if _, err := store.PublishManifest(ctx, Manifest{
		Topic: topic, Partition: partition, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []Entry{
			{ObjectKey: kA, BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "test/a", SourceEpoch: 0},
			{ObjectKey: kB, BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "test/b", SourceEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	// Arrange for the first ConditionalPut on the manifest key to fail.
	manifestKey := ManifestKeyForBucket(topic, partition, date, hour)
	fs.injectConditionalPutConflict(manifestKey, 1)

	kD := "parquet/d.parquet"
	compactArgs := func() (Manifest, error) {
		return store.CompactBucket(ctx, topic, partition, date, hour,
			[]string{kA},
			[]Entry{{ObjectKey: kD, BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "compaction/test/d", SourceEpoch: 0}})
	}

	// First attempt: must surface an ErrConflict-wrapped error.
	if _, err := compactArgs(); err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("first compact: err = %v, want ErrConflict", err)
	}
	// Manifest must be unchanged at gen=1.
	if m, err := store.GetManifest(ctx, topic, partition, time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("get after conflict: %v", err)
	} else if m.Generation != 1 {
		t.Fatalf("gen after conflict = %d, want 1 (no partial write)", m.Generation)
	}

	// Retry with identical args — the fault is one-shot.
	final, err := compactArgs()
	if err != nil {
		t.Fatalf("retry compact: %v", err)
	}
	if final.Generation != 2 {
		t.Fatalf("retry gen = %d, want 2", final.Generation)
	}
	gotKeys := make(map[string]bool, len(final.Entries))
	for _, e := range final.Entries {
		gotKeys[e.ObjectKey] = true
	}
	if !gotKeys[kB] || !gotKeys[kD] {
		t.Fatalf("final entries = %+v, want {kB, kD}", final.Entries)
	}
	if gotKeys[kA] {
		t.Fatalf("final entries still contain removed kA: %+v", final.Entries)
	}
}

// TestCompactBucketLostCASLeavesOrphansForReconcile covers the
// reviewer's leakage concern: when a compaction uploads replacement
// data objects and then loses the manifest CAS without retrying (e.g.
// leader death), the uploaded objects become orphans. ReconcileBucket
// must detect and clean them.
func TestCompactBucketLostCASLeavesOrphansForReconcile(t *testing.T) {
	store, fs, _ := newStore()
	ctx := context.Background()

	topic, partition := "events", 0
	ts := time.Date(2026, 4, 11, 13, 0, 0, 0, time.UTC)
	date, hour := BucketDateHour(ts)

	// Seed manifest with a real data reference, and plant the data
	// object so reconcile wouldn't flag it as orphaned.
	kA := ExportObjectKey(topic, partition, ts, 0, 9, 1, "events/0/0-9-1.segment|epoch=1")
	plantDataObject(fs, kA)
	if _, err := store.PublishManifest(ctx, Manifest{
		Topic: topic, Partition: partition, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []Entry{{ObjectKey: kA, BaseOffset: 0, EndOffset: 9, SchemaVersion: 1, SourceKey: "events/0/0-9-1.segment", SourceEpoch: 1}},
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	// Simulate the caller having already uploaded its replacement blob.
	kD := ExportObjectKey(topic, partition, ts, 10, 19, 1, "events/0/10-19-1.segment|epoch=1")
	plantDataObject(fs, kD)

	// Force a permanent CAS conflict on the manifest; the caller does
	// NOT retry, modeling leader death between upload and publish.
	manifestKey := ManifestKeyForBucket(topic, partition, date, hour)
	fs.injectConditionalPutConflict(manifestKey, 100)

	_, err := store.CompactBucket(ctx, topic, partition, date, hour,
		[]string{kA},
		[]Entry{{ObjectKey: kD, BaseOffset: 10, EndOffset: 19, SchemaVersion: 1, SourceKey: "compaction/events/0/10-19", SourceEpoch: 0}})
	if err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("compact: err = %v, want ErrConflict", err)
	}

	// kD is on S3 but not in any manifest → orphan. Reconcile the
	// bucket (after the age fence has passed) and it must be deleted.
	now := ts.Add(2 * time.Hour)
	removed, err := store.ReconcileBucket(ctx, topic, date, hour, now, 15*time.Minute)
	if err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1 (kD orphan)", removed)
	}
	if _, err := fs.Get(ctx, kD); !errors.Is(err, ErrNotFound) {
		t.Fatalf("kD orphan survived reconcile: %v", err)
	}
	if _, err := fs.Get(ctx, kA); err != nil {
		t.Fatalf("kA (referenced) deleted: %v", err)
	}
}
