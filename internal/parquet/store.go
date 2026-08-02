package parquet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ObjectStore is the minimal object-storage contract the Parquet
// metadata layer needs. Implementations must translate backend-specific
// errors into this package's ErrNotFound / ErrConflict via error wrapping
// (errors.Is is used by callers).
//
// This is deliberately smaller than storage.S3Client — the package does
// no direct Put without ETag preconditions, so unconditional writes and
// PutOpts are not in the interface.
type ObjectStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetWithETag(ctx context.Context, key string) ([]byte, string, error)
	ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// Fencer reports whether Parquet writes for a topic must be refused
// because topic deletion is in progress. A minimal seam so the parquet
// package does not need to know about server-side topic lifecycle state.
type Fencer interface {
	TopicDeletionPending(ctx context.Context, topic string) bool
}

// NoFencer is a Fencer that never fences. Use in tooling or tests.
type NoFencer struct{}

func (NoFencer) TopicDeletionPending(context.Context, string) bool { return false }

// Store is the Parquet metadata layer. It is safe to use from multiple
// goroutines as long as the underlying ObjectStore is.
type Store struct {
	objects ObjectStore
	fencer  Fencer
}

// NewStore constructs a Store. If fencer is nil, a NoFencer is used.
func NewStore(objects ObjectStore, fencer Fencer) *Store {
	if fencer == nil {
		fencer = NoFencer{}
	}
	return &Store{objects: objects, fencer: fencer}
}

func (s *Store) canPublish(ctx context.Context, topic string) error {
	if s.fencer.TopicDeletionPending(ctx, topic) {
		return fmt.Errorf("%w: topic %q is deleting", ErrFenced, topic)
	}
	return nil
}

// validateEntrySources enforces the provenance required for every durable
// manifest entry. SourceEpoch deliberately permits zero: the initial leader
// epoch is a valid source identity.
func validateEntrySources(entries []Entry) error {
	for _, entry := range entries {
		if strings.TrimSpace(entry.SourceKey) == "" {
			return fmt.Errorf("parquet entry %q is missing source_key", entry.ObjectKey)
		}
	}
	return nil
}

// GetManifest loads the manifest for the bucket containing ingestTime.
func (s *Store) GetManifest(ctx context.Context, topic string, partition int, ingestTime time.Time) (Manifest, error) {
	key := ManifestKey(topic, partition, ingestTime)
	data, err := s.objects.Get(ctx, key)
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("unmarshal parquet manifest %q: %w", key, err)
	}
	return m, nil
}

// PublishManifest writes a manifest for one bucket using CAS. It is
// idempotent on retry: if the incoming entry set is already a subset of
// what is stored, the existing manifest is returned unchanged (no
// generation bump). This covers the crash-between-upload-and-checkpoint
// path — the export job's retry looks indistinguishable from a fresh
// first-success.
func (s *Store) PublishManifest(ctx context.Context, manifest Manifest) (Manifest, error) {
	if err := validateEntrySources(manifest.Entries); err != nil {
		return Manifest{}, err
	}
	if err := s.canPublish(ctx, manifest.Topic); err != nil {
		return Manifest{}, err
	}
	if manifest.Date == "" || manifest.Hour == "" {
		return Manifest{}, fmt.Errorf("parquet manifest bucket is required")
	}

	sortEntries(manifest.Entries)

	key := ManifestKeyForBucket(manifest.Topic, manifest.Partition, manifest.Date, manifest.Hour)
	currentData, currentETag, err := s.objects.GetWithETag(ctx, key)
	switch {
	case err == nil:
		var current Manifest
		if err := json.Unmarshal(currentData, &current); err != nil {
			return Manifest{}, fmt.Errorf("unmarshal parquet manifest %q: %w", key, err)
		}
		if entriesContain(current.Entries, manifest.Entries) {
			return current, nil
		}
		manifest.Generation = current.Generation + 1
	case errors.Is(err, ErrNotFound):
		manifest.Generation = 1
		currentETag = ""
	default:
		return Manifest{}, fmt.Errorf("get parquet manifest %q: %w", key, err)
	}

	manifest.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, fmt.Errorf("marshal parquet manifest %q: %w", key, err)
	}
	if _, err := s.objects.ConditionalPut(ctx, key, data, currentETag); err != nil {
		return Manifest{}, fmt.Errorf("conditional put parquet manifest %q: %w", key, err)
	}
	return manifest, nil
}

// CompactBucket atomically swaps small Parquet entries in a bucket for
// larger replacement entries.
//
//   - removeKeys: object keys of files that should no longer appear in
//     the manifest. Data objects are NOT deleted here; the caller deletes
//     them only after the new manifest is durably published.
//   - addEntries: replacement entries (typically one big file that covers
//     the offset span of the removed inputs).
//
// Idempotency: a compaction job that crashed after publishing the new
// manifest but before the caller deleted the old data objects will
// replay safely. Calling CompactBucket twice with the same arguments is
// a no-op the second time — the function detects that the state-to-publish
// already equals the stored state and returns the current manifest
// unchanged.
//
// Partial crash recovery: if some of removeKeys were already removed
// by a prior attempt (and the addEntries were already present), the
// function still converges to the correct target set without error.
func (s *Store) CompactBucket(ctx context.Context, topic string, partition int, date, hour string, removeKeys []string, addEntries []Entry) (Manifest, error) {
	if err := validateEntrySources(addEntries); err != nil {
		return Manifest{}, err
	}
	if err := s.canPublish(ctx, topic); err != nil {
		return Manifest{}, err
	}
	if date == "" || hour == "" {
		return Manifest{}, fmt.Errorf("parquet manifest bucket is required")
	}

	key := ManifestKeyForBucket(topic, partition, date, hour)
	currentData, currentETag, err := s.objects.GetWithETag(ctx, key)
	var current Manifest
	switch {
	case err == nil:
		if err := json.Unmarshal(currentData, &current); err != nil {
			return Manifest{}, fmt.Errorf("unmarshal parquet manifest %q: %w", key, err)
		}
	case errors.Is(err, ErrNotFound):
		current = Manifest{Topic: topic, Partition: partition, Date: date, Hour: hour}
		currentETag = ""
	default:
		return Manifest{}, fmt.Errorf("get parquet manifest %q: %w", key, err)
	}

	removeSet := make(map[string]struct{}, len(removeKeys))
	for _, k := range removeKeys {
		removeSet[k] = struct{}{}
	}
	targetByKey := make(map[string]Entry, len(current.Entries)+len(addEntries))
	for _, e := range current.Entries {
		if _, drop := removeSet[e.ObjectKey]; drop {
			continue
		}
		targetByKey[e.ObjectKey] = e
	}
	for _, e := range addEntries {
		targetByKey[e.ObjectKey] = e
	}
	target := make([]Entry, 0, len(targetByKey))
	for _, e := range targetByKey {
		target = append(target, e)
	}
	sortEntries(target)
	if err := validateEntrySources(target); err != nil {
		return Manifest{}, err
	}

	if entrySetsEqual(current.Entries, target) {
		return current, nil
	}

	next := Manifest{
		Generation:    current.Generation + 1,
		Topic:         topic,
		Partition:     partition,
		Date:          date,
		Hour:          hour,
		SchemaVersion: current.SchemaVersion,
		Entries:       target,
		UpdatedAt:     time.Now().UTC(),
	}
	for _, e := range addEntries {
		if e.SchemaVersion > next.SchemaVersion {
			next.SchemaVersion = e.SchemaVersion
		}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return Manifest{}, fmt.Errorf("marshal parquet manifest %q: %w", key, err)
	}
	if _, err := s.objects.ConditionalPut(ctx, key, data, currentETag); err != nil {
		return Manifest{}, fmt.Errorf("conditional put parquet manifest %q: %w", key, err)
	}
	return next, nil
}

// ReplaceOverlappingEntries atomically installs addEntries and removes every
// existing entry whose inclusive offset range intersects one of them. This is
// used when a newer authoritative native segment supersedes a divergent
// export that used the same offsets. Non-overlapping entries are preserved.
//
// Repeating the same replacement is idempotent. A bounded CAS retry handles
// concurrent manifest updates without ever publishing a partial target set.
func (s *Store) ReplaceOverlappingEntries(ctx context.Context, topic string, partition int, date, hour string, addEntries []Entry) (Manifest, error) {
	if err := validateEntrySources(addEntries); err != nil {
		return Manifest{}, err
	}
	if err := s.canPublish(ctx, topic); err != nil {
		return Manifest{}, err
	}
	if date == "" || hour == "" {
		return Manifest{}, fmt.Errorf("parquet manifest bucket is required")
	}
	if len(addEntries) == 0 {
		return Manifest{}, fmt.Errorf("parquet replacement requires entries")
	}

	key := ManifestKeyForBucket(topic, partition, date, hour)
	const attempts = 3
	for i := 0; i < attempts; i++ {
		currentData, currentETag, err := s.objects.GetWithETag(ctx, key)
		var current Manifest
		switch {
		case err == nil:
			if err := json.Unmarshal(currentData, &current); err != nil {
				return Manifest{}, fmt.Errorf("unmarshal parquet manifest %q: %w", key, err)
			}
		case errors.Is(err, ErrNotFound):
			current = Manifest{Topic: topic, Partition: partition, Date: date, Hour: hour}
			currentETag = ""
		default:
			return Manifest{}, fmt.Errorf("get parquet manifest %q: %w", key, err)
		}

		targetByKey := make(map[string]Entry, len(current.Entries)+len(addEntries))
		for _, existing := range current.Entries {
			if overlapsAny(existing, addEntries) {
				continue
			}
			targetByKey[existing.ObjectKey] = existing
		}
		for _, entry := range addEntries {
			targetByKey[entry.ObjectKey] = entry
		}
		target := make([]Entry, 0, len(targetByKey))
		for _, entry := range targetByKey {
			target = append(target, entry)
		}
		sortEntries(target)
		if err := validateEntrySources(target); err != nil {
			return Manifest{}, err
		}
		if entrySetsEqual(current.Entries, target) {
			return current, nil
		}

		next := Manifest{Generation: current.Generation + 1, Topic: topic, Partition: partition, Date: date, Hour: hour, SchemaVersion: current.SchemaVersion, Entries: target, UpdatedAt: time.Now().UTC()}
		for _, entry := range addEntries {
			if entry.SchemaVersion > next.SchemaVersion {
				next.SchemaVersion = entry.SchemaVersion
			}
		}
		data, err := json.Marshal(next)
		if err != nil {
			return Manifest{}, fmt.Errorf("marshal parquet manifest %q: %w", key, err)
		}
		if _, err := s.objects.ConditionalPut(ctx, key, data, currentETag); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Manifest{}, fmt.Errorf("conditional put parquet manifest %q: %w", key, err)
		}
		return next, nil
	}
	return Manifest{}, fmt.Errorf("replace parquet manifest %q: CAS conflict after %d attempts", key, attempts)
}

// ListTopicManifests enumerates manifests for a topic whose buckets
// intersect the inclusive range [from, to]. Time filtering is applied to
// each manifest key BEFORE its body is fetched — O(retention-buckets)
// listings do not translate into O(retention-buckets) GETs.
//
// A zero from or to disables that side of the range.
func (s *Store) ListTopicManifests(ctx context.Context, topic string, from, to time.Time) ([]Manifest, error) {
	keys, err := s.objects.List(ctx, ManifestPrefix+topic+"/")
	if err != nil {
		return nil, err
	}
	fromUTC := from.UTC()
	toUTC := to.UTC()
	manifests := make([]Manifest, 0, len(keys))
	for _, key := range keys {
		date, hour, ok := ParseManifestKey(topic, key)
		if !ok {
			continue
		}
		bucketAt, err := time.Parse("2006-01-02 15", date+" "+hour)
		if err != nil {
			return nil, fmt.Errorf("parse parquet manifest bucket %q: %w", key, err)
		}
		bucketAt = bucketAt.UTC()
		bucketEnd := bucketAt.Add(time.Hour)
		if !from.IsZero() && !bucketEnd.After(fromUTC) {
			continue
		}
		if !to.IsZero() && bucketAt.After(toUTC) {
			continue
		}

		data, err := s.objects.Get(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("unmarshal parquet manifest %q: %w", key, err)
		}
		manifests = append(manifests, m)
	}
	slices.SortFunc(manifests, func(a, b Manifest) int {
		if a.Date == b.Date {
			return cmpString(a.Hour, b.Hour)
		}
		return cmpString(a.Date, b.Date)
	})
	return manifests, nil
}

// DeleteTopicMetadata removes all Parquet-related state for a topic:
// manifests, the query catalog entry, and the
// underlying Parquet data objects under DataPrefix.
//
// Metadata is deleted before data so that a concurrent query which
// already observed a manifest cannot end up pointing at a deleted data
// object. The caller must mark the topic as deletion-pending via the
// Fencer BEFORE invoking this — the fence blocks in-flight export jobs
// from republishing state that this routine is about to erase.
func (s *Store) DeleteTopicMetadata(ctx context.Context, topic string) error {
	for _, prefix := range []string{
		ManifestPrefix + topic + "/",
		BucketIndexPrefix + topic + "/",
	} {
		keys, err := s.objects.List(ctx, prefix)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := s.objects.Delete(ctx, key); err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
		}
	}
	dataKeys, err := s.objects.List(ctx, DataPrefix)
	if err != nil {
		return err
	}
	topicMarker := "/topic=" + topic + "/"
	for _, key := range dataKeys {
		if !strings.Contains(key, topicMarker) {
			continue
		}
		if err := s.objects.Delete(ctx, key); err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	if err := s.objects.Delete(ctx, QueryCatalogTopicKey(topic)); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// ReconcileBucket removes Parquet data objects in a (topic, date, hour)
// bucket that are not referenced by any partition's manifest for that
// bucket. It exists to clean up objects left behind by a job that
// uploaded data and then lost (or never attempted) the manifest CAS —
// without this, such losers accumulate in S3 and are never otherwise
// collected.
//
// Data layout is topic-scoped, not partition-scoped: all partitions
// share one DataBucketPrefix(topic, date, hour). Reconciliation unions
// referenced object keys across every partition's manifest for the
// bucket before diffing.
//
// Age fence: the function refuses to act on a bucket whose hour end
// (date + hour + 1h) is less than minBucketAge before `now`. This
// avoids a race with an in-flight upload whose manifest PUT has not
// yet landed — the uploaded object would otherwise be misread as an
// orphan. A small but non-zero minBucketAge (e.g. 15–60 minutes) gives
// any sequential export pass ample time to publish.
//
// Fenced by topic deletion: if TopicDeletionPending returns true, the
// function returns ErrFenced so DeleteTopicMetadata (which deletes the
// whole topic prefix) is the single authority during cleanup.
//
// Returns the number of data objects deleted.
func (s *Store) ReconcileBucket(ctx context.Context, topic, date, hour string, now time.Time, minBucketAge time.Duration) (int, error) {
	if err := s.canPublish(ctx, topic); err != nil {
		return 0, err
	}
	if date == "" || hour == "" {
		return 0, fmt.Errorf("parquet reconcile bucket: date and hour are required")
	}
	bucketAt, err := time.Parse("2006-01-02 15", date+" "+hour)
	if err != nil {
		return 0, fmt.Errorf("parquet reconcile bucket %s/%s: parse time: %w", date, hour, err)
	}
	bucketEnd := bucketAt.UTC().Add(time.Hour)
	if now.UTC().Sub(bucketEnd) < minBucketAge {
		return 0, nil
	}

	manifestKeys, err := s.objects.List(ctx, ManifestBucketPrefix(topic, date, hour))
	if err != nil {
		return 0, fmt.Errorf("list manifests for bucket %s/%s: %w", date, hour, err)
	}
	referenced := make(map[string]struct{})
	for _, key := range manifestKeys {
		data, err := s.objects.Get(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return 0, fmt.Errorf("get manifest %q: %w", key, err)
		}
		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return 0, fmt.Errorf("unmarshal manifest %q: %w", key, err)
		}
		for _, entry := range m.Entries {
			referenced[entry.ObjectKey] = struct{}{}
		}
	}

	dataKeys, err := s.objects.List(ctx, DataBucketPrefix(topic, date, hour))
	if err != nil {
		return 0, fmt.Errorf("list data for bucket %s/%s: %w", date, hour, err)
	}
	removed := 0
	for _, key := range dataKeys {
		if _, ok := referenced[key]; ok {
			continue
		}
		// Re-check immediately before every destructive operation. Topic
		// deletion may begin after listing/manifests were read; once fenced,
		// stop without touching any later objects.
		if err := s.canPublish(ctx, topic); err != nil {
			return removed, err
		}
		if err := s.objects.Delete(ctx, key); err != nil && !errors.Is(err, ErrNotFound) {
			return removed, fmt.Errorf("delete orphan %q: %w", key, err)
		}
		removed++
	}
	return removed, nil
}

// ResolveEntriesForTime returns the entries of the manifest whose bucket
// contains ingestTime, along with the manifest itself.
func (s *Store) ResolveEntriesForTime(ctx context.Context, topic string, partition int, ingestTime time.Time) ([]Entry, Manifest, error) {
	m, err := s.GetManifest(ctx, topic, partition, ingestTime)
	if err != nil {
		return nil, Manifest{}, err
	}
	return append([]Entry(nil), m.Entries...), m, nil
}

// ---- helpers ----

func sortEntries(entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int {
		switch {
		case a.BaseOffset < b.BaseOffset:
			return -1
		case a.BaseOffset > b.BaseOffset:
			return 1
		default:
			return 0
		}
	})
}

func overlapsAny(entry Entry, candidates []Entry) bool {
	for _, candidate := range candidates {
		if entry.BaseOffset <= candidate.EndOffset && candidate.BaseOffset <= entry.EndOffset {
			return true
		}
	}
	return false
}

// entriesContain reports whether `superset` contains every entry in
// `subset`, keyed by object key + offset range.
func entriesContain(superset, subset []Entry) bool {
	if len(subset) == 0 {
		return true
	}
	index := make(map[string]Entry, len(superset))
	for _, e := range superset {
		index[e.ObjectKey] = e
	}
	for _, e := range subset {
		existing, ok := index[e.ObjectKey]
		if !ok {
			return false
		}
		if existing.BaseOffset != e.BaseOffset || existing.EndOffset != e.EndOffset {
			return false
		}
	}
	return true
}

func entrySetsEqual(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	index := make(map[string]Entry, len(a))
	for _, e := range a {
		index[e.ObjectKey] = e
	}
	for _, e := range b {
		existing, ok := index[e.ObjectKey]
		if !ok {
			return false
		}
		if existing.BaseOffset != e.BaseOffset || existing.EndOffset != e.EndOffset {
			return false
		}
	}
	return true
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
