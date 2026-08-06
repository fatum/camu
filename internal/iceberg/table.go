package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/maksim/camu/internal/meta"
)

// TableStore owns a single Iceberg table (one camu topic) stored under a
// warehouse prefix. Metadata commits follow the Iceberg file-catalog
// convention: immutable {version}-{uuid}.metadata.json files plus a
// version-hint.text pointer advanced by a conditional write. Multiple
// partition leaders commit to the same table with optimistic concurrency:
// a lost version-hint CAS is retried against freshly loaded metadata.
type TableStore struct {
	objects   ObjectStore
	fencer    Fencer
	warehouse string
}

// DefaultWarehouse is the object-store prefix Iceberg tables live under when
// no warehouse is configured.
const DefaultWarehouse = "warehouse/"

// NewTableStore creates a TableStore rooted at warehouse. A nil fencer behaves
// like NoFencer.
func NewTableStore(objects ObjectStore, fencer Fencer, warehouse string) *TableStore {
	if fencer == nil {
		fencer = NoFencer{}
	}
	if warehouse == "" {
		warehouse = DefaultWarehouse
	}
	return &TableStore{objects: objects, fencer: fencer, warehouse: strings.TrimSuffix(warehouse, "/") + "/"}
}

func sanitizeTable(topic string) string {
	return strings.ReplaceAll(topic, "/", "_")
}

func (ts *TableStore) tableRoot(topic string) string {
	return ts.warehouse + sanitizeTable(topic) + "/"
}

func (ts *TableStore) metadataDir(topic string) string {
	return ts.tableRoot(topic) + "metadata/"
}

func (ts *TableStore) dataDir(topic string) string {
	return ts.tableRoot(topic) + "data/"
}

func (ts *TableStore) versionHintKey(topic string) string {
	return ts.metadataDir(topic) + "version-hint.text"
}

// metadataFileKey returns an immutable metadata-file key for a version. The
// UUID is fresh per commit (as in Iceberg's HadoopTableOperations), so a
// writer that loses the version-hint CAS leaves a harmless orphan instead of
// blocking the next attempt with a create-if-not-exists conflict; readers
// match the current version by the numeric prefix.
func (ts *TableStore) metadataFileKey(topic string, version int) string {
	return fmt.Sprintf("%s%05d-%s.metadata.json", ts.metadataDir(topic), version, uuid.NewString())
}

// manifestListKey returns the manifest-list (snapshot) Avro file key.
func (ts *TableStore) manifestListKey(topic string, snapshotID int64, attempt, manifestCount int) string {
	return fmt.Sprintf("%ssnap-%d-%d-%d.avro", ts.metadataDir(topic), snapshotID, attempt, manifestCount)
}

// manifestKey returns one manifest Avro file key for a snapshot.
func (ts *TableStore) manifestKey(topic string, snapshotID int64, manifestCount int) string {
	return fmt.Sprintf("%s%d-m%d.avro", ts.metadataDir(topic), snapshotID, manifestCount)
}

// dataFileKey returns the content-addressed data-file key under the table's
// data directory. id must identify the file (see exportFileID).
func (ts *TableStore) dataFileKey(topic, id string) string {
	return ts.dataDir(topic) + id + ".parquet"
}

// Create initializes a new table with no snapshots. It fails with ErrConflict
// if the table already exists.
func (ts *TableStore) Create(ctx context.Context, topic string, topicSchema *meta.TopicSchema) (*TableMetadata, error) {
	if ts.fencer.TopicDeletionPending(ctx, topic) {
		return nil, fmt.Errorf("%w: topic %q is deleting", ErrFenced, topic)
	}
	md := NewTableMetadata(ts.tableRoot(topic), topicSchema)
	md.version = 0
	encoded, err := json.Marshal(md)
	if err != nil {
		return nil, err
	}
	if _, err := ts.objects.ConditionalPut(ctx, ts.metadataFileKey(topic, 0), encoded, ""); err != nil {
		return nil, fmt.Errorf("create iceberg table %q: %w", topic, err)
	}
	if _, err := ts.objects.ConditionalPut(ctx, ts.versionHintKey(topic), []byte("0"), ""); err != nil {
		return nil, fmt.Errorf("create iceberg table %q version hint: %w", topic, err)
	}
	return md, nil
}

// Load returns the current table metadata, or ErrNotFound when the table has
// not been created.
func (ts *TableStore) Load(ctx context.Context, topic string) (*TableMetadata, error) {
	hint, err := ts.objects.Get(ctx, ts.versionHintKey(topic))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errors.Join(err, fmt.Errorf("iceberg table %q does not exist", topic))
		}
		return nil, fmt.Errorf("read iceberg version hint %q: %w", topic, err)
	}
	version, err := strconv.Atoi(strings.TrimSpace(string(hint)))
	if err != nil {
		return nil, fmt.Errorf("invalid iceberg version hint %q: %w", topic, err)
	}
	key, err := ts.metadataFileKeyForVersion(ctx, topic, version)
	if err != nil {
		return nil, err
	}
	encoded, err := ts.objects.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read iceberg table metadata %q: %w", topic, err)
	}
	var md TableMetadata
	if err := json.Unmarshal(encoded, &md); err != nil {
		return nil, fmt.Errorf("parse iceberg table metadata %q: %w", topic, err)
	}
	md.version = version
	if err := md.validate(); err != nil {
		return nil, fmt.Errorf("iceberg table %q: %w", topic, err)
	}
	return &md, nil
}

// metadataFileKeyForVersion finds the metadata file for a version by listing
// the metadata directory, matching the version-hint convention readers use.
func (ts *TableStore) metadataFileKeyForVersion(ctx context.Context, topic string, version int) (string, error) {
	prefix := fmt.Sprintf("%s%05d-", ts.metadataDir(topic), version)
	keys, err := ts.objects.List(ctx, prefix)
	if err != nil {
		return "", fmt.Errorf("list iceberg metadata %q: %w", topic, err)
	}
	for _, key := range keys {
		if strings.HasSuffix(key, ".metadata.json") {
			return key, nil
		}
	}
	return "", errors.Join(ErrNotFound, fmt.Errorf("iceberg table %q metadata version %d not found", topic, version))
}

// AppendSnapshot commits a new snapshot whose manifest list is stored at
// manifestListKey, retrying on version-hint CAS conflicts. The snapshot id is
// derived from the manifest list key, so a retry of the same commit is
// idempotent: if a snapshot for that manifest list is already current, it is
// returned without a new commit.
func (ts *TableStore) AppendSnapshot(ctx context.Context, topic string, manifestListKey string, summary SnapshotSummary) (*Snapshot, error) {
	snapshotID := snapshotIDFor(manifestListKey)
	for attempt := 0; ; attempt++ {
		current, err := ts.Load(ctx, topic)
		if err != nil {
			return nil, err
		}
		if existing := current.snapshotByID(snapshotID); existing != nil {
			return existing, nil // idempotent retry
		}
		next := current.clone()
		snap := &Snapshot{
			SnapshotID:     snapshotID,
			SequenceNumber: current.nextSequenceNumber(),
			TimestampMS:    time.Now().UnixMilli(),
			ManifestList:   manifestListKey,
			Summary:        summary,
		}
		next.Snapshots = append(next.Snapshots, snap)
		next.CurrentSnapshotID = &snap.SnapshotID
		next.Refs["main"] = SnapshotRef{SnapshotID: snap.SnapshotID, Type: "branch"}
		next.SnapshotLog = append(next.SnapshotLog, SnapshotLogEntry{SnapshotID: snap.SnapshotID, TimestampMS: snap.TimestampMS})
		next.LastUpdatedMS = snap.TimestampMS
		committed, err := ts.commitCAS(ctx, topic, current, next)
		if err == nil {
			return committed.snapshotByID(snapshotID), nil
		}
		if !errors.Is(err, ErrConflict) {
			return nil, err
		}
		if attempt >= maxTableCommitAttempts {
			return nil, fmt.Errorf("commit iceberg snapshot %q: CAS conflict after %d attempts", topic, attempt+1)
		}
	}
}

const maxTableCommitAttempts = 6

// commitCAS writes next as the new metadata version (current.version+1) and
// atomically advances the version hint. It returns ErrConflict when a
// concurrent writer won, in which case the caller must reload and reapply.
func (ts *TableStore) commitCAS(ctx context.Context, topic string, current, next *TableMetadata) (*TableMetadata, error) {
	if ts.fencer.TopicDeletionPending(ctx, topic) {
		return nil, fmt.Errorf("%w: topic %q is deleting", ErrFenced, topic)
	}
	nextVersion := current.version + 1
	next.version = nextVersion
	next.LastUpdatedMS = time.Now().UnixMilli()
	encoded, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	fileKey := ts.metadataFileKey(topic, nextVersion)
	if _, err := ts.objects.ConditionalPut(ctx, fileKey, encoded, ""); err != nil {
		return nil, fmt.Errorf("write iceberg metadata %q: %w", fileKey, err)
	}
	hintKey := ts.versionHintKey(topic)
	_, hintETag, err := ts.objects.GetWithETag(ctx, hintKey)
	if err != nil {
		return nil, fmt.Errorf("read iceberg version hint %q: %w", topic, err)
	}
	if _, err := ts.objects.ConditionalPut(ctx, hintKey, []byte(strconv.Itoa(nextVersion)), hintETag); err != nil {
		// Best-effort removal of the just-written metadata file: it is the
		// loser of a concurrent commit and would otherwise linger as an orphan
		// at the winning version, where a reader listing by version prefix
		// could pick it. A reader never observed it while it was current: the
		// version hint still pointed at the previous version.
		_ = ts.objects.Delete(ctx, fileKey)
		return nil, fmt.Errorf("commit iceberg version hint %q: %w", topic, err)
	}
	return next, nil
}

// DeleteTable removes the entire table (metadata, manifest lists, manifests,
// and data) plus the version hint.
func (ts *TableStore) DeleteTable(ctx context.Context, topic string) error {
	keys, err := ts.objects.List(ctx, ts.tableRoot(topic))
	if err != nil {
		return fmt.Errorf("list iceberg table %q: %w", topic, err)
	}
	if len(keys) > 0 {
		if err := ts.objects.DeleteMany(ctx, keys); err != nil {
			return fmt.Errorf("delete iceberg table %q: %w", topic, err)
		}
	}
	return nil
}

// snapshotIDFor derives a stable snapshot id from the manifest list key so a
// retried commit converges on the same snapshot.
func snapshotIDFor(key string) int64 {
	sum := sha256.Sum256([]byte(key))
	return int64(binary.BigEndian.Uint64(sum[:8]) & (1<<63 - 1))
}
