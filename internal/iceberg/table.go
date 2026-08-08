package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

// sanitizeTable maps a topic name to a safe, injective table name so distinct
// topics never share a table root. Underscores are doubled and slashes become
// a single underscore (see sanitizeSchemaTopic in the server's schema
// registry for the same scheme), so a_b and a/b map to different tables.
func sanitizeTable(topic string) string {
	if !strings.ContainsAny(topic, "_/") {
		return topic
	}
	var b strings.Builder
	for i := 0; i < len(topic); i++ {
		switch topic[i] {
		case '_':
			b.WriteString("__")
		case '/':
			b.WriteByte('_')
		default:
			b.WriteByte(topic[i])
		}
	}
	return b.String()
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

// dataFileKey returns the content-addressed data-file key under the table's
// data directory, laid out by the dt/hour partition values. id must identify
// the file (see exportFileID).
func (ts *TableStore) dataFileKey(topic, id, dt string, hour int) string {
	return fmt.Sprintf("%sdt=%s/hour=%02d/%s.parquet", ts.dataDir(topic), dt, hour, id)
}

// ExportDataFileKey returns the deterministic data-file key for one exported
// source range. ingestTime is the partition leader's segment-flush time (see
// ExportObjectKey) and sourceIdentity must identify the immutable native
// segment being exported; together they make retries converge on one object.
func (ts *TableStore) ExportDataFileKey(topic string, partition int, ingestTime time.Time, base, end int64, sourceIdentity string) string {
	dt, hour := BucketDateHour(ingestTime)
	hourInt, _ := strconv.Atoi(hour)
	id := exportFileID(topic, partition, dt, hour, base, end, 1, sourceIdentity)
	return ts.dataFileKey(topic, id, dt, hourInt)
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
	fileKey := ts.metadataFileKey(topic, 0)
	if _, err := ts.objects.ConditionalPut(ctx, fileKey, encoded, ""); err != nil {
		return nil, fmt.Errorf("create iceberg table %q: %w", topic, err)
	}
	if _, err := ts.objects.ConditionalPut(ctx, ts.versionHintKey(topic), []byte("0"), ""); err != nil {
		return nil, fmt.Errorf("create iceberg table %q version hint: %w", topic, err)
	}
	md.metadataKey = fileKey
	return md, nil
}

// Load returns the current table metadata, or ErrNotFound when the table has
// not been created. When the version-hint points to a non-existent metadata
// file, it scans backwards to self-heal from a CAS that succeeded server-side
// while the client deleted the metadata file.
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
		if !isMetadataVersionNotFound(err) {
			return nil, err
		}
		// Version-hint points to a version whose metadata file was lost
		// (CAS race). Scan backwards to find the last reachable version.
		for version > 0 {
			version--
			key, err = ts.metadataFileKeyForVersion(ctx, topic, version)
			if err == nil {
				break
			}
			if !isMetadataVersionNotFound(err) {
				return nil, err
			}
		}
		if version <= 0 && err != nil {
			return nil, fmt.Errorf("iceberg table %q: no reachable metadata version found", topic)
		}
		// Self-heal the version hint. A blank etag makes this a best-effort
		// overwrite; a concurrent commit may advance it further.
		_, _ = ts.objects.ConditionalPut(ctx, ts.versionHintKey(topic), []byte(strconv.Itoa(version)), "")
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
	md.metadataKey = key
	if err := md.validate(); err != nil {
		return nil, fmt.Errorf("iceberg table %q: %w", topic, err)
	}
	return &md, nil
}

func isMetadataVersionNotFound(err error) bool {
	return errors.Is(err, ErrNotFound) && strings.Contains(err.Error(), "metadata version")
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

// maxManifestsPerSnapshot bounds the number of manifests a snapshot's manifest
// list carries before a commit merges them into one (minor compaction), so the
// manifest list never grows without bound under sustained snapshot commits.
const maxManifestsPerSnapshot = 8

// CommitSnapshot appends the given data files to the table as a new snapshot.
// It writes the manifest referencing the files, the manifest list (carrying
// the parent snapshot's manifests forward, or merging them when the list would
// exceed maxManifestsPerSnapshot), and commits the metadata with the
// version-hint CAS. The snapshot id is derived from the file set, so a retry
// of the same commit is idempotent: if a snapshot for those files is already
// committed it is returned without a new commit. Manifest and manifest-list
// keys are content-addressed, so retries after a concurrent commit never
// collide on an existing object.
func (ts *TableStore) CommitSnapshot(ctx context.Context, topic string, files []DataFile) (*Snapshot, error) {
	snapshotID := snapshotIDForFiles(files)
	return ts.commitSnapshot(ctx, topic, snapshotID, func(current *TableMetadata) (string, SnapshotSummary, *Snapshot, error) {
		seq := current.nextSequenceNumber()
		added := make([]ManifestEntry, 0, len(files))
		var addedRows int64
		for _, f := range files {
			addedRows += f.RecordCount
			added = append(added, ManifestEntry{
				Status:         manifestEntryAdded,
				SnapshotID:     snapshotID,
				SequenceNumber: seq,
				DataFile:       f,
			})
		}
		parentManifests, err := ts.readParentManifestList(ctx, current)
		if err != nil {
			return "", nil, nil, err
		}
		var entries []ManifestEntry
		var list []ManifestFile
		if len(parentManifests)+1 > maxManifestsPerSnapshot {
			// Minor compaction: merge the parent manifests and the new files
			// into a single manifest so the manifest list stays bounded.
			entries, err = ts.mergeParentManifests(ctx, parentManifests, added)
			if err != nil {
				return "", nil, nil, err
			}
			manifestKey, manifestLen, err := ts.writeManifestFile(ctx, topic, snapshotID, entries)
			if err != nil {
				return "", nil, nil, err
			}
			minSeq := seq
			for _, e := range entries {
				if e.SequenceNumber < minSeq {
					minSeq = e.SequenceNumber
				}
			}
			list = []ManifestFile{{
				ManifestPath:       manifestKey,
				ManifestLength:     manifestLen,
				PartitionSpecID:    current.DefaultSpecID,
				Content:            DataFileContentData,
				SequenceNumber:     seq,
				MinSequenceNumber:  minSeq,
				AddedSnapshotID:    snapshotID,
				AddedFilesCount:    len(files),
				ExistingFilesCount: len(entries) - len(files),
				AddedRowsCount:     addedRows,
				Partitions:         partitionSummariesFor(entryFiles(entries)),
			}}
		} else {
			manifestKey, manifestLen, err := ts.writeManifestFile(ctx, topic, snapshotID, added)
			if err != nil {
				return "", nil, nil, err
			}
			list = append(list, parentManifests...)
			list = append(list, ManifestFile{
				ManifestPath:      manifestKey,
				ManifestLength:    manifestLen,
				PartitionSpecID:   current.DefaultSpecID,
				Content:           DataFileContentData,
				SequenceNumber:    seq,
				MinSequenceNumber: seq,
				AddedSnapshotID:   snapshotID,
				AddedFilesCount:   len(files),
				AddedRowsCount:    addedRows,
				Partitions:        partitionSummariesFor(files),
			})
		}
		listKey, _, err := ts.writeManifestListFile(ctx, topic, snapshotID, list)
		if err != nil {
			return "", nil, nil, err
		}
		summary := SnapshotSummary{
			"added-data-files": strconv.Itoa(len(files)),
			"added-records":    strconv.FormatInt(addedRows, 10),
		}
		return listKey, summary, &Snapshot{
			SnapshotID:     snapshotID,
			SequenceNumber: seq,
			TimestampMS:    time.Now().UnixMilli(),
			ManifestList:   listKey,
			Summary:        summary,
			SchemaID:       &current.CurrentSchemaID,
		}, nil
	})
}

// EnsureSchema advances the table to the given topic schema version when it is
// newer than the table's current schema, appending the new Iceberg schema with
// stable column ids for fields already present. It returns the current table
// metadata (unchanged when no evolution is needed).
func (ts *TableStore) EnsureSchema(ctx context.Context, topic string, topicSchema *meta.TopicSchema, schemaID int) (*TableMetadata, error) {
	for attempt := 0; ; attempt++ {
		current, err := ts.Load(ctx, topic)
		if err != nil {
			return nil, err
		}
		if current.CurrentSchemaID >= schemaID {
			return current, nil
		}
		next := current.clone()
		next.Schemas = append(next.Schemas, buildTableSchema(next.currentSchema(), topicSchema, schemaID))
		next.CurrentSchemaID = schemaID
		newest := next.Schemas[len(next.Schemas)-1]
		if last := maxSchemaFieldID(newest.Fields); last > next.LastColumnID {
			next.LastColumnID = last
		}
		committed, err := ts.commitCAS(ctx, topic, current, next)
		if err == nil {
			return committed, nil
		}
		if !errors.Is(err, ErrConflict) {
			return nil, err
		}
		if attempt >= maxTableCommitAttempts {
			return nil, fmt.Errorf("evolve iceberg schema %q: CAS conflict after %d attempts", topic, attempt+1)
		}
	}
}

// AppendSnapshot commits a new snapshot whose manifest list is stored at
// manifestListKey, retrying on version-hint CAS conflicts. Idempotent: if a
// snapshot with the given id is already committed, it is returned unchanged.
func (ts *TableStore) AppendSnapshot(ctx context.Context, topic string, snapshotID int64, manifestListKey string, summary SnapshotSummary) (*Snapshot, error) {
	return ts.commitSnapshot(ctx, topic, snapshotID, func(current *TableMetadata) (string, SnapshotSummary, *Snapshot, error) {
		seq := current.nextSequenceNumber()
		snap := &Snapshot{
			SnapshotID:     snapshotID,
			SequenceNumber: seq,
			TimestampMS:    time.Now().UnixMilli(),
			ManifestList:   manifestListKey,
			Summary:        summary,
		}
		return manifestListKey, summary, snap, nil
	})
}

// commitSnapshot runs a commit with a caller-supplied builder. The builder
// receives the freshly loaded current metadata and returns the manifest list
// key, the summary, and the snapshot to commit; it must derive everything it
// needs from that single loaded state so a snapshot's sequence number matches
// the manifest entries it references. A lost version-hint CAS reloads and
// retries.
func (ts *TableStore) commitSnapshot(ctx context.Context, topic string, snapshotID int64, build func(*TableMetadata) (string, SnapshotSummary, *Snapshot, error)) (*Snapshot, error) {
	for attempt := 0; ; attempt++ {
		current, err := ts.Load(ctx, topic)
		if err != nil {
			return nil, err
		}
		if existing := current.snapshotByID(snapshotID); existing != nil {
			return existing, nil // idempotent retry
		}
		_, _, snap, err := build(current)
		if err != nil {
			return nil, err
		}
		next := current.clone()
		next.Snapshots = append(next.Snapshots, snap)
		next.CurrentSnapshotID = &snap.SnapshotID
		next.LastSequenceNumber = snap.SequenceNumber
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

// metadataLogCap bounds the metadata-log history kept per table (matching
// Iceberg's default metadata.previous-versions-max).
const metadataLogCap = 100

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
	if current.metadataKey != "" {
		next.MetadataLog = append(next.MetadataLog, MetadataLogEntry{TimestampMS: current.LastUpdatedMS, MetadataFile: current.metadataKey})
		if len(next.MetadataLog) > metadataLogCap {
			next.MetadataLog = next.MetadataLog[len(next.MetadataLog)-metadataLogCap:]
		}
	}
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
		// The CAS reported failure, but S3 eventual consistency may mean the
		// write succeeded on the server while the client got an error (network
		// timeout, reset, etc.). Re-read the version hint to confirm. If it
		// advanced to nextVersion or beyond, the CAS actually committed and a
		// concurrent writer may even have advanced it further. Only delete the
		// metadata file when the hint is truly stale.
		_ = ts.objects.Delete(ctx, fileKey)
		if actual, readErr := ts.objects.Get(ctx, hintKey); readErr == nil {
			if actualVersion, _ := strconv.Atoi(strings.TrimSpace(string(actual))); actualVersion >= nextVersion {
				// CAS succeeded despite the error response. The concurrently
				// written metadata file was our best-effort delete victim, but
				// we still have the correct state in next. Reload to pick up
				// the canonical metadata file key.
				reloaded, reloadErr := ts.Load(ctx, topic)
				if reloadErr == nil {
					return reloaded, nil
				}
				return next, nil
			}
		}
		return nil, fmt.Errorf("commit iceberg version hint %q: %w", topic, err)
	}
	// The commit won: remove any sibling metadata file a concurrent loser wrote
	// at this same version, so a reader listing by version prefix can never
	// resolve the wrong (orphaned) file. Best-effort: a failed cleanup leaves a
	// file that describes the same table state.
	ts.removeOrphanMetadataFiles(ctx, topic, nextVersion, fileKey)
	return next, nil
}

// removeOrphanMetadataFiles deletes every metadata file at a version except
// the one the winning commit just wrote.
func (ts *TableStore) removeOrphanMetadataFiles(ctx context.Context, topic string, version int, keep string) {
	prefix := fmt.Sprintf("%s%05d-", ts.metadataDir(topic), version)
	keys, err := ts.objects.List(ctx, prefix)
	if err != nil {
		return
	}
	var stale []string
	for _, key := range keys {
		if strings.HasSuffix(key, ".metadata.json") && key != keep {
			stale = append(stale, key)
		}
	}
	if len(stale) > 0 {
		_ = ts.objects.DeleteMany(ctx, stale)
	}
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

// writeManifestFile encodes entries as an Iceberg manifest and stores it under
// a content-addressed key; it returns the key and the file size in bytes.
func (ts *TableStore) writeManifestFile(ctx context.Context, topic string, snapshotID int64, entries []ManifestEntry) (string, int64, error) {
	records, err := encodeManifestEntries(entries)
	if err != nil {
		return "", 0, err
	}
	key := fmt.Sprintf("%s%d-m%s.avro", ts.metadataDir(topic), snapshotID, metadataContentHash(records))
	n, err := ts.storeOCF(ctx, key, manifestEntryAvroSchema, records)
	return key, n, err
}

// writeManifestListFile encodes manifests as an Iceberg manifest list and
// stores it under a content-addressed key; it returns the key and file size.
func (ts *TableStore) writeManifestListFile(ctx context.Context, topic string, snapshotID int64, manifests []ManifestFile) (string, int64, error) {
	records, err := encodeManifestListRows(manifests)
	if err != nil {
		return "", 0, err
	}
	key := fmt.Sprintf("%ssnap-%d-%s.avro", ts.metadataDir(topic), snapshotID, metadataContentHash(records))
	n, err := ts.storeOCF(ctx, key, manifestListAvroSchema, records)
	return key, n, err
}

// mergeParentManifests reads every parent manifest and returns a single merged
// entry set: carried-over entries become EXISTING with their original snapshot
// and sequence numbers, followed by the newly added entries.
func (ts *TableStore) mergeParentManifests(ctx context.Context, parentManifests []ManifestFile, added []ManifestEntry) ([]ManifestEntry, error) {
	var out []ManifestEntry
	for _, mf := range parentManifests {
		data, err := ts.objects.Get(ctx, mf.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("read manifest %q for merge: %w", mf.ManifestPath, err)
		}
		entries, err := readManifestEntries(data)
		if err != nil {
			return nil, fmt.Errorf("parse manifest %q for merge: %w", mf.ManifestPath, err)
		}
		for _, e := range entries {
			if e.Status == manifestEntryDeleted {
				continue
			}
			e.Status = manifestEntryExisting
			out = append(out, e)
		}
	}
	return append(out, added...), nil
}

// CurrentDataFiles returns the data files referenced by the current snapshot,
// resolved through the snapshot's manifest list and manifests.
func (ts *TableStore) CurrentDataFiles(ctx context.Context, topic string) ([]DataFile, error) {
	current, err := ts.Load(ctx, topic)
	if err != nil {
		return nil, err
	}
	manifests, err := ts.readParentManifestList(ctx, current)
	if err != nil {
		return nil, err
	}
	var files []DataFile
	for _, mf := range manifests {
		data, err := ts.objects.Get(ctx, mf.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("read manifest %q: %w", mf.ManifestPath, err)
		}
		entries, err := readManifestEntries(data)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.Status != manifestEntryDeleted {
				files = append(files, e.DataFile)
			}
		}
	}
	return files, nil
}

// ValidateTable walks the current table state and verifies its structure is a
// consistent Iceberg table: metadata.json invariants, the current snapshot's
// manifest list resolves to existing manifests, every manifest entry has a
// valid status and the dt/hour partition values declared by the spec, and
// every referenced data file exists. It is a diagnostic for tooling and a
// spec-fidelity check before engines read the table.
func (ts *TableStore) ValidateTable(ctx context.Context, topic string) error {
	current, err := ts.Load(ctx, topic)
	if err != nil {
		return fmt.Errorf("validate table %q: %w", topic, err)
	}
	if err := current.validate(); err != nil {
		return fmt.Errorf("validate table %q metadata: %w", topic, err)
	}
	snap := current.currentSnapshot()
	if snap == nil {
		return nil // empty table is valid
	}
	if snap.ManifestList == "" {
		return fmt.Errorf("validate table %q: snapshot %d has no manifest list", topic, snap.SnapshotID)
	}
	listData, err := ts.objects.Get(ctx, snap.ManifestList)
	if err != nil {
		return fmt.Errorf("validate table %q: read manifest list %q: %w", topic, snap.ManifestList, err)
	}
	manifests, err := readManifestList(listData)
	if err != nil {
		return fmt.Errorf("validate table %q: parse manifest list %q: %w", topic, snap.ManifestList, err)
	}
	spec := current.currentPartitionSpec()
	if len(manifests) > 0 && len(spec.Fields) != 2 {
		return fmt.Errorf("validate table %q: manifest list references manifests but partition spec has %d fields", topic, len(spec.Fields))
	}
	for _, mf := range manifests {
		if mf.PartitionSpecID != current.DefaultSpecID {
			return fmt.Errorf("validate table %q: manifest %q uses spec %d, want %d", topic, mf.ManifestPath, mf.PartitionSpecID, current.DefaultSpecID)
		}
		if len(mf.Partitions) != 2 {
			return fmt.Errorf("validate table %q: manifest %q has %d partition summaries, want 2", topic, mf.ManifestPath, len(mf.Partitions))
		}
		data, err := ts.objects.Get(ctx, mf.ManifestPath)
		if err != nil {
			return fmt.Errorf("validate table %q: read manifest %q: %w", topic, mf.ManifestPath, err)
		}
		entries, err := readManifestEntries(data)
		if err != nil {
			return fmt.Errorf("validate table %q: parse manifest %q: %w", topic, mf.ManifestPath, err)
		}
		for _, e := range entries {
			if e.Status != manifestEntryExisting && e.Status != manifestEntryAdded {
				return fmt.Errorf("validate table %q: manifest %q entry for %q has invalid status %d", topic, mf.ManifestPath, e.DataFile.FilePath, e.Status)
			}
			if e.DataFile.DT == "" {
				return fmt.Errorf("validate table %q: manifest %q data file %q has no dt partition value", topic, mf.ManifestPath, e.DataFile.FilePath)
			}
			if _, err := ts.objects.Get(ctx, e.DataFile.FilePath); err != nil {
				return fmt.Errorf("validate table %q: manifest %q references missing data file %q: %w", topic, mf.ManifestPath, e.DataFile.FilePath, err)
			}
		}
	}
	return nil
}

// readParentManifestList returns the current snapshot's manifest list entries,
// which a new snapshot carries forward so its manifest list covers the full
// table state.
func (ts *TableStore) readParentManifestList(ctx context.Context, current *TableMetadata) ([]ManifestFile, error) {
	snap := current.currentSnapshot()
	if snap == nil || snap.ManifestList == "" {
		return nil, nil
	}
	data, err := ts.objects.Get(ctx, snap.ManifestList)
	if err != nil {
		return nil, fmt.Errorf("read parent manifest list %q: %w", snap.ManifestList, err)
	}
	manifests, err := readManifestList(data)
	if err != nil {
		return nil, fmt.Errorf("parse parent manifest list %q: %w", snap.ManifestList, err)
	}
	return manifests, nil
}

// snapshotIDFor derives a stable snapshot id from the manifest list key so a
// retried commit converges on the same snapshot.
func snapshotIDFor(key string) int64 {
	sum := sha256.Sum256([]byte(key))
	return int64(binary.BigEndian.Uint64(sum[:8]) & (1<<63 - 1))
}

// snapshotIDForFiles derives a stable snapshot id from the committed file set
// (paths, sizes, record counts), independent of wall clock, so a retried
// commit of the same data converges on the same snapshot.
func snapshotIDForFiles(files []DataFile) int64 {
	h := sha256.New()
	sorted := append([]DataFile(nil), files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].FilePath < sorted[j].FilePath })
	for _, f := range sorted {
		_, _ = fmt.Fprintf(h, "%s|%d|%d|", f.FilePath, f.RecordCount, f.FileSizeBytes)
	}
	return int64(binary.BigEndian.Uint64(h.Sum(nil)[:8]) & (1<<63 - 1))
}
