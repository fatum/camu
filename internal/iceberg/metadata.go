package iceberg

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/maksim/camu/internal/meta"
)

// This file models the Apache Iceberg table metadata (the metadata.json
// object), format version 2. Camu writes self-managed Iceberg tables: the
// export pipeline appends Parquet data files and commits a new snapshot by
// atomically publishing a new metadata.json via a version-hint CAS. External
// query engines (DuckDB iceberg_scan, Trino, Spark) read the same layout.
//
// The metadata model intentionally follows the Iceberg spec (v2) so the output
// is a standard table. Only the fields Camu produces are modeled.

// SchemaField is one column of the Iceberg schema.
type SchemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

// Schema is an Iceberg table schema.
type Schema struct {
	Type     string        `json:"type"`
	SchemaID int           `json:"schema-id"`
	Fields   []SchemaField `json:"fields"`
}

// PartitionField is one entry of a partition spec.
type PartitionField struct {
	Name      string `json:"name"`
	Transform string `json:"transform"`
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
}

// PartitionSpec describes how the table is partitioned.
type PartitionSpec struct {
	SpecID int              `json:"spec-id"`
	Fields []PartitionField `json:"fields"`
}

// SortOrder is the default sort order (empty: unsorted).
type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

// SortField is one entry of a sort order.
type SortField struct {
	Transform string `json:"transform"`
	SourceID  int    `json:"source-id"`
	Direction string `json:"direction"`
}

// SnapshotSummary is the free-form summary attached to a snapshot.
type SnapshotSummary map[string]string

// Snapshot is one committed table state; its manifest list references the
// manifests that reference the data files.
type Snapshot struct {
	SnapshotID     int64           `json:"snapshot-id"`
	SequenceNumber int64           `json:"sequence-number,omitempty"`
	TimestampMS    int64           `json:"timestamp-ms"`
	ManifestList   string          `json:"manifest-list,omitempty"`
	Summary        SnapshotSummary `json:"summary,omitempty"`
	SchemaID       *int            `json:"schema-id,omitempty"`
}

// SnapshotLogEntry records when each snapshot became current.
type SnapshotLogEntry struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMS int64 `json:"timestamp-ms"`
}

// MetadataLogEntry records each previous metadata file.
type MetadataLogEntry struct {
	TimestampMS  int64  `json:"timestamp-ms"`
	MetadataFile string `json:"metadata-file"`
}

// SnapshotRef is a named reference (Iceberg refs map), e.g. "main".
type SnapshotRef struct {
	SnapshotID int64  `json:"snapshot-id"`
	Type       string `json:"type,omitempty"`
}

// TableMetadata is the Iceberg table metadata object (metadata.json).
// Version is the file-version used for naming and CAS, not part of the
// Iceberg spec.
type TableMetadata struct {
	FormatVersion      int                    `json:"format-version"`
	TableUUID          string                 `json:"table-uuid"`
	Location           string                 `json:"location"`
	LastUpdatedMS      int64                  `json:"last-updated-ms"`
	LastColumnID       int                    `json:"last-column-id"`
	LastSequenceNumber int64                  `json:"last-sequence-number"`
	CurrentSchemaID    int                    `json:"current-schema-id"`
	Schemas            []*Schema              `json:"schemas"`
	DefaultSpecID      int                    `json:"default-spec-id"`
	PartitionSpecs     []*PartitionSpec       `json:"partition-specs"`
	LastPartitionID    int                    `json:"last-partition-id"`
	DefaultSortOrderID int                    `json:"default-sort-order-id"`
	SortOrders         []SortOrder            `json:"sort-orders"`
	Properties         map[string]string      `json:"properties,omitempty"`
	CurrentSnapshotID  *int64                 `json:"current-snapshot-id"`
	Snapshots          []*Snapshot            `json:"snapshots,omitempty"`
	SnapshotLog        []SnapshotLogEntry     `json:"snapshot-log,omitempty"`
	MetadataLog        []MetadataLogEntry     `json:"metadata-log,omitempty"`
	Refs               map[string]SnapshotRef `json:"refs,omitempty"`

	// version and metadataKey are bookkeeping, not part of the Iceberg spec:
	// version is the file-version used for naming and CAS, and metadataKey is
	// the exact object key the metadata was loaded from (used to populate the
	// metadata-log chain on the next commit).
	version     int    `json:"-"`
	metadataKey string `json:"-"`
}

// tableFormatVersion is the Iceberg format-version Camu writes.
const tableFormatVersion = 2

// Base export column ids. dt/hour are the ingest-time partition columns; typed
// topic columns follow starting at baseExportColumnIDCount+1.
const (
	exportColumnRecordOffset    = "record_offset"
	exportColumnRecordTimestamp = "record_timestamp"
	exportColumnKey             = "key"
	exportColumnValue           = "value"
	exportColumnHeaders         = "headers"
	exportColumnDT              = "dt"
	exportColumnHour            = "hour"
	baseExportColumnIDCount     = 7
	// partition field ids for the dt/hour identity partition spec.
	partitionFieldIDDT   = 1000
	partitionFieldIDHour = 1001
)

// SchemaFromTopic derives the Iceberg schema for a camu topic from its typed
// schema. The seven base export columns are always present (including the
// dt/hour partition columns); typed topic columns are appended (nullable per
// the topic schema). Column order matches the Parquet file written by
// EncodeChunk. The schema id is the topic schema's version, so a schema
// evolution registers a new Iceberg schema id.
func SchemaFromTopic(topicSchema *meta.TopicSchema) *Schema {
	schemaID := 0
	if topicSchema != nil {
		schemaID = topicSchema.Version
	}
	return buildTableSchema(nil, topicSchema, schemaID)
}

// buildTableSchema builds an Iceberg schema for a topic schema version,
// preserving column ids for fields that appeared in an earlier schema so that
// schema evolution never renumbers a column a reader already knows. prior is
// the table's previous schema (nil for the initial version).
func buildTableSchema(prior *Schema, topicSchema *meta.TopicSchema, schemaID int) *Schema {
	fields := []SchemaField{
		{ID: 1, Name: exportColumnRecordOffset, Required: true, Type: "long"},
		{ID: 2, Name: exportColumnRecordTimestamp, Required: true, Type: "long"},
		{ID: 3, Name: exportColumnKey, Required: true, Type: "binary"},
		{ID: 4, Name: exportColumnValue, Required: true, Type: "binary"},
		{ID: 5, Name: exportColumnHeaders, Required: true, Type: "string"},
		{ID: 6, Name: exportColumnDT, Required: true, Type: "string"},
		{ID: 7, Name: exportColumnHour, Required: true, Type: "int"},
	}
	nameToID := make(map[string]int, len(fields))
	for _, f := range fields {
		nameToID[f.Name] = f.ID
	}
	nextID := baseExportColumnIDCount + 1
	if prior != nil {
		for _, f := range prior.Fields {
			if _, ok := nameToID[f.Name]; ok {
				continue // base column
			}
			nameToID[f.Name] = f.ID
			if f.ID >= nextID {
				nextID = f.ID + 1
			}
		}
	}
	if topicSchema != nil {
		for _, field := range topicSchema.Fields {
			id, ok := nameToID[field.Name]
			if !ok {
				id = nextID
				nextID++
				nameToID[field.Name] = id
			}
			fields = append(fields, SchemaField{
				ID:       id,
				Name:     field.Name,
				Required: !field.Nullable,
				Type:     icebergFieldType(field.Type),
			})
		}
	}
	return &Schema{Type: "struct", SchemaID: schemaID, Fields: fields}
}

func icebergFieldType(t string) string {
	switch t {
	case "int64":
		return "long"
	case "float64":
		return "double"
	case "bool":
		return "boolean"
	case "timestamp":
		return "timestamp_ns"
	default:
		return "string"
	}
}

// NewTableMetadata creates the initial (version 0) metadata for a table with
// no snapshots. tableLocation is the object-store root of the table. Tables
// are partitioned by identity(dt), identity(hour) on the ingest-time columns.
func NewTableMetadata(tableLocation string, topicSchema *meta.TopicSchema) *TableMetadata {
	schema := SchemaFromTopic(topicSchema)
	lastColumnID := maxSchemaFieldID(schema.Fields)
	now := time.Now().UnixMilli()
	return &TableMetadata{
		FormatVersion:      tableFormatVersion,
		TableUUID:          uuid.NewString(),
		Location:           tableLocation,
		LastUpdatedMS:      now,
		LastColumnID:       lastColumnID,
		LastSequenceNumber: 0,
		CurrentSchemaID:    schema.SchemaID,
		Schemas:            []*Schema{schema},
		DefaultSpecID:      0,
		PartitionSpecs: []*PartitionSpec{{
			SpecID: 0,
			Fields: []PartitionField{
				{Name: exportColumnDT, Transform: "identity", SourceID: 6, FieldID: partitionFieldIDDT},
				{Name: exportColumnHour, Transform: "identity", SourceID: 7, FieldID: partitionFieldIDHour},
			},
		}},
		LastPartitionID:    partitionFieldIDHour,
		DefaultSortOrderID: 0,
		SortOrders:         []SortOrder{{OrderID: 0, Fields: []SortField{}}},
		Properties:         map[string]string{},
		Snapshots:          []*Snapshot{},
		SnapshotLog:        []SnapshotLogEntry{},
		MetadataLog:        []MetadataLogEntry{},
		Refs:               map[string]SnapshotRef{},
	}
}

// maxSchemaFieldID returns the highest column id in a schema, which is what
// last-column-id must reflect regardless of field ordering.
func maxSchemaFieldID(fields []SchemaField) int {
	max := 0
	for _, f := range fields {
		if f.ID > max {
			max = f.ID
		}
	}
	return max
}

// currentSchema returns the current schema, falling back to schemas[0].
func (m *TableMetadata) currentSchema() *Schema {
	for _, s := range m.Schemas {
		if s.SchemaID == m.CurrentSchemaID {
			return s
		}
	}
	if len(m.Schemas) > 0 {
		return m.Schemas[0]
	}
	return nil
}

// currentPartitionSpec returns the default partition spec.
func (m *TableMetadata) currentPartitionSpec() *PartitionSpec {
	for _, s := range m.PartitionSpecs {
		if s.SpecID == m.DefaultSpecID {
			return s
		}
	}
	if len(m.PartitionSpecs) > 0 {
		return m.PartitionSpecs[0]
	}
	return nil
}

// currentSnapshot returns the snapshot named by CurrentSnapshotID, or nil.
func (m *TableMetadata) currentSnapshot() *Snapshot {
	if m.CurrentSnapshotID == nil {
		return nil
	}
	for _, s := range m.Snapshots {
		if s.SnapshotID == *m.CurrentSnapshotID {
			return s
		}
	}
	return nil
}

// snapshotByID returns the snapshot with the given id, or nil.
func (m *TableMetadata) snapshotByID(id int64) *Snapshot {
	for _, s := range m.Snapshots {
		if s.SnapshotID == id {
			return s
		}
	}
	return nil
}

// sortedSnapshots returns the table's snapshots ordered by sequence number.
func (m *TableMetadata) sortedSnapshots() []*Snapshot {
	out := append([]*Snapshot(nil), m.Snapshots...)
	sort.Slice(out, func(i, j int) bool { return out[i].SequenceNumber < out[j].SequenceNumber })
	return out
}

// nextSequenceNumber returns the sequence number for the next snapshot.
func (m *TableMetadata) nextSequenceNumber() int64 {
	seq := m.LastSequenceNumber + 1
	m.LastSequenceNumber = seq
	return seq
}

// clone deep-copies the metadata so a commit can mutate a copy without
// corrupting the loaded instance on a CAS conflict.
func (m *TableMetadata) clone() *TableMetadata {
	c := *m
	c.Schemas = append([]*Schema(nil), m.Schemas...)
	c.PartitionSpecs = append([]*PartitionSpec(nil), m.PartitionSpecs...)
	c.SortOrders = append([]SortOrder(nil), m.SortOrders...)
	c.Properties = make(map[string]string, len(m.Properties))
	for k, v := range m.Properties {
		c.Properties[k] = v
	}
	c.Snapshots = append([]*Snapshot(nil), m.Snapshots...)
	for i, s := range m.Snapshots {
		ss := *s
		if s.Summary != nil {
			ss.Summary = make(SnapshotSummary, len(s.Summary))
			for k, v := range s.Summary {
				ss.Summary[k] = v
			}
		}
		c.Snapshots[i] = &ss
	}
	c.SnapshotLog = append([]SnapshotLogEntry(nil), m.SnapshotLog...)
	c.MetadataLog = append([]MetadataLogEntry(nil), m.MetadataLog...)
	c.Refs = make(map[string]SnapshotRef, len(m.Refs))
	for k, v := range m.Refs {
		c.Refs[k] = v
	}
	c.metadataKey = m.metadataKey
	return &c
}

// validate checks the invariants Camu relies on when committing.
func (m *TableMetadata) validate() error {
	if m.FormatVersion != tableFormatVersion {
		return fmt.Errorf("unsupported iceberg format-version %d", m.FormatVersion)
	}
	if m.TableUUID == "" {
		return fmt.Errorf("iceberg table metadata missing table-uuid")
	}
	if m.currentSchema() == nil {
		return fmt.Errorf("iceberg table metadata has no schema")
	}
	return nil
}
