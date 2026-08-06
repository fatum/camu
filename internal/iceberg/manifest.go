package iceberg

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/hamba/avro/v2"
)

// This file implements the Iceberg v2 manifest and manifest-list Avro files
// that make a snapshot's data files visible to query engines. The Avro Object
// Container File framing is assembled here around hamba/avro's per-record
// encoding; the Iceberg field schemas below follow the spec so the output is a
// standard Iceberg table.

// Data file content types (Iceberg content field).
const (
	DataFileContentData    = 0
	DataFileContentDeletes = 1

	// DataFileFormatParquet is the only format camu writes.
	DataFileFormatParquet = "PARQUET"
)

// manifest entry statuses (Iceberg status field).
const (
	manifestEntryExisting = 0
	manifestEntryAdded    = 1
	manifestEntryDeleted  = 2
)

// DataFile describes one Parquet data file referenced by a manifest.
type DataFile struct {
	Content       int
	FilePath      string
	FileFormat    string
	RecordCount   int64
	FileSizeBytes int64
}

// ManifestEntry is one row of a manifest file.
type ManifestEntry struct {
	Status         int
	SnapshotID     int64
	SequenceNumber int64
	DataFile       DataFile
}

// ManifestFile is one row of a manifest list (a summary of one manifest).
type ManifestFile struct {
	ManifestPath       string
	ManifestLength     int64
	PartitionSpecID    int
	Content            int
	SequenceNumber     int64
	MinSequenceNumber  int64
	AddedSnapshotID    int64
	AddedFilesCount    int
	ExistingFilesCount int
	AddedRowsCount     int64
	ExistingRowsCount  int64
}

// manifestEntryAvroSchema is the Iceberg v2 manifest entry schema, with the
// partition struct inlined as an empty record (camu writes unpartitioned
// tables today).
const manifestEntryAvroSchema = `{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": "long", "optional": true, "field-id": 501},
    {"name": "sequence_number", "type": "long", "optional": true, "field-id": 502},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content", "type": "int", "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "partition", "type": {
          "type": "record", "name": "r102", "fields": []
        }, "field-id": 102},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
        {"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}, "logicalType": "map", "key-id": 117, "value-id": 118}], "field-id": 108, "optional": true},
        {"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v119", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}, "logicalType": "map", "key-id": 117, "value-id": 119}], "field-id": 109, "optional": true},
        {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v120", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}, "logicalType": "map", "key-id": 117, "value-id": 120}], "field-id": 110, "optional": true},
        {"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v138", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}, "logicalType": "map", "key-id": 117, "value-id": 138}], "field-id": 137, "optional": true},
        {"name": "distinct_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v121", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}, "logicalType": "map", "key-id": 117, "value-id": 121}], "field-id": 111, "optional": true},
        {"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v126", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}, "logicalType": "map", "key-id": 117, "value-id": 126}], "field-id": 125, "optional": true},
        {"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v127", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}, "logicalType": "map", "key-id": 117, "value-id": 127}], "field-id": 126, "optional": true},
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 131, "optional": true},
        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "field-id": 132, "optional": true},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "field-id": 135, "optional": true},
        {"name": "sort_order_id", "type": ["null", "int"], "field-id": 140, "optional": true}
      ]
    }}
  ]
}`

// manifestListAvroSchema is the Iceberg v2 manifest list schema (a row per
// manifest in the snapshot's manifest list).
const manifestListAvroSchema = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517},
    {"name": "sequence_number", "type": "long", "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    {"name": "partitions", "type": {"type": "array", "items": {
      "type": "record",
      "name": "r508",
      "fields": [
        {"name": "contains_null", "type": "boolean", "field-id": 509},
        {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518, "optional": true},
        {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 510, "optional": true},
        {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 511, "optional": true}
      ]
    }, "element-id": 508}, "field-id": 508},
    {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519, "optional": true}
  ]
}`

// WriteManifest serializes entries as an Iceberg manifest Avro object container
// file and returns the number of bytes written.
func WriteManifest(w io.Writer, entries []ManifestEntry) (int64, error) {
	records, err := encodeManifestEntries(entries)
	if err != nil {
		return 0, err
	}
	return writeOCF(w, manifestEntryAvroSchema, records)
}

func encodeManifestEntries(entries []ManifestEntry) ([][]byte, error) {
	schema, err := avro.Parse(manifestEntryAvroSchema)
	if err != nil {
		return nil, fmt.Errorf("parse manifest schema: %w", err)
	}
	records := make([][]byte, 0, len(entries))
	for _, e := range entries {
		row := map[string]any{
			"status":          e.Status,
			"snapshot_id":     e.SnapshotID,
			"sequence_number": e.SequenceNumber,
			"data_file": map[string]any{
				"content":            e.DataFile.Content,
				"file_path":          e.DataFile.FilePath,
				"file_format":        e.DataFile.FileFormat,
				"partition":          map[string]any{},
				"record_count":       e.DataFile.RecordCount,
				"file_size_in_bytes": e.DataFile.FileSizeBytes,
				"column_sizes":       nil,
				"value_counts":       nil,
				"null_value_counts":  nil,
				"nan_value_counts":   nil,
				"distinct_counts":    nil,
				"lower_bounds":       nil,
				"upper_bounds":       nil,
				"key_metadata":       nil,
				"split_offsets":      nil,
				"equality_ids":       nil,
				"sort_order_id":      nil,
			},
		}
		encoded, err := avro.Marshal(schema, row)
		if err != nil {
			return nil, fmt.Errorf("encode manifest entry: %w", err)
		}
		records = append(records, encoded)
	}
	return records, nil
}

// WriteManifestList serializes manifests as an Iceberg manifest list Avro
// object container file and returns the number of bytes written.
func WriteManifestList(w io.Writer, manifests []ManifestFile) (int64, error) {
	records, err := encodeManifestListRows(manifests)
	if err != nil {
		return 0, err
	}
	return writeOCF(w, manifestListAvroSchema, records)
}

func encodeManifestListRows(manifests []ManifestFile) ([][]byte, error) {
	schema, err := avro.Parse(manifestListAvroSchema)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list schema: %w", err)
	}
	records := make([][]byte, 0, len(manifests))
	for _, m := range manifests {
		row := map[string]any{
			"manifest_path":        m.ManifestPath,
			"manifest_length":      m.ManifestLength,
			"partition_spec_id":    m.PartitionSpecID,
			"content":              m.Content,
			"sequence_number":      m.SequenceNumber,
			"min_sequence_number":  m.MinSequenceNumber,
			"added_snapshot_id":    m.AddedSnapshotID,
			"added_files_count":    m.AddedFilesCount,
			"existing_files_count": m.ExistingFilesCount,
			"deleted_files_count":  0,
			"added_rows_count":     m.AddedRowsCount,
			"existing_rows_count":  m.ExistingRowsCount,
			"deleted_rows_count":   0,
			"partitions":           []any{},
			"key_metadata":         nil,
		}
		encoded, err := avro.Marshal(schema, row)
		if err != nil {
			return nil, fmt.Errorf("encode manifest list row: %w", err)
		}
		records = append(records, encoded)
	}
	return records, nil
}

// storeOCF writes an Avro OCF under a content-addressed key with
// create-if-not-exists semantics and returns the number of bytes stored.
func (ts *TableStore) storeOCF(ctx context.Context, key, schemaJSON string, records [][]byte) (int64, error) {
	var buf bytes.Buffer
	n, err := writeOCF(&buf, schemaJSON, records)
	if err != nil {
		return 0, err
	}
	if _, err := ts.objects.ConditionalPut(ctx, key, buf.Bytes(), ""); err != nil {
		return 0, fmt.Errorf("write iceberg metadata file %q: %w", key, err)
	}
	return n, nil
}

// metadataContentHash returns a short content hash over encoded Avro records.
func metadataContentHash(records [][]byte) string {
	h := sha256.New()
	for _, r := range records {
		h.Write(r)
	}
	return hex.EncodeToString(h.Sum(nil)[:6])
}

// readManifestEntries reads every entry from an Iceberg manifest.
func readManifestEntries(data []byte) ([]ManifestEntry, error) {
	schema, err := avro.Parse(manifestEntryAvroSchema)
	if err != nil {
		return nil, fmt.Errorf("parse manifest schema: %w", err)
	}
	rows, err := readOCF(data, schema)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	entries := make([]ManifestEntry, 0, len(rows))
	for _, row := range rows {
		m, ok := row.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("manifest entry is not a record")
		}
		df, _ := m["data_file"].(map[string]any)
		entries = append(entries, ManifestEntry{
			Status:         intVal(m["status"]),
			SnapshotID:     longVal(m["snapshot_id"]),
			SequenceNumber: longVal(m["sequence_number"]),
			DataFile: DataFile{
				Content:       intVal(df["content"]),
				FilePath:      strVal(df["file_path"]),
				FileFormat:    strVal(df["file_format"]),
				RecordCount:   longVal(df["record_count"]),
				FileSizeBytes: longVal(df["file_size_in_bytes"]),
			},
		})
	}
	return entries, nil
}

// readManifestList reads every manifest summary from an Iceberg manifest list.
func readManifestList(data []byte) ([]ManifestFile, error) {
	schema, err := avro.Parse(manifestListAvroSchema)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list schema: %w", err)
	}
	rows, err := readOCF(data, schema)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}
	manifests := make([]ManifestFile, 0, len(rows))
	for _, row := range rows {
		m, ok := row.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("manifest list row is not a record")
		}
		manifests = append(manifests, ManifestFile{
			ManifestPath:      strVal(m["manifest_path"]),
			ManifestLength:    longVal(m["manifest_length"]),
			PartitionSpecID:   intVal(m["partition_spec_id"]),
			Content:           intVal(m["content"]),
			SequenceNumber:    longVal(m["sequence_number"]),
			MinSequenceNumber: longVal(m["min_sequence_number"]),
			AddedSnapshotID:   longVal(m["added_snapshot_id"]),
			AddedFilesCount:   intVal(m["added_files_count"]),
			AddedRowsCount:    longVal(m["added_rows_count"]),
		})
	}
	return manifests, nil
}

// writeOCF frames records into an Avro Object Container File: the magic
// header, the schema + codec metadata map, a sync marker, then one data block
// per record (each block carries a fresh sync marker), followed by a
// terminating block. Writing one record per block keeps reads exact: a reader
// can decode each block with avro.Unmarshal without locating record
// boundaries inside a shared block. This is a valid Iceberg manifest layout
// (engines must handle arbitrary block sizes); camu is the only writer of its
// own tables.
func writeOCF(w io.Writer, schemaJSON string, records [][]byte) (int64, error) {
	var buf bytes.Buffer
	buf.WriteString("Obj\x01")

	// File metadata map: avro.schema + avro.codec (null).
	putAvroLong(&buf, 2)
	putAvroString(&buf, "avro.schema")
	putAvroBytes(&buf, []byte(schemaJSON))
	putAvroString(&buf, "avro.codec")
	putAvroBytes(&buf, []byte("null"))
	putAvroLong(&buf, 0)

	sync := make([]byte, 16)
	if _, err := rand.Read(sync); err != nil {
		return 0, fmt.Errorf("generate sync marker: %w", err)
	}
	buf.Write(sync)

	for _, record := range records {
		putAvroLong(&buf, 1) // block count
		putAvroLong(&buf, int64(len(record)))
		buf.Write(record)
		buf.Write(sync)
	}

	// Terminating block.
	putAvroLong(&buf, 0)
	putAvroLong(&buf, 0)
	buf.Write(sync)

	n, err := w.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

// readOCF parses an Avro Object Container File written by writeOCF (one record
// per block) and returns one decoded value per block.
func readOCF(data []byte, schema avro.Schema) ([]any, error) {
	r := bytes.NewReader(data)
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil || string(header) != "Obj\x01" {
		return nil, fmt.Errorf("not an avro object container file")
	}
	// File metadata map: count of entries, then key/bytes pairs.
	count, err := readAvroLong(r)
	if err != nil {
		return nil, err
	}
	for i := int64(0); i < count; i++ {
		if _, err := readAvroString(r); err != nil {
			return nil, err
		}
		if _, err := readAvroBytes(r); err != nil {
			return nil, err
		}
	}
	if _, err := readAvroLong(r); err != nil { // map end marker
		return nil, err
	}
	sync := make([]byte, 16)
	if _, err := io.ReadFull(r, sync); err != nil {
		return nil, err
	}

	var out []any
	for {
		blockCount, err := readAvroLong(r)
		if err != nil {
			return nil, err
		}
		blockSize, err := readAvroLong(r)
		if err != nil {
			return nil, err
		}
		if blockCount == 0 {
			break
		}
		block := make([]byte, blockSize)
		if _, err := io.ReadFull(r, block); err != nil {
			return nil, fmt.Errorf("truncated avro data block: %w", err)
		}
		marker := make([]byte, 16)
		if _, err := io.ReadFull(r, marker); err != nil {
			return nil, fmt.Errorf("truncated avro sync marker: %w", err)
		}
		if !bytes.Equal(marker, sync) {
			return nil, fmt.Errorf("avro sync marker mismatch")
		}
		if blockCount != 1 {
			return nil, fmt.Errorf("unsupported multi-record avro block (%d records)", blockCount)
		}
		var v any
		if err := avro.Unmarshal(schema, block, &v); err != nil {
			return nil, fmt.Errorf("decode avro record: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

func putAvroLong(buf *bytes.Buffer, v int64) {
	u := uint64((v << 1) ^ (v >> 63))
	for u >= 0x80 {
		buf.WriteByte(byte(u) | 0x80)
		u >>= 7
	}
	buf.WriteByte(byte(u))
}

func putAvroBytes(buf *bytes.Buffer, b []byte) {
	putAvroLong(buf, int64(len(b)))
	buf.Write(b)
}

func putAvroString(buf *bytes.Buffer, s string) {
	putAvroBytes(buf, []byte(s))
}

func readAvroLong(r *bytes.Reader) (int64, error) {
	var u uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		u |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return int64(u>>1) ^ -int64(u&1), nil
}

func readAvroBytes(r *bytes.Reader) ([]byte, error) {
	n, err := readAvroLong(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func readAvroString(r *bytes.Reader) (string, error) {
	b, err := readAvroBytes(r)
	return string(b), err
}

func intVal(v any) int {
	switch t := v.(type) {
	case int32:
		return int(t)
	case int64:
		return int(t)
	case int:
		return t
	}
	return 0
}

func longVal(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int32:
		return int64(t)
	case int:
		return int64(t)
	}
	return 0
}

func strVal(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
