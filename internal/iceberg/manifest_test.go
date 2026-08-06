package iceberg

import (
	"bytes"
	"testing"
)

func TestManifestWriteReadRoundTrip(t *testing.T) {
	entries := []ManifestEntry{
		{
			Status:         manifestEntryAdded,
			SnapshotID:     42,
			SequenceNumber: 1,
			DataFile: DataFile{
				Content:       DataFileContentData,
				FilePath:      "warehouse/events/data/abc.parquet",
				FileFormat:    DataFileFormatParquet,
				DT:            "2026-08-06",
				Hour:          13,
				RecordCount:   10,
				FileSizeBytes: 2048,
			},
		},
		{
			Status:         manifestEntryAdded,
			SnapshotID:     42,
			SequenceNumber: 1,
			DataFile: DataFile{
				Content:       DataFileContentData,
				FilePath:      "warehouse/events/data/def.parquet",
				FileFormat:    DataFileFormatParquet,
				DT:            "2026-08-06",
				Hour:          14,
				RecordCount:   3,
				FileSizeBytes: 512,
			},
		},
	}
	var buf bytes.Buffer
	written, err := WriteManifest(&buf, entries)
	if err != nil {
		t.Fatalf("WriteManifest() error = %v", err)
	}
	if written <= 0 {
		t.Fatalf("WriteManifest() wrote %d bytes, want > 0", written)
	}
	got, err := readManifestEntries(buf.Bytes())
	if err != nil {
		t.Fatalf("readManifestEntries() error = %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("entries = %d, want %d", len(got), len(entries))
	}
	for i, e := range got {
		if e.Status != entries[i].Status || e.SnapshotID != entries[i].SnapshotID || e.SequenceNumber != entries[i].SequenceNumber {
			t.Fatalf("entry %d = %+v, want %+v", i, e, entries[i])
		}
		if e.DataFile.FilePath != entries[i].DataFile.FilePath || e.DataFile.FileFormat != "PARQUET" || e.DataFile.DT != entries[i].DataFile.DT || e.DataFile.Hour != entries[i].DataFile.Hour || e.DataFile.RecordCount != entries[i].DataFile.RecordCount || e.DataFile.FileSizeBytes != entries[i].DataFile.FileSizeBytes || e.DataFile.Content != DataFileContentData {
			t.Fatalf("entry %d data file = %+v, want %+v", i, e.DataFile, entries[i].DataFile)
		}
	}
}

func TestManifestListWriteReadRoundTrip(t *testing.T) {
	manifests := []ManifestFile{
		{
			ManifestPath:      "warehouse/events/metadata/42-m1.avro",
			ManifestLength:    1024,
			PartitionSpecID:   0,
			Content:           DataFileContentData,
			SequenceNumber:    1,
			MinSequenceNumber: 1,
			AddedSnapshotID:   42,
			AddedFilesCount:   2,
			AddedRowsCount:    13,
			Partitions: []PartitionFieldSummary{
				{ContainsNull: false, LowerBound: []byte("2026-08-06"), UpperBound: []byte("2026-08-06")},
				{ContainsNull: false, LowerBound: []byte{26}, UpperBound: []byte{28}},
			},
		},
	}
	var buf bytes.Buffer
	written, err := WriteManifestList(&buf, manifests)
	if err != nil {
		t.Fatalf("WriteManifestList() error = %v", err)
	}
	if written <= 0 {
		t.Fatalf("WriteManifestList() wrote %d bytes, want > 0", written)
	}
	got, err := readManifestList(buf.Bytes())
	if err != nil {
		t.Fatalf("readManifestList() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("manifest list rows = %d, want 1", len(got))
	}
	m := got[0]
	want := manifests[0]
	if m.ManifestPath != want.ManifestPath || m.ManifestLength != want.ManifestLength || m.PartitionSpecID != 0 || m.Content != DataFileContentData || m.SequenceNumber != 1 || m.MinSequenceNumber != 1 || m.AddedSnapshotID != 42 || m.AddedFilesCount != 2 || m.AddedRowsCount != 13 || len(m.Partitions) != 2 || !bytes.Equal(m.Partitions[0].LowerBound, want.Partitions[0].LowerBound) || !bytes.Equal(m.Partitions[1].UpperBound, want.Partitions[1].UpperBound) {
		t.Fatalf("manifest list row = %+v, want %+v", m, want)
	}
}

func TestWriteManifestRejectsBadData(t *testing.T) {
	if _, err := WriteManifest(bytes.NewBuffer(nil), nil); err != nil {
		t.Fatalf("empty manifest error = %v, want nil", err)
	}
}
