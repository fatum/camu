package main

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestReadParquetFooter verifies that the verify pass can extract row counts
// from a parquet footer. The old hand-rolled Thrift scanner always returned
// zero rows, which made verify reports flag every diskless topic as "data
// loss". parquet-go must report the real row group count and total rows.
func TestReadParquetFooter(t *testing.T) {
	var buf bytes.Buffer
	if err := parquet.Write(&buf, []struct {
		Seq int64
	}{{1}, {2}, {3}, {4}, {5}}); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	data := buf.Bytes()

	footerLen, rowGroups, totalRows := readParquetFooter(data)
	if footerLen <= 0 {
		t.Fatalf("footerLen = %d, want > 0", footerLen)
	}
	if rowGroups != 1 {
		t.Fatalf("rowGroups = %d, want 1", rowGroups)
	}
	if totalRows != 5 {
		t.Fatalf("totalRows = %d, want 5", totalRows)
	}
}

func TestReadParquetFooterNotParquet(t *testing.T) {
	if footerLen, rowGroups, totalRows := readParquetFooter([]byte("not parquet data")); footerLen != 0 || rowGroups != 0 || totalRows != 0 {
		t.Fatalf("got (%d, %d, %d), want (0, 0, 0)", footerLen, rowGroups, totalRows)
	}
}
