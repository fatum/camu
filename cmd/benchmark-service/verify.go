package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type verificationReport struct {
	Time     time.Time               `json:"time"`
	RunID    string                  `json:"run_id"`
	Topics   map[string]*topicVerify `json:"topics"`
	Duration time.Duration           `json:"duration"`
	Errors   []string                `json:"errors,omitempty"`
}

type topicVerify struct {
	Name             string     `json:"name"`
	MergedFiles      int        `json:"merged_files"`
	MergedBytes      int64      `json:"merged_bytes"`
	ParquetFiles     int        `json:"parquet_files"`
	ParquetBytes     int64      `json:"parquet_bytes"`
	ParquetRows      int64      `json:"parquet_rows"`
	IcebergSnapshots int        `json:"iceberg_snapshots"`
	IcebergVersion   int        `json:"iceberg_version"`
	ExpectedRecords  int64      `json:"expected_records"`
	DataLoss         bool       `json:"data_loss"`
	Delay            delayStats `json:"delay"`
}

type delayStats struct {
	MergeDelaySec   float64 `json:"merge_delay_sec"`
	ExportDelaySec  float64 `json:"export_delay_sec"`
	CommitDelaySec  float64 `json:"commit_delay_sec"`
	LatestProduced  int64   `json:"latest_produced"`
	LatestMerged    int64   `json:"latest_merged"`
	LatestExported  int64   `json:"latest_exported"`
}

func runVerificationPass(ctx context.Context, cfg serviceConfig, stats *statsAccumulator) {
	slog.Info("verification_pass_starting")
	started := time.Now()
	report := verificationReport{
		Time:   time.Now().UTC(),
		RunID:  cfg.RunID,
		Topics: make(map[string]*topicVerify),
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, topic := range cfg.Topics {
		topic := strings.TrimSpace(topic)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tv := verifyTopic(ctx, cfg, topic, stats)
			mu.Lock()
			report.Topics[topic] = tv
			if tv.DataLoss {
				report.Errors = append(report.Errors, fmt.Sprintf("%s: data loss detected", topic))
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	report.Duration = time.Since(started)

	b, _ := json.MarshalIndent(report, "", "  ")
	key := fmt.Sprintf("%s/%s/%s/verify-%s.json",
		cfg.S3Prefix,
		cfg.RunID,
		cfg.NodeID,
		report.Time.Format("20060102T150405Z"),
	)
	if err := s3PutRetry(key, b); err != nil {
		slog.Warn("verify_upload_failed", "key", key, "error", err)
	} else {
		slog.Info("verify_report_uploaded", "key", key, "bytes", len(b))
	}
}

func verifyTopic(ctx context.Context, cfg serviceConfig, topic string, stats *statsAccumulator) *topicVerify {
	tv := &topicVerify{Name: topic}

	var latestMerged, latestExported int64

	// 1. Check merged segment files — also parse end offsets for delay
	tv.MergedFiles, tv.MergedBytes, latestMerged = countDisklessMergeFiles(topic)

	// 2. Check Parquet export files — track latest offset seen
	tv.ParquetFiles, tv.ParquetBytes, tv.ParquetRows, latestExported = countParquetFiles(topic)

	// 3. Check Iceberg table metadata
	tv.IcebergVersion, tv.IcebergSnapshots = checkIcebergMetadata(ctx, topic)

	// 4. Compute delays
	latestProduced := stats.totalProd.Load()
	tv.Delay = computeDelay(stats, latestProduced, latestMerged, latestExported)

	// 5. Data loss check
	tv.ExpectedRecords = tv.ParquetRows
	tv.DataLoss = tv.ParquetRows == 0 && tv.MergedFiles > 0

	return tv
}

func computeDelay(stats *statsAccumulator, latestProduced, latestMerged, latestExported int64) delayStats {
	now := time.Now()
	ds := delayStats{
		LatestProduced: latestProduced,
		LatestMerged:   latestMerged,
		LatestExported: latestExported,
	}

	// Merge delay: time since records at the merged offset were produced
	if latestMerged > 0 && stats != nil {
		producedAt := stats.produceTime(latestMerged / 100 * 100)
		if !producedAt.IsZero() {
			ds.MergeDelaySec = now.Sub(producedAt).Seconds()
		}
	}

	// Export delay: time since records with total exported rows were produced
	if latestExported > 0 && stats != nil {
		producedAt := stats.produceTime(latestExported / 100 * 100)
		if !producedAt.IsZero() {
			ds.ExportDelaySec = now.Sub(producedAt).Seconds()
		}
	}

	// Commit delay: max of merge and export
	ds.CommitDelaySec = ds.MergeDelaySec
	if ds.ExportDelaySec > ds.CommitDelaySec {
		ds.CommitDelaySec = ds.ExportDelaySec
	}

	return ds
}

func countDisklessMergeFiles(topic string) (int, int64, int64) {
	prefix := fmt.Sprintf("_diskless_merge/%s/", topic)
	keys, err := s3ListRetry(prefix)
	if err != nil {
		return 0, 0, 0
	}
	var files, bytes, latestEndOffset int64
	for _, k := range keys {
		if !strings.HasSuffix(k, ".data") {
			continue
		}
		files++
		data, err := s3GetRetry(k)
		if err == nil {
			bytes += int64(len(data))
		}
		// Parse end offset from filename: .../00000-00000000000000873814.data
		parts := strings.Split(k, "/")
		last := parts[len(parts)-1]
		if idx := strings.LastIndex(last, "-"); idx >= 0 {
			suffix := last[idx+1:]
			suffix = strings.TrimSuffix(suffix, ".data")
			var endOff int64
			fmt.Sscanf(suffix, "%d", &endOff)
			if endOff > latestEndOffset {
				latestEndOffset = endOff
			}
		}
	}
	return int(files), bytes, latestEndOffset
}

func countParquetFiles(topic string) (int, int64, int64, int64) {
	prefix := fmt.Sprintf("warehouse/%s/data/", topic)
	keys, err := s3ListRetry(prefix)
	if err != nil {
		return 0, 0, 0, 0
	}
	var files, rows, bytes int64
	for _, k := range keys {
		if !strings.HasSuffix(k, ".parquet") {
			continue
		}
		files++
		data, err := s3GetRetry(k)
		if err != nil {
			continue
		}
		bytes += int64(len(data))
		_, rgCount, totalRows := readParquetFooter(data)
		if rgCount > 0 {
			rows += totalRows
		}
	}
	return int(files), bytes, rows, rows
}

// readParquetFooter reads the footer metadata length from the last 8 bytes
// of the file, then parses the footer to extract row group count and total row count.
func readParquetFooter(data []byte) (footerLen int, rowGroups int, totalRows int64) {
	if len(data) < 12 {
		return 0, 0, 0
	}
	// Last 4 bytes: "PAR1" magic
	magic := string(data[len(data)-4:])
	if magic != "PAR1" {
		return 0, 0, 0
	}
	// 4 bytes before magic: footer length (little-endian int32)
	fl := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	if fl <= 0 || fl > len(data)-8 {
		return 0, 0, 0
	}
	footerStart := len(data) - 8 - fl
	footerBytes := data[footerStart : footerStart+fl]

	// Quick scan for "num_rows" in the Thrift-compact footer.
	// Parquet FileMetaData thrift struct: version(1) -> schema -> num_rows -> row_groups
	// num_rows is field 2 (i64) after schema. We do a simple search.
	totalRows = scanThriftI64Field(footerBytes, 2)
	rowGroupCount := countRowGroups(footerBytes)
	return fl, rowGroupCount, totalRows
}

func scanThriftI64Field(footer []byte, fieldID int) int64 {
	// Simple ThriftCompactProtocol scanner for i64 field.
	// Format: field header (fieldID<<3 | type), then zigzag-encoded i64.
	pos := 0
	skipStructHeader := true
	for pos < len(footer) {
		if skipStructHeader {
			// Struct header: 1 byte version + name string
			skipStructHeader = false
			if pos >= len(footer) {
				break
			}
			nameLen := int(footer[pos])
			pos += 1 + nameLen
			continue
		}
		if pos >= len(footer) {
			break
		}
		fh := footer[pos]
		pos++
		fID := int(fh >> 3 & 0x0F)
		fType := fh & 0x07
		
		// Handle 2-byte field headers (field ID >= 16)
		if fID == 0 && fType != 0 {
			// delta-encoded field ID
			delta := int(footer[pos])
			pos++
			fID += delta
		}

		if fType == 0 { // STOP
			break
		}

		switch fType {
		case 10: // i64
			if fID == fieldID {
				val, _ := readZigZagI64(footer[pos:])
				return val
			}
			_, n := readZigZagI64(footer[pos:])
			pos += n
		case 8: // i32
			_, n := readZigZagI32(footer[pos:])
			pos += n
		case 11: // binary/string
			size, n := readZigZagI32(footer[pos:])
			pos += n + int(size)
		case 12: // struct
			pos++ // skip struct header byte
		case 15: // list
			// elem type + size
			pos++ // elem type byte
			size, n := readZigZagI32(footer[pos:])
			pos += n + int(size)
		default:
			return 0
		}
	}
	return 0
}

func readZigZagI64(buf []byte) (int64, int) {
	var u uint64
	var shift uint
	for i := 0; i < 10; i++ {
		b := buf[i]
		u |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return int64(u>>1) ^ -int64(u&1), i + 1
		}
		shift += 7
	}
	return 0, 0
}

func readZigZagI32(buf []byte) (int32, int) {
	var u uint32
	var shift uint
	for i := 0; i < 5; i++ {
		b := buf[i]
		u |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			return int32(u>>1) ^ -int32(u&1), i + 1
		}
		shift += 7
	}
	return 0, 0
}

func countRowGroups(footer []byte) int {
	// Count "row_groups" entries by scanning for list markers
	count := 0
	for i := 0; i < len(footer)-4; i++ {
		if footer[i] == 'r' && footer[i+1] == 'o' && footer[i+2] == 'w' {
			count++
		}
	}
	return count
}

func checkIcebergMetadata(ctx context.Context, topic string) (version int, snapshots int) {
	prefix := fmt.Sprintf("warehouse/%s/metadata/", topic)
	keys, err := s3ListRetry(prefix)
	if err != nil {
		return 0, 0
	}
	snapCount := 0
	versionHint := 0
	for _, k := range keys {
		if strings.Contains(k, "version-hint.text") {
			data, err := s3GetRetry(k)
			if err == nil {
				var v int
				fmt.Sscanf(string(data), "%d", &v)
				versionHint = v
			}
		}
		if strings.HasSuffix(k, ".metadata.json") && strings.Contains(k, "/metadata/") {
			// Parse metadata JSON to count snapshots
			data, err := s3GetRetry(k)
			if err != nil {
				continue
			}
			var md struct {
				Snapshots []struct {
					SnapshotID int64 `json:"snapshot-id"`
				} `json:"snapshots"`
			}
			if json.Unmarshal(data, &md) == nil {
				snapCount = len(md.Snapshots)
				break // Use latest metadata file for snapshot count
			}
		}
	}
	return versionHint, snapCount
}

func startVerificationLoop(ctx context.Context, cfg serviceConfig, stats *statsAccumulator) {
	interval := max(cfg.StatsInterval*2, 10*time.Minute)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	slog.Info("verification_loop_started", "interval", interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runVerificationPass(ctx, cfg, stats)
		}
	}
}
