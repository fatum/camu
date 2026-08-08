package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
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
	MergeDelaySec  float64 `json:"merge_delay_sec"`
	ExportDelaySec float64 `json:"export_delay_sec"`
	CommitDelaySec float64 `json:"commit_delay_sec"`
	LatestProduced int64   `json:"latest_produced"`
	LatestMerged   int64   `json:"latest_merged"`
	LatestExported int64   `json:"latest_exported"`
}

func runVerificationPass(cfg serviceConfig, stats *statsAccumulator) {
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
			tv := verifyTopic(topic, stats)
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

func verifyTopic(topic string, stats *statsAccumulator) *topicVerify {
	tv := &topicVerify{Name: topic}

	var latestMerged, latestExported int64

	// 1. Check merged segment files — also parse end offsets for delay
	tv.MergedFiles, tv.MergedBytes, latestMerged = countDisklessMergeFiles(topic)

	// 2. Check Parquet export files — track latest offset seen
	tv.ParquetFiles, tv.ParquetBytes, tv.ParquetRows, latestExported = countParquetFiles(topic)

	// 3. Check Iceberg table metadata
	tv.IcebergVersion, tv.IcebergSnapshots = checkIcebergMetadata(topic)

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

// readParquetFooter reads the footer metadata of a parquet file and returns
// the footer length, the number of row groups, and the total number of rows.
// It relies on the parquet-go library rather than hand-parsing the Thrift
// footer, which previously always returned zero rows.
func readParquetFooter(data []byte) (footerLen int, rowGroups int, totalRows int64) {
	if len(data) < 12 {
		return 0, 0, 0
	}
	// Last 4 bytes: "PAR1" magic
	if string(data[len(data)-4:]) != "PAR1" {
		return 0, 0, 0
	}
	fl := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	if fl <= 0 || fl > len(data)-8 {
		return 0, 0, 0
	}
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fl, 0, 0
	}
	md := pf.Metadata()
	return fl, len(md.RowGroups), md.NumRows
}

func checkIcebergMetadata(topic string) (version int, snapshots int) {
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
			runVerificationPass(cfg, stats)
		}
	}
}
