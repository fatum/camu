package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

type analysisReport struct {
	RunID           string                    `json:"run_id"`
	GeneratedAt     time.Time                 `json:"generated_at"`
	Duration        time.Duration             `json:"duration"`
	WindowCount     int                       `json:"window_count"`
	Nodes           []string                  `json:"nodes"`
	TopicSummary    map[string]*topicAnalysis `json:"topics"`
	OverallProduced int64                     `json:"overall_produced"`
	OverallConsumed int64                     `json:"overall_consumed"`
	OverallErrors   int64                     `json:"overall_errors"`
	Anomalies       []anomaly                 `json:"anomalies,omitempty"`
}

type topicAnalysis struct {
	Name             string             `json:"name"`
	TotalProduced    int64              `json:"total_produced"`
	TotalConsumed    int64              `json:"total_consumed"`
	TotalErrors      int64              `json:"total_errors"`
	PartitionSummary map[int]*partition `json:"partitions,omitempty"`
	RateProduce      float64            `json:"rate_produce_per_sec"`
	RateConsume      float64            `json:"rate_consume_per_sec"`
}

type partition struct {
	Records      int64 `json:"records"`
	Consumed     int64 `json:"consumed"`
	Errors       int64 `json:"errors"`
	OffsetGaps   int64 `json:"offset_gaps"`
	SeqGaps      int64 `json:"seq_gaps"`
	DecodeErrors int64 `json:"decode_errors"`
}

type anomaly struct {
	Topic     string `json:"topic"`
	Node      string `json:"node"`
	Window    string `json:"window"`
	Severity  string `json:"severity"`
	Message   string `json:"message"`
}

func runAnalyze() int {
	runID := env("BENCHMARK_RUN_ID", "")
	if runID == "" {
		fmt.Fprintf(os.Stderr, "BENCHMARK_RUN_ID is required\n")
		return 2
	}
	prefix := env("S3_PREFIX", "benchmark-stats") + "/" + runID + "/"
	initS3()
	if s3c == nil {
		fmt.Fprintln(os.Stderr, "S3 not configured — set S3_BUCKET, S3_ENDPOINT, and credentials")
		return 1
	}
	keys, err := s3ListRetry(prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list stats: %v\n", err)
		return 1
	}
	if len(keys) == 0 {
		fmt.Fprintf(os.Stderr, "no stats found for run %q at %s\n", runID, prefix)
		return 1
	}
	var windows []snapshot
	for _, key := range keys {
		// verify-* files use a different schema (verificationReport, no
		// start/end/node_id); parsing them as snapshots yields zero-time
		// windows that corrupt duration and node lists.
		if strings.HasPrefix(path.Base(key), "verify-") {
			continue
		}
		data, err := s3GetRetry(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %v\n", key, err)
			continue
		}
		var snap snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", key, err)
			continue
		}
		if snap.Start.IsZero() || snap.End.IsZero() || snap.NodeID == "" {
			continue
		}
		windows = append(windows, snap)
	}
	if len(windows) == 0 {
		fmt.Fprintln(os.Stderr, "no valid stat windows parsed")
		return 1
	}
	sort.Slice(windows, func(i, j int) bool {
		return windows[i].Start.Before(windows[j].Start)
	})

	report := buildReport(windows)
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	return 0
}

func buildReport(windows []snapshot) analysisReport {
	first := windows[0].Start
	last := windows[len(windows)-1].End
	report := analysisReport{
		GeneratedAt:  time.Now().UTC(),
		RunID:        windows[0].RunID,
		Duration:     last.Sub(first),
		WindowCount:  len(windows),
		TopicSummary: make(map[string]*topicAnalysis),
	}
	nodesSeen := make(map[string]bool)
	topicMinTime := make(map[string]time.Time)
	topicMaxTime := make(map[string]time.Time)

	for _, snap := range windows {
		nodesSeen[snap.NodeID] = true
		for topic, ts := range snap.Topics {
			ta := report.TopicSummary[topic]
			if ta == nil {
				ta = &topicAnalysis{Name: topic, PartitionSummary: make(map[int]*partition)}
				report.TopicSummary[topic] = ta
				topicMinTime[topic] = snap.Start
				topicMaxTime[topic] = snap.End
			}
			if snap.Start.Before(topicMinTime[topic]) {
				topicMinTime[topic] = snap.Start
			}
			if snap.End.After(topicMaxTime[topic]) {
				topicMaxTime[topic] = snap.End
			}
			ta.TotalProduced += ts.Producer.Records
			ta.TotalConsumed += ts.Consumer.Records
			ta.TotalErrors += ts.Producer.Errors + ts.Consumer.Errors
			report.OverallProduced += ts.Producer.Records
			report.OverallConsumed += ts.Consumer.Records
			report.OverallErrors += ts.Producer.Errors + ts.Consumer.Errors

			for pid, ps := range ts.Partitions {
				p := ta.PartitionSummary[pid]
				if p == nil {
					p = &partition{}
					ta.PartitionSummary[pid] = p
				}
				p.Records += ps.Records
				p.Consumed += ps.Consumed
				p.Errors += ps.OffsetGaps + ps.SeqGaps + ps.DecodeErrors
				p.OffsetGaps += ps.OffsetGaps
				p.SeqGaps += ps.SeqGaps
				p.DecodeErrors += ps.DecodeErrors
				if ps.OffsetGaps > 0 {
					report.Anomalies = append(report.Anomalies, anomaly{
						Topic:    topic,
						Node:     snap.NodeID,
						Window:   snap.Start.UTC().Format(time.RFC3339),
						Severity: "error",
						Message:  fmt.Sprintf("partition %d: %d offset gaps (missing records)", pid, ps.OffsetGaps),
					})
				}
				if ps.SeqGaps > 0 {
					report.Anomalies = append(report.Anomalies, anomaly{
						Topic:    topic,
						Node:     snap.NodeID,
						Window:   snap.Start.UTC().Format(time.RFC3339),
						Severity: "warning",
						Message:  fmt.Sprintf("partition %d: %d sequence gaps (may be producer restarts)", pid, ps.SeqGaps),
					})
				}
				if ps.DecodeErrors > 0 {
					report.Anomalies = append(report.Anomalies, anomaly{
						Topic:    topic,
						Node:     snap.NodeID,
						Window:   snap.Start.UTC().Format(time.RFC3339),
						Severity: "error",
						Message:  fmt.Sprintf("partition %d: %d decode errors", pid, ps.DecodeErrors),
					})
				}
			}
		}
	}
	for topic, ta := range report.TopicSummary {
		d := topicMaxTime[topic].Sub(topicMinTime[topic])
		if d > 0 {
			ta.RateProduce = float64(ta.TotalProduced) / d.Seconds()
			ta.RateConsume = float64(ta.TotalConsumed) / d.Seconds()
		}
	}
	for node := range nodesSeen {
		report.Nodes = append(report.Nodes, node)
	}
	sort.Strings(report.Nodes)

	// Flag consumer lag anomalies
	for _, ta := range report.TopicSummary {
		if ta.TotalConsumed < ta.TotalProduced {
			report.Anomalies = append(report.Anomalies, anomaly{
				Topic:    ta.Name,
				Severity: "warning",
				Message:  fmt.Sprintf("consumer lag: %d records behind producer", ta.TotalProduced-ta.TotalConsumed),
			})
		}
	}
	return report
}

// runAnalyzeCmd is the entry point when the binary is invoked as "analyze".
func runAnalyzeCmd() {
	os.Exit(runAnalyze())
}
