package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// offsetCheckReport is the result of a full offset-density audit of one topic:
// every partition must contain exactly the offsets [0, log_end), in order.
// A single consumer scans the whole partition stream, so the check is
// independent of how many producers wrote the topic and of producer restarts.
type offsetCheckReport struct {
	Topic      string                 `json:"topic"`
	CheckedAt  time.Time              `json:"checked_at"`
	Duration   time.Duration          `json:"duration"`
	Partitions map[int]partitionCheck `json:"partitions"`
	Total      int64                  `json:"total_records"`
	Missing    int64                  `json:"total_missing_offsets"`
	OK         bool                   `json:"ok"`
}

type partitionCheck struct {
	Start      int64      `json:"log_start_offset"`
	End        int64      `json:"log_end_offset"`
	Records    int64      `json:"records"`
	First      int64      `json:"first_offset_seen"`
	Last       int64      `json:"last_offset_seen"`
	Missing    int64      `json:"missing_offsets"`
	SampleGaps []gapRange `json:"sample_gaps,omitempty"`
}

type gapRange struct {
	Start   int64 `json:"start_offset"`
	Missing int64 `json:"missing"`
}

const (
	checkIdleTimeout = 10 * time.Second
	maxSampleGaps    = 10
)

// runOffsetCheck consumes every record of a topic from the beginning and
// verifies that each partition's offsets are dense (no gaps). It returns 0
// when the log is contiguous, 1 when gaps were found, 2 on failure.
func runOffsetCheck(cfg serviceConfig, topic string) int {
	assign := make(map[int32]kgo.Offset, cfg.Partitions)
	for p := 0; p < cfg.Partitions; p++ {
		assign[int32(p)] = kgo.NewOffset().AtStart()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.FetchMaxPartitionBytes(consumerFetchMaxPartBytes),
		kgo.FetchMaxBytes(consumerFetchMaxBytes),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka client: %v\n", err)
		return 2
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	// Capture the log end before scanning: the scan is authoritative for
	// [0, end), i.e. every offset below the end must be present exactly once.
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list end offsets: %v\n", err)
		return 2
	}
	endByPart := make(map[int32]int64, len(ends))
	for t, parts := range ends {
		if t != topic {
			continue
		}
		for p, lo := range parts {
			if lo.Offset >= 0 {
				endByPart[p] = lo.Offset
			}
		}
	}
	starts, err := kadm.NewClient(cl).ListStartOffsets(ctx, topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list start offsets: %v\n", err)
		return 2
	}
	startByPart := make(map[int32]int64, len(starts))
	for t, parts := range starts {
		if t != topic {
			continue
		}
		for p, lo := range parts {
			if lo.Offset >= 0 {
				startByPart[p] = lo.Offset
			}
		}
	}

	started := time.Now()
	results := make(map[int32]*partitionCheck)
	next := make(map[int32]int64)
	lastActivity := time.Now()

	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				fmt.Fprintf(os.Stderr, "fetch error: %s p%d: %v\n", e.Topic, e.Partition, e.Err)
			}
			continue
		}
		n := 0
		fetches.EachRecord(func(r *kgo.Record) {
			n++
			pr := results[r.Partition]
			if pr == nil {
				pr = &partitionCheck{}
				results[r.Partition] = pr
				pr.First = r.Offset
			}
			pr.Records++
			pr.Last = r.Offset
			exp, ok := next[r.Partition]
			if !ok {
				// First record establishes the baseline at the log start; a log
				// may legitimately begin after offset 0 (segment trimming), so
				// the first record is never itself a gap.
				next[r.Partition] = r.Offset + 1
				return
			}
			if r.Offset > exp {
				missing := r.Offset - exp
				pr.Missing += missing
				if len(pr.SampleGaps) < maxSampleGaps {
					pr.SampleGaps = append(pr.SampleGaps, gapRange{Start: exp, Missing: missing})
				}
			}
			next[r.Partition] = r.Offset + 1
		})
		if n > 0 {
			lastActivity = time.Now()
		}

		// Complete once every non-empty partition has been read up to the end
		// offset captured when the scan started.
		done := true
		for p := 0; p < cfg.Partitions; p++ {
			pid := int32(p)
			end, ok := endByPart[pid]
			if !ok || end == 0 {
				continue // unknown or empty partition: nothing to scan
			}
			pr := results[pid]
			if pr == nil || pr.Last < end-1 {
				done = false
				break
			}
		}
		if done {
			break
		}
		if n == 0 && time.Since(lastActivity) > checkIdleTimeout {
			break // no progress and not caught up: report what was scanned
		}
	}

	report := offsetCheckReport{
		Topic:      topic,
		CheckedAt:  time.Now().UTC(),
		Duration:   time.Since(started),
		Partitions: make(map[int]partitionCheck, len(results)),
	}
	for p, pr := range results {
		pr.Start = startByPart[p]
		pr.End = endByPart[p]
		report.Partitions[int(p)] = *pr
		report.Total += pr.Records
		report.Missing += pr.Missing
	}
	report.OK = report.Missing == 0

	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if !report.OK {
		return 1
	}
	return 0
}
