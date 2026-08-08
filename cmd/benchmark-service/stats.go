package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type statsAccumulator struct {
	mu           sync.Mutex
	produceTimes map[int64]time.Time // seq -> wall clock when produced
	topics       map[string]*topicCounters
	cfg          serviceConfig
	totalProd    atomic.Int64
	totalCons    atomic.Int64
	totalErr     atomic.Int64
}

type topicCounters struct {
	producer   phaseCounters
	consumer   phaseCounters
	partitions map[int]*partitionCounters
}

type phaseCounters struct {
	records    int64
	bytes      int64
	errors     int64
	latencySum float64
	latencyN   int64
}

type partitionCounters struct {
	records      int64
	bytes        int64
	consumed     int64
	offsetGaps   int64
	seqGaps      int64
	decodeErrors int64
}

type snapshot struct {
	Start  time.Time                `json:"start"`
	End    time.Time                `json:"end"`
	NodeID string                   `json:"node_id"`
	RunID  string                   `json:"run_id"`
	Topics map[string]topicSnapshot `json:"topics"`
}

type topicSnapshot struct {
	Producer   phaseSnapshot             `json:"producer"`
	Consumer   phaseSnapshot             `json:"consumer"`
	Partitions map[int]partitionSnapshot `json:"partitions"`
}

type phaseSnapshot struct {
	Records    int64   `json:"records"`
	Bytes      int64   `json:"bytes"`
	Errors     int64   `json:"errors"`
	LatencyP50 float64 `json:"latency_p50,omitempty"`
	LatencyP95 float64 `json:"latency_p95,omitempty"`
	LatencyP99 float64 `json:"latency_p99,omitempty"`
}

type partitionSnapshot struct {
	Records      int64 `json:"records"`
	Bytes        int64 `json:"bytes"`
	Consumed     int64 `json:"consumed,omitempty"`
	OffsetGaps   int64 `json:"offset_gaps"`
	SeqGaps      int64 `json:"seq_gaps,omitempty"`
	DecodeErrors int64 `json:"decode_errors"`
}

func newStats(cfg serviceConfig) *statsAccumulator {
	s := &statsAccumulator{
		cfg:          cfg,
		topics:       make(map[string]*topicCounters),
		produceTimes: make(map[int64]time.Time),
	}
	for _, t := range cfg.Topics {
		s.topics[t] = &topicCounters{partitions: make(map[int]*partitionCounters)}
	}
	return s
}

func (s *statsAccumulator) recordProduce(topic string, partition int, bytes int64, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tc := s.topics[topic]
	if tc == nil {
		return
	}
	tc.producer.records++
	tc.producer.bytes += bytes
	tc.producer.latencySum += latency.Seconds()
	tc.producer.latencyN++

	p := tc.partitions[partition]
	if p == nil {
		p = &partitionCounters{}
		tc.partitions[partition] = p
	}
	p.records++
	p.bytes += bytes
	s.totalProd.Add(1)

	// Sample every 100th produce for delay tracking
	seq := s.totalProd.Load()
	if seq%100 == 0 {
		if len(s.produceTimes) > 10000 {
			// Evict oldest entries to bound memory
			for k := range s.produceTimes {
				delete(s.produceTimes, k)
				break
			}
		}
		s.produceTimes[seq] = time.Now()
	}
}

func (s *statsAccumulator) recordConsume(topic string, partition int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tc := s.topics[topic]
	if tc == nil {
		return
	}
	tc.consumer.records++
	tc.consumer.bytes += bytes

	p := tc.partitions[partition]
	if p == nil {
		p = &partitionCounters{}
		tc.partitions[partition] = p
	}
	p.consumed++
	s.totalCons.Add(1)
}

func (s *statsAccumulator) recordError(topic string, partition int, phase string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tc := s.topics[topic]
	if tc == nil {
		return
	}
	switch phase {
	case "produce":
		tc.producer.errors++
	case "consume":
		tc.consumer.errors++
	}
	p := tc.partitions[partition]
	if p == nil {
		p = &partitionCounters{}
		tc.partitions[partition] = p
	}
	switch phase {
	case "offset":
		p.offsetGaps++
	case "validate":
		p.seqGaps++
	case "decode":
		p.decodeErrors++
	}
	s.totalErr.Add(1)
}

func (s *statsAccumulator) snapshot(start, end time.Time) snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := snapshot{
		Start:  start,
		End:    end,
		NodeID: s.cfg.NodeID,
		RunID:  s.cfg.RunID,
		Topics: make(map[string]topicSnapshot, len(s.topics)),
	}
	for topic, tc := range s.topics {
		ts := topicSnapshot{
			Producer: phaseSnapshot{
				Records: tc.producer.records,
				Bytes:   tc.producer.bytes,
				Errors:  tc.producer.errors,
			},
			Consumer: phaseSnapshot{
				Records: tc.consumer.records,
				Bytes:   tc.consumer.bytes,
				Errors:  tc.consumer.errors,
			},
			Partitions: make(map[int]partitionSnapshot, len(tc.partitions)),
		}
		if tc.producer.latencyN > 0 {
			avg := tc.producer.latencySum / float64(tc.producer.latencyN)
			ts.Producer.LatencyP50 = avg
			ts.Producer.LatencyP95 = avg
			ts.Producer.LatencyP99 = avg
		}
		for p, pc := range tc.partitions {
			ts.Partitions[p] = partitionSnapshot{
				Records:      pc.records,
				Bytes:        pc.bytes,
				Consumed:     pc.consumed,
				OffsetGaps:   pc.offsetGaps,
				SeqGaps:      pc.seqGaps,
				DecodeErrors: pc.decodeErrors,
			}
		}
		snap.Topics[topic] = ts

		tc.producer = phaseCounters{}
		tc.consumer = phaseCounters{}
		for p := range tc.partitions {
			tc.partitions[p] = &partitionCounters{}
		}
	}
	return snap
}

func (s *statsAccumulator) uploadSnapshot(snap snapshot) {
	if s.cfg.S3Bucket == "" {
		return
	}
	b, err := json.Marshal(snap)
	if err != nil {
		slog.Warn("stats_marshal_failed", "error", err)
		return
	}
	key := fmt.Sprintf("%s/%s/%s/%s.json",
		s.cfg.S3Prefix,
		s.cfg.RunID,
		s.cfg.NodeID,
		snap.Start.UTC().Format("20060102T150405Z"),
	)
	if err := s3PutRetry(key, b); err != nil {
		slog.Warn("stats_upload_failed", "key", key, "error", err)
		return
	}
	slog.Info("stats_uploaded", "key", key, "bytes", len(b))
}

// produceTime returns the wall clock time when approximately seq records had
// been produced, or the zero time if unknown.
func (s *statsAccumulator) produceTime(seq int64) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.produceTimes[seq]; ok {
		return t
	}
	// Linear scan for nearest sampled time
	var bestSeq int64
	var bestTime time.Time
	for k, v := range s.produceTimes {
		if k <= seq && k > bestSeq {
			bestSeq = k
			bestTime = v
		}
	}
	return bestTime
}
