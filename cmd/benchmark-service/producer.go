package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const batchSize = 5000

type typedRecord struct {
	RunID   string `json:"run_id"`
	Seq     int64  `json:"seq"`
	Payload string `json:"payload"`
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("json marshal: %v", err))
	}
	return b
}

func newKafkaProducer(cfg serviceConfig) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.RecordDeliveryTimeout(5 * time.Minute),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
}

// produceLoop runs one of producerConcurrency concurrent produce loops. Each
// loop owns a disjoint subset of partitions ({i, i+N, ...}) and writes them in
// sequence order, so a partition's records are produced by exactly one loop in
// monotonic seq order and the consumer can validate seq contiguity per
// partition even with multiple clients sharing the topic. A shared token
// bucket paces the aggregate rate to BENCHMARK_RATE.
func produceLoop(ctx context.Context, cfg serviceConfig, topic string, stats *statsAccumulator, limiter *rateLimiter, loopIndex, loopCount int) {
	slog.Info("producer_starting", "topic", topic, "batch", batchSize, "loop", loopIndex, "of", loopCount)
	defer slog.Info("producer_stopped", "topic", topic, "loop", loopIndex)

	var kafkaClient *kgo.Client
	if err := retryWithBackoff(ctx, "producer_kafka_client", 5, func() error {
		var err error
		kafkaClient, err = newKafkaProducer(cfg)
		return err
	}); err != nil {
		slog.Error("producer_kafka_client_failed", "topic", topic, "error", err)
		return
	}
	defer kafkaClient.Close()

	payloadText := strings.Repeat("x", int(cfg.MessageBytes))
	// Partitions owned by this loop: p % loopCount == loopIndex.
	myPartitions := make([]int32, 0, cfg.Partitions/loopCount+1)
	for p := loopIndex; p < cfg.Partitions; p += loopCount {
		myPartitions = append(myPartitions, int32(p))
	}
	// Per-partition sequence index: partition p carries seqs p, p+P, p+2P, ...
	seqIndex := make([]int64, len(myPartitions))
	lastLog := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Pace the batch against the shared rate limit before sending.
		if err := limiter.wait(ctx, batchSize); err != nil {
			return
		}

		records := make([]*kgo.Record, 0, batchSize)
		seqs := make([]int64, 0, batchSize)
		for len(records) < batchSize {
			for pi, partition := range myPartitions {
				if len(records) >= batchSize {
					break
				}
				seq := int64(partition) + seqIndex[pi]*int64(cfg.Partitions)
				seqIndex[pi]++
				value := typedRecord{
					RunID:   cfg.RunID,
					Seq:     seq,
					Payload: payloadText,
				}
				key := cfg.RunID + ":" + strconv.FormatInt(seq, 10)
				records = append(records, &kgo.Record{
					Topic:     topic,
					Partition: partition,
					Key:       []byte(key),
					Value:     mustJSON(value),
				})
				seqs = append(seqs, seq)
			}
		}

		started := time.Now()
		results := kafkaClient.ProduceSync(ctx, records...)
		latency := time.Since(started)
		for i, r := range results {
			if r.Err != nil {
				stats.recordError(topic, int(r.Record.Partition), "produce")
				slog.Warn("produce_error", "topic", topic, "partition", r.Record.Partition, "seq", seqs[i], "error", r.Err)
			} else {
				stats.recordProduce(topic, int(r.Record.Partition), int64(len(r.Record.Value)), latency)
			}
		}

		if time.Since(lastLog) > 10*time.Second {
			total := stats.totalProd.Load()
			slog.Info("produce_progress", "topic", topic, "loop", loopIndex, "records", total, "batch_latency_ms", latency.Milliseconds())
			lastLog = time.Now()
		}
	}
}

// rateLimiter is a shared token bucket pacing records/sec across all produce
// loops. Tokens accrue continuously up to one second's worth of burst.
type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	tokens float64
	last   time.Time
}

func newRateLimiter(rate int) *rateLimiter {
	return &rateLimiter{rate: float64(rate), tokens: float64(rate), last: time.Now()}
}

// wait blocks until n tokens are available or ctx is done.
func (l *rateLimiter) wait(ctx context.Context, n int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		now := time.Now()
		l.tokens += now.Sub(l.last).Seconds() * l.rate
		l.last = now
		if l.tokens > l.rate {
			l.tokens = l.rate // cap burst at one second of capacity
		}
		if l.tokens >= float64(n) {
			l.tokens -= float64(n)
			return nil
		}
		need := float64(n) - l.tokens
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			l.mu.Lock()
			return ctx.Err()
		case <-time.After(time.Duration(need / l.rate * float64(time.Second))):
		}
		l.mu.Lock()
	}
}
