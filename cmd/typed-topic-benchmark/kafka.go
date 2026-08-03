package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func newKafkaClient(cfg config, consumer bool) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.RecordDeliveryTimeout(2 * time.Minute),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
	if consumer {
		// Keep the client and server fetch budgets aligned: the server returns at
		// most 16 MiB per partition, and this client can receive four such pages.
		opts = append(opts,
			kgo.FetchMaxPartitionBytes(16<<20),
			kgo.FetchMaxBytes(64<<20),
		)
		partitions := make(map[int32]kgo.Offset, cfg.Partitions)
		for partition := 0; partition < cfg.Partitions; partition++ {
			partitions[int32(partition)] = kgo.NewOffset().At(0)
		}
		opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: partitions}))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	return client, nil
}

func produceKafka(ctx context.Context, cfg config, count int64, expected []hashState, progress func(int64)) (phaseResult, error) {
	start := time.Now()
	client, err := newKafkaClient(cfg, false)
	if err != nil {
		return phaseResult{}, err
	}
	defer client.Close()

	var total int64
	var serialized int64
	var wg sync.WaitGroup
	errs := make(chan error, cfg.Partitions)
	jobs := make(chan int)
	workers := cfg.ProducerConcurrency
	if workers > cfg.Partitions {
		workers = cfg.Partitions
	}
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partition := range jobs {
				for first := firstSequenceForPartition(cfg.SequenceStart, partition, cfg.Partitions); first < cfg.SequenceStart+count; first += int64(cfg.Partitions * cfg.BatchMessages) {
					records := make([]*kgo.Record, 0, cfg.BatchMessages)
					for sequence := first; sequence < cfg.SequenceStart+count && len(records) < cfg.BatchMessages; sequence += int64(cfg.Partitions) {
						value := typedValue{ID: sequence, Payload: payload(cfg.MessageBytes), PayloadBytes: cfg.MessageBytes, Sequence: sequence}
						key := strconv.FormatInt(sequence, 10)
						valueBytes := mustJSON(value)
						records = append(records, &kgo.Record{Topic: cfg.Topic, Partition: int32(partition), Key: []byte(key), Value: valueBytes})
						expected[partition].add(value)
						atomic.AddInt64(&serialized, int64(len(key)+len(valueBytes)))
					}
					results := client.ProduceSync(ctx, records...)
					for _, result := range results {
						if result.Err != nil {
							errs <- fmt.Errorf("produce partition %d: %w", partition, result.Err)
							return
						}
					}
					atomic.AddInt64(&total, int64(len(records)))
					progress(int64(len(records)))
				}
			}
		}()
	}
	for partition := 0; partition < cfg.Partitions; partition++ {
		jobs <- partition
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return phaseResult{}, err
		}
	}

	duration := time.Since(start)
	return phaseResult{
		Records:          total,
		Bytes:            total * cfg.MessageBytes,
		SerializedBytes:  serialized,
		DurationSeconds:  duration.Seconds(),
		RecordsPerSecond: float64(total) / duration.Seconds(),
		BytesPerSecond:   float64(total*cfg.MessageBytes) / duration.Seconds(),
	}, nil
}

func consumeKafka(ctx context.Context, cfg config, expected []hashState, actual []hashState, count int64, progress func(int64)) (phaseResult, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.ConsumeTimeout)
	defer cancel()
	start := time.Now()
	client, err := newKafkaClient(cfg, true)
	if err != nil {
		return phaseResult{}, err
	}
	defer client.Close()
	reporter := newKafkaConsumeProgress(cfg, expected, count, start, benchmarkLog)
	reporter.startup()

	var records int64
	var bytesRead int64
	partitionRecords := make([]int64, cfg.Partitions)
	for records < count {
		reporter.beginPoll()
		pollStarted := time.Now()
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			return phaseResult{}, fmt.Errorf("consume Kafka: %v", errs[0].Err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if records >= count {
				return
			}
			var value typedValue
			if decodeErr := json.Unmarshal(record.Value, &value); decodeErr != nil {
				err = decodeErr
				return
			}
			partition := int(record.Partition)
			if partition < 0 || partition >= len(actual) {
				err = fmt.Errorf("consume Kafka: invalid partition %d", partition)
				return
			}
			actual[partition].add(value)
			records++
			bytesRead += value.PayloadBytes
			partitionRecords[partition]++
			reporter.record(partition, record.Offset)
			progress(1)
		})
		if err != nil {
			return phaseResult{}, err
		}
		reporter.poll(time.Now(), time.Since(pollStarted), records, bytesRead, partitionRecords)
	}

	var totalBytes int64
	h := make([]byte, 0, len(actual)*32)
	for partition := range actual {
		_, bytesRead, digest, digestErr := actual[partition].result()
		if digestErr != nil {
			return phaseResult{}, digestErr
		}
		totalBytes += bytesRead
		digestBytes, decodeErr := hex.DecodeString(digest)
		if decodeErr != nil {
			return phaseResult{}, decodeErr
		}
		h = append(h, digestBytes...)
	}
	duration := time.Since(start)
	digest := sha256.Sum256(h)
	return phaseResult{
		Records:          count,
		Bytes:            totalBytes,
		DurationSeconds:  duration.Seconds(),
		RecordsPerSecond: float64(count) / duration.Seconds(),
		BytesPerSecond:   float64(totalBytes) / duration.Seconds(),
		Digest:           hex.EncodeToString(digest[:]),
	}, nil
}

const kafkaConsumeLogInterval = time.Second
const kafkaConsumeSlowPollThreshold = time.Second

// kafkaConsumeProgress keeps benchmark diagnostics useful without turning a
// high-throughput fetch loop into a logging workload. It has no role in
// consumer accounting or timeout handling.
type kafkaConsumeProgress struct {
	cfg               config
	expected          []int64
	totalExpected     int64
	started           time.Time
	lastAggregate     time.Time
	lastEmpty         time.Time
	lastSlow          time.Time
	lastPartitionLogs []time.Time
	fetchRecords      []int
	lastOffsets       []int64
	activePartitions  []int
	logf              func(string, ...any)
}

func newKafkaConsumeProgress(cfg config, expected []hashState, totalExpected int64, started time.Time, logf func(string, ...any)) *kafkaConsumeProgress {
	partitionExpected := make([]int64, cfg.Partitions)
	for partition := range partitionExpected {
		if partition < len(expected) {
			partitionExpected[partition] = expected[partition].recordsSnapshot()
		}
	}
	lastPartitionLogs := make([]time.Time, cfg.Partitions)
	lastOffsets := make([]int64, cfg.Partitions)
	for partition := range lastPartitionLogs {
		lastPartitionLogs[partition] = started
		lastOffsets[partition] = -1
	}
	return &kafkaConsumeProgress{cfg: cfg, expected: partitionExpected, totalExpected: totalExpected, started: started, lastAggregate: started, lastEmpty: started, lastSlow: started, lastPartitionLogs: lastPartitionLogs, fetchRecords: make([]int, cfg.Partitions), lastOffsets: lastOffsets, activePartitions: make([]int, 0, cfg.Partitions), logf: logf}
}

func (p *kafkaConsumeProgress) startup() {
	p.logf("kafka consume starting brokers=%s topic=%s partitions=%d expected_records=%d", strings.Join(p.cfg.KafkaBrokers, ","), p.cfg.Topic, p.cfg.Partitions, p.totalExpected)
	for partition, expected := range p.expected {
		p.logf("kafka consume partition=%d expected_records=%d", partition, expected)
	}
}

// beginPoll clears only partitions that returned records in the prior fetch.
// This avoids allocating or sweeping a partition-sized diagnostics slice for
// every PollFetches call.
func (p *kafkaConsumeProgress) beginPoll() {
	for _, partition := range p.activePartitions {
		p.fetchRecords[partition] = 0
		p.lastOffsets[partition] = -1
	}
	p.activePartitions = p.activePartitions[:0]
}

func (p *kafkaConsumeProgress) record(partition int, offset int64) {
	if p.fetchRecords[partition] == 0 {
		p.activePartitions = append(p.activePartitions, partition)
	}
	p.fetchRecords[partition]++
	p.lastOffsets[partition] = offset
}

func (p *kafkaConsumeProgress) poll(now time.Time, pollDuration time.Duration, records, bytesRead int64, partitionRecords []int64) {
	if now.Sub(p.lastAggregate) >= kafkaConsumeLogInterval {
		elapsed := now.Sub(p.started).Seconds()
		rate := float64(0)
		if elapsed > 0 {
			rate = float64(records) / elapsed
		}
		p.logf("kafka consume progress records=%d/%d bytes=%d rate=%.2f records/s", records, p.totalExpected, bytesRead, rate)
		p.lastAggregate = now
	}

	for _, partition := range p.activePartitions {
		fetchCount := p.fetchRecords[partition]
		if now.Sub(p.lastPartitionLogs[partition]) < kafkaConsumeLogInterval {
			continue
		}
		p.logf("kafka consume partition=%d records=%d expected=%d last_offset=%d fetch_records=%d", partition, partitionRecords[partition], p.expected[partition], p.lastOffsets[partition], fetchCount)
		p.lastPartitionLogs[partition] = now
	}
	if len(p.activePartitions) == 0 && now.Sub(p.lastEmpty) >= kafkaConsumeLogInterval {
		p.logf("kafka consume empty poll duration=%s records=%d/%d", pollDuration, records, p.totalExpected)
		p.lastEmpty = now
	}
	if pollDuration >= kafkaConsumeSlowPollThreshold && now.Sub(p.lastSlow) >= kafkaConsumeLogInterval {
		p.logf("kafka consume slow poll duration=%s records=%d/%d fetch_records=%d", pollDuration, records, p.totalExpected, p.fetchRecordCount())
		p.lastSlow = now
	}
}

func (p *kafkaConsumeProgress) fetchRecordCount() int {
	total := 0
	for _, partition := range p.activePartitions {
		total += p.fetchRecords[partition]
	}
	return total
}
