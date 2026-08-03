package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
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

	var records int64
	for records < count {
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
			progress(1)
		})
		if err != nil {
			return phaseResult{}, err
		}
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
