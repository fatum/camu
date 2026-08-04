package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

// BenchmarkInternalConcurrentAppendConsume measures the internal append and
// committed-read path without HTTP, Docker, or an object-store round trip.
// Capture hotspots with:
//
// go test ./internal/server -run '^$' -bench BenchmarkInternalConcurrentAppendConsume -benchmem -benchtime=5s -cpuprofile /tmp/camu-internal.cpu.pprof
// go tool pprof -top /tmp/camu-internal.cpu.pprof
func BenchmarkInternalConcurrentAppendConsume(b *testing.B) {
	const (
		topic     = "bench-internal-concurrent"
		batchSize = 500
		payloadSz = 1024
		readLimit = 20_000
		readBytes = 4 << 20
	)

	pm := newConcurrentBenchmarkPartitionManager(b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc := meta.TopicConfig{
		Name:              topic,
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
	}
	if err := pm.InitTopic(ctx, tc, map[int]uint64{}); err != nil {
		b.Fatalf("InitTopic() error: %v", err)
	}

	payload := make([]byte, payloadSz)
	for i := range payload {
		payload[i] = 'x'
	}

	producerResult := make(chan error, 1)
	b.ReportAllocs()
	b.SetBytes(payloadSz)
	b.ResetTimer()
	go func() {
		for first := 0; first < b.N; first += batchSize {
			last := min(first+batchSize, b.N)
			batch := make([]log.Message, 0, last-first)
			for sequence := first; sequence < last; sequence++ {
				batch = append(batch, log.Message{
					Key:   []byte(fmt.Sprintf("key-%d", sequence)),
					Value: payload,
				})
			}
			if _, err := pm.AppendBatch(ctx, topic, 0, batch); err != nil {
				producerResult <- err
				return
			}
		}
		producerResult <- nil
	}()

	var offset uint64
	var consumed, reads int
	var producerComplete bool
	for consumed < b.N {
		raw, _, err := pm.ReadRawBatches(ctx, topic, 0, int64(offset), readBytes)
		if err != nil {
			b.Fatalf("ReadRawBatches() error: %v", err)
		}
		if len(raw) == 0 {
			if !producerComplete {
				select {
				case err := <-producerResult:
					producerComplete = true
					if err != nil {
						b.Fatalf("AppendBatch() error: %v", err)
					}
				default:
				}
			}
			if producerComplete {
				b.Fatalf("producer completed at offset %d; consumed %d of %d records", offset, consumed, b.N)
			}
			time.Sleep(time.Millisecond)
			continue
		}
		reads++
		messages, err := decodeCommittedPage(raw, offset, readLimit)
		if err != nil {
			b.Fatalf("decodeCommittedPage() error: %v", err)
		}
		if len(messages) == 0 {
			b.Fatalf("decodeCommittedPage() returned no messages for %d raw bytes", len(raw))
		}
		offset = messages[len(messages)-1].Offset + 1
		consumed += len(messages)
	}
	if !producerComplete {
		if err := <-producerResult; err != nil {
			b.Fatalf("AppendBatch() error: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(reads), "reads")
	b.ReportMetric(float64(reads)/float64(consumed), "reads/record")
}

func newConcurrentBenchmarkPartitionManager(b *testing.B) *PartitionManager {
	b.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{Bucket: "benchmark", Endpoint: "memory://"})
	if err != nil {
		b.Fatalf("NewS3Client() error: %v", err)
	}
	cfg := &config.Config{}
	cfg.Cache.Directory = filepath.Join(b.TempDir(), "cache")
	cfg.Segments.MaxSize = 1 << 40 // Keep the concurrent workload in the active segment.
	cfg.Segments.MaxAge = "1h"
	pm, err := NewPartitionManager(cfg, s3Client)
	if err != nil {
		b.Fatalf("NewPartitionManager() error: %v", err)
	}
	b.Cleanup(func() {
		if err := pm.Shutdown(context.Background()); err != nil {
			b.Errorf("Shutdown() error: %v", err)
		}
	})
	return pm
}
