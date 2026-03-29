//go:build integration

package bench

import (
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

func BenchmarkProduceThroughput(b *testing.B) {
	restoreLogger := muteBenchLogs()
	defer restoreLogger()

	env := camutest.New(b, camutest.WithInstances(1))
	defer env.Cleanup()
	client := newAPIBenchClient(b, env.Server(0))

	if err := client.createTopic("bench-produce", 1, 24*time.Hour); err != nil {
		b.Fatalf("createTopic() error: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.produceToPartition("bench-produce", 0, []camutest.ProduceMessage{
			{Key: fmt.Sprintf("key-%d", i), Value: "benchmark payload data here"},
		})
		if err != nil {
			b.Fatalf("produce() error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkProduceBatchThroughput(b *testing.B) {
	restoreLogger := muteBenchLogs()
	defer restoreLogger()

	env := camutest.New(b, camutest.WithInstances(1))
	defer env.Cleanup()
	client := newAPIBenchClient(b, env.Server(0))

	if err := client.createTopic("bench-batch", 1, 24*time.Hour); err != nil {
		b.Fatalf("createTopic() error: %v", err)
	}

	batchSize := 100
	batch := make([]camutest.ProduceMessage, batchSize)
	for i := range batch {
		batch[i] = camutest.ProduceMessage{Key: fmt.Sprintf("k%d", i), Value: "payload data"}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.produceToPartition("bench-batch", 0, batch)
		if err != nil {
			b.Fatalf("produce() error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkProduceBatchThroughputNoFlush(b *testing.B) {
	restoreLogger := muteBenchLogs()
	defer restoreLogger()

	env := camutest.New(b,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Segments.MaxSize = 1 << 40
			cfg.Segments.MaxAge = "24h"
		}),
	)
	defer env.Cleanup()
	client := newAPIBenchClient(b, env.Server(0))

	if err := client.createTopic("bench-batch-noflush", 1, 24*time.Hour); err != nil {
		b.Fatalf("createTopic() error: %v", err)
	}

	batchSize := 100
	batch := make([]camutest.ProduceMessage, batchSize)
	for i := range batch {
		batch[i] = camutest.ProduceMessage{Key: fmt.Sprintf("k%d", i), Value: "payload data"}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.produceToPartition("bench-batch-noflush", 0, batch)
		if err != nil {
			b.Fatalf("produce() error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkConsumeThroughput(b *testing.B) {
	restoreLogger := muteBenchLogs()
	defer restoreLogger()

	env := camutest.New(b, camutest.WithInstances(1))
	defer env.Cleanup()
	client := newAPIBenchClient(b, env.Server(0))

	if err := client.createTopic("bench-consume", 1, 24*time.Hour); err != nil {
		b.Fatalf("createTopic() error: %v", err)
	}

	// Pre-populate
	for i := 0; i < 1000; i++ {
		_, err := client.produceToPartition("bench-consume", 0, []camutest.ProduceMessage{
			{Key: "k", Value: "benchmark payload data here"},
		})
		if err != nil {
			b.Fatalf("produce() error: %v", err)
		}
	}
	time.Sleep(6 * time.Second) // wait for flush

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.consume("bench-consume", 0, 0, 100)
		if err != nil {
			b.Fatalf("consume() error: %v", err)
		}
	}
}
