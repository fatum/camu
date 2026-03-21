//go:build integration

package bench

import (
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func BenchmarkProduceThroughput(b *testing.B) {
	env := camutest.New(b, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("bench-produce", 4, 24*time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Produce("bench-produce", []camutest.ProduceMessage{
			{Key: fmt.Sprintf("key-%d", i), Value: "benchmark payload data here"},
		})
		if err != nil {
			b.Fatalf("Produce() error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkProduceBatchThroughput(b *testing.B) {
	env := camutest.New(b, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("bench-batch", 4, 24*time.Hour)

	batchSize := 100
	batch := make([]camutest.ProduceMessage, batchSize)
	for i := range batch {
		batch[i] = camutest.ProduceMessage{Key: fmt.Sprintf("k%d", i), Value: "payload data"}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Produce("bench-batch", batch)
		if err != nil {
			b.Fatalf("Produce() error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkConsumeThroughput(b *testing.B) {
	env := camutest.New(b, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("bench-consume", 1, 24*time.Hour)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		client.Produce("bench-consume", []camutest.ProduceMessage{
			{Key: "k", Value: "benchmark payload data here"},
		})
	}
	time.Sleep(6 * time.Second) // wait for flush

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Consume("bench-consume", 0, 0, 100)
		if err != nil {
			b.Fatalf("Consume() error: %v", err)
		}
	}
}
