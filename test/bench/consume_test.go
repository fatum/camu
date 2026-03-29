//go:build integration

package bench

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

const consumeBenchTopic = "bench-consume-path"

func benchmarkConsumePath(b *testing.B, limit int, waitForFlush bool) {
	restoreLogger := muteBenchLogs()
	defer restoreLogger()

	env := camutest.New(b, camutest.WithInstances(1))
	defer env.Cleanup()

	client := newAPIBenchClient(b, env.Server(0))
	if err := client.createTopic(consumeBenchTopic, 1, 24*time.Hour); err != nil {
		b.Fatalf("createTopic() error: %v", err)
	}

	const totalMessages = 1000
	for i := 0; i < totalMessages; i++ {
		_, err := client.produce(consumeBenchTopic, []camutest.ProduceMessage{
			{Key: fmt.Sprintf("k-%d", i), Value: fmt.Sprintf("benchmark payload %d", i)},
		})
		if err != nil {
			b.Fatalf("produce() error: %v", err)
		}
	}

	if waitForFlush {
		time.Sleep(6 * time.Second)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.consume(consumeBenchTopic, 0, 0, limit)
		if err != nil {
			b.Fatalf("consume() error: %v", err)
		}
	}
}

func BenchmarkConsumeSegmentLimit1(b *testing.B) {
	benchmarkConsumePath(b, 1, true)
}

func BenchmarkConsumeSegmentLimit100(b *testing.B) {
	benchmarkConsumePath(b, 100, true)
}

func BenchmarkConsumeWALLimit1(b *testing.B) {
	benchmarkConsumePath(b, 1, false)
}

func BenchmarkConsumeWALLimit100(b *testing.B) {
	benchmarkConsumePath(b, 100, false)
}

func muteBenchLogs() func() {
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	return func() {
		slog.SetDefault(old)
	}
}
