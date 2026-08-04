//go:build benchmark

package server

import (
	"runtime"
	"testing"

	"github.com/maksim/camu/internal/log"
)

// BenchmarkParquetExportMemoryPressure supports allocation profiling of the
// complete export path, including committed-record decoding inputs, typed JSON
// conversion, native Parquet encoding, and the in-memory output buffer.
//
// Example:
//
//	go test -tags benchmark ./internal/server -run '^$' -bench BenchmarkParquetExportMemoryPressure \
//	  -benchmem -benchtime=3x -memprofile /tmp/camu-export.mem
func BenchmarkParquetExportMemoryPressure(b *testing.B) {
	messages, schema := parquetBenchmarkMessages()
	b.ReportAllocs()
	b.SetBytes(int64(parquetBenchmarkSourceBytes(messages)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := writeParquetChunk(messages, schema)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(data)
	}
}

func parquetBenchmarkSourceBytes(messages []log.Message) int {
	total := 0
	for _, message := range messages {
		total += len(message.Key) + len(message.Value)
	}
	return total
}
