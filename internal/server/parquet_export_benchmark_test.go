//go:build benchmark

package server

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
)

func BenchmarkWriteParquetChunk(b *testing.B) {
	messages, schema := parquetBenchmarkMessages()
	b.ReportAllocs()
	b.SetBytes(16_384 * 1_024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := writeParquetChunk(messages, schema)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(data)
	}
}

func parquetBenchmarkMessages() ([]log.Message, *meta.TopicSchema) {
	const (
		records     = 16_384
		payloadSize = 1_024
	)
	schema := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "id", Type: "int64", Path: "$.id"},
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}}
	messages := make([]log.Message, records)
	for i := range messages {
		value := fmt.Sprintf(`{"id":%d,"name":"event-%d","enabled":true,"occurred_at":"2026-08-03T12:00:00Z","payload":"%s"}`, i, i, strings.Repeat("x", payloadSize-100))
		messages[i] = log.Message{Offset: uint64(i), Timestamp: time.Now().UnixMilli(), Value: []byte(value)}
	}
	return messages, schema
}
