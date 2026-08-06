package server

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/meta"
)

var benchValidationTime = time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

func BenchmarkValidateTypedValueJSON(b *testing.B) {
	srv := newTestServer(b)
	ctx := context.Background()
	tc := topicCfgWithSchema(&meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}})
	value := `{"name":"alpha","count":7,"ratio":1.5,"enabled":true,"occurred_at":"2026-08-06T12:00:00Z"}`
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		if err := srv.validateTypedValue(ctx, tc, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateTypedValueAvro(b *testing.B) {
	srv := newTestServer(b)
	ctx := context.Background()
	schema := &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}}
	raw, err := iceberg.EncodeAvroValue(schema, map[string]any{
		"name": "alpha", "count": int64(7), "ratio": 1.5, "enabled": true, "occurred_at": benchValidationTime,
	})
	if err != nil {
		b.Fatal(err)
	}
	value := base64.StdEncoding.EncodeToString(raw)
	tc := topicCfgWithSchema(schema)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		if err := srv.validateTypedValue(ctx, tc, value); err != nil {
			b.Fatal(err)
		}
	}
}
