package iceberg

import (
	"context"
	"testing"
	"time"

	"github.com/maksim/camu/internal/meta"
)

var benchTime = time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

func benchJSONSchema() *meta.TopicSchema {
	return &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}}
}

func benchAvroSchema() *meta.TopicSchema {
	return &meta.TopicSchema{Encoding: "avro", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}}
}

func benchProtobufSchema() *meta.TopicSchema {
	return &meta.TopicSchema{Encoding: "protobuf", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}}
}

const benchJSONValue = `{"name":"alpha","count":7,"ratio":1.5,"enabled":true,"occurred_at":"2026-08-06T12:00:00Z"}`

func BenchmarkDecodeJSONTypedFields(b *testing.B) {
	schema := benchJSONSchema()
	input := []byte(benchJSONValue)
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	for i := 0; i < b.N; i++ {
		if _, err := DecodeTypedFields(context.Background(), "t", schema, nil, input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeAvroTypedFields(b *testing.B) {
	schema := benchAvroSchema()
	payload, err := EncodeAvroValue(schema, map[string]any{
		"name": "alpha", "count": int64(7), "ratio": 1.5, "enabled": true,
		"occurred_at": benchTime,
	})
	if err != nil {
		b.Fatal(err)
	}
	input := AvroWrap(0, payload)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, err := DecodeAvroTypedFields(context.Background(), "t", schema, nil, input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeProtobufTypedFields(b *testing.B) {
	schema := benchProtobufSchema()
	payload, err := EncodeProtobufValue(schema, map[string]any{
		"name": "alpha", "count": int64(7), "ratio": 1.5, "enabled": true,
		"occurred_at": benchTime,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, err := DecodeProtobufTypedFields(context.Background(), "t", schema, nil, payload); err != nil {
			b.Fatal(err)
		}
	}
}
