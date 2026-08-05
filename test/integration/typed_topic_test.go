//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/pkg/camutest"
)

func TestIntegrationTypedTopicExportSQL(t *testing.T) {
	enabled := true
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithConfigMutator(func(cfg *config.Config) {
		cfg.Coordination.HeartbeatInterval = "500ms"
		cfg.Segments.MaxAge = "1s"
		cfg.SQL.Enabled = &enabled
		cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "cache")
		cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "tmp")
	}))
	defer env.Cleanup()
	c := env.Client()
	dlq, topic := "typed-dlq", "typed-orders"
	if err := c.CreateTopic(dlq, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{"name": topic, "partitions": 1, "retention": "1h", "export_enabled": true, "schema": map[string]any{"encoding": "json", "dead_letter_topic": dlq, "fields": []map[string]any{{"name": "id", "type": "int64", "path": "$.id"}}}})
	resp, err := http.Post(c.BaseURL()+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create typed topic status %d", resp.StatusCode)
	}
	if _, err := c.Produce(topic, []camutest.ProduceMessage{{Key: "a", Value: `{"id":7}`}}); err != nil {
		t.Fatal(err)
	}
	bad, _ := json.Marshal([]camutest.ProduceMessage{{Key: "bad", Value: `{"id":"not-an-int"}`}})
	badResp, err := http.Post(c.BaseURL()+"/v1/topics/"+topic+"/messages", "application/json", bytes.NewReader(bad))
	if err != nil {
		t.Fatal(err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed typed produce status %d, want 400", badResp.StatusCode)
	}
	cr, err := c.Consume(topic, 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cr.Messages) != 1 {
		t.Fatalf("native typed records = %d, want 1 after rejected publish", len(cr.Messages))
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		r, e := c.SQLQuery(camutest.SQLQueryRequest{SQL: `select id from "typed-orders"`, Topics: []string{topic}})
		if e == nil && len(r.Rows) == 1 && len(r.Columns) == 1 && r.Columns[0].Name == "id" && r.Columns[0].Type != "" {
			if got, ok := r.Rows[0][0].(float64); !ok || got != 7 {
				t.Fatalf("typed id = %#v, want 7", r.Rows[0][0])
			}
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("typed record was not exported and queryable")
}

func TestIntegrationTypedTopicOpaqueKafkaDecodeFailureDLQ(t *testing.T) {
	enabled := true
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithConfigMutator(func(cfg *config.Config) {
		cfg.Server.KafkaPort = kafkaPort
		cfg.Coordination.HeartbeatInterval = "500ms"
		cfg.Segments.MaxAge = "1s"
		cfg.SQL.Enabled = &enabled
		cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "cache")
		cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "tmp")
	}))
	defer env.Cleanup()

	httpClient := env.Client()
	dlq, topic := "typed-kafka-dlq", "typed-kafka-orders"
	if err := httpClient.CreateTopic(dlq, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{
		"name": topic, "partitions": 1, "retention": "1h", "export_enabled": true,
		"schema": map[string]any{
			"encoding": "json", "dead_letter_topic": dlq,
			"fields": []map[string]any{{"name": "id", "type": "int64", "path": "$.id"}},
		},
	})
	resp, err := http.Post(httpClient.BaseURL()+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create typed topic status %d", resp.StatusCode)
	}

	broker := fmt.Sprintf("127.0.0.1:%d", kafkaPort)
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.MaxVersions(kversion.V1_0_0()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	badKey := []byte{0xfe, 0x01}
	badValue := []byte{0xff, 0x00, '{'}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: badKey, Value: badValue})
	if err := result.FirstErr(); err != nil {
		t.Fatalf("opaque Kafka produce error: %v", err)
	}

	type deadLetter struct {
		SourceTopic     string `json:"source_topic"`
		SourcePartition int    `json:"source_partition"`
		SourceOffset    uint64 `json:"source_offset"`
		SchemaEncoding  string `json:"schema_encoding"`
		Error           string `json:"error"`
		OriginalKey     string `json:"original_key"`
		OriginalValue   string `json:"original_value"`
	}
	var got deadLetter
	var count int
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		cr, consumeErr := httpClient.Consume(dlq, 0, 0, 10)
		if consumeErr == nil && len(cr.Messages) > 0 {
			count = len(cr.Messages)
			if err := json.Unmarshal([]byte(cr.Messages[0].Value), &got); err != nil {
				t.Fatalf("decode DLQ payload: %v", err)
			}
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if count != 1 {
		t.Fatalf("DLQ records = %d, want exactly one", count)
	}
	if got.SourceTopic != topic || got.SourcePartition != 0 || got.SourceOffset != 0 {
		t.Fatalf("DLQ origin = %s/%d/%d, want %s/0/0", got.SourceTopic, got.SourcePartition, got.SourceOffset, topic)
	}
	if got.SchemaEncoding != "json" || got.Error == "" {
		t.Fatalf("DLQ schema failure metadata = encoding %q error %q", got.SchemaEncoding, got.Error)
	}
	decodedKey, err := base64.StdEncoding.DecodeString(got.OriginalKey)
	if err != nil || !bytes.Equal(decodedKey, badKey) {
		t.Fatalf("DLQ original key = %q, want exact bytes %x", got.OriginalKey, badKey)
	}
	decodedValue, err := base64.StdEncoding.DecodeString(got.OriginalValue)
	if err != nil || !bytes.Equal(decodedValue, badValue) {
		t.Fatalf("DLQ original value = %q, want exact bytes %x", got.OriginalValue, badValue)
	}

	// A subsequent maintenance pass must not replay the same source offset.
	time.Sleep(2 * time.Second)
	cr, err := httpClient.Consume(dlq, 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cr.Messages) != 1 {
		t.Fatalf("DLQ records after replay window = %d, want exactly one", len(cr.Messages))
	}
}

func TestIntegrationTypedTopicOpaqueKafkaValidValuePhysicalParquet(t *testing.T) {
	enabled := true
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithConfigMutator(func(cfg *config.Config) {
		cfg.Server.KafkaPort = kafkaPort
		cfg.Coordination.HeartbeatInterval = "500ms"
		cfg.Segments.MaxAge = "1s"
		cfg.SQL.Enabled = &enabled
		cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "cache")
		cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "tmp")
	}))
	defer env.Cleanup()
	httpClient := env.Client()
	topic := "typed-kafka-valid"
	body, _ := json.Marshal(map[string]any{
		"name": topic, "partitions": 1, "retention": "1h", "export_enabled": true,
		"schema": map[string]any{"encoding": "json", "fields": []map[string]any{
			{"name": "id", "type": "int64", "path": "$.id"},
			{"name": "paid", "type": "bool", "path": "$.paid"},
		}},
	})
	resp, err := http.Post(httpClient.BaseURL()+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create typed topic status %d", resp.StatusCode)
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", kafkaPort)), kgo.MaxVersions(kversion.V1_0_0()), kgo.DisableIdempotentWrite())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if result := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte(`{"id":42,"paid":true}`)}); result.FirstErr() != nil {
		t.Fatalf("opaque Kafka produce error: %v", result.FirstErr())
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, e := httpClient.SQLQuery(camutest.SQLQueryRequest{SQL: `select id, paid from "typed-kafka-valid"`, Topics: []string{topic}})
		if e == nil && len(r.Rows) == 1 && len(r.Columns) == 2 {
			if got, ok := r.Rows[0][0].(float64); !ok || got != 42 {
				t.Fatalf("typed id = %#v, want 42", r.Rows[0][0])
			}
			if got, ok := r.Rows[0][1].(bool); !ok || !got {
				t.Fatalf("typed paid = %#v, want true", r.Rows[0][1])
			}
			if r.Columns[0].Type == "" || r.Columns[1].Type == "" {
				t.Fatal("typed SQL columns have empty types")
			}
			store := pipeline.NewCheckpointStore(env.S3Client(), pipeline.NoFence{})
			if _, e := store.Load(ctx, "parquet-export", topic, 0); e != nil {
				t.Fatalf("Parquet pipeline checkpoint missing: %v", e)
			}
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("valid opaque Kafka record was not exported as typed Parquet")
}

func TestIntegrationTypedTopicOpaqueKafkaDecodeSkipAdvancesCheckpoint(t *testing.T) {
	enabled := true
	kafkaPort := freeTCPPort(t)
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithConfigMutator(func(cfg *config.Config) {
		cfg.Server.KafkaPort = kafkaPort
		cfg.Coordination.HeartbeatInterval = "500ms"
		cfg.Segments.MaxAge = "1s"
		cfg.SQL.Enabled = &enabled
		cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "cache")
		cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "tmp")
	}))
	defer env.Cleanup()
	httpClient := env.Client()
	topic := "typed-kafka-skip"
	body, _ := json.Marshal(map[string]any{"name": topic, "partitions": 1, "retention": "1h", "export_enabled": true, "schema": map[string]any{"encoding": "json", "fields": []map[string]any{{"name": "id", "type": "int64", "path": "$.id"}}}})
	resp, err := http.Post(httpClient.BaseURL()+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create typed topic status %d", resp.StatusCode)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", kafkaPort)), kgo.MaxVersions(kversion.V1_0_0()), kgo.DisableIdempotentWrite())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if result := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte{0xff, 0x00, '{'}}); result.FirstErr() != nil {
		t.Fatalf("opaque Kafka produce error: %v", result.FirstErr())
	}
	store := pipeline.NewCheckpointStore(env.S3Client(), pipeline.NoFence{})
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cp, e := store.Load(ctx, "parquet-export", topic, 0)
		if e == nil && cp.NextOffset > 0 {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("malformed opaque record did not advance the Parquet pipeline checkpoint")
}

// TestIntegrationDisklessTypedTopicExportSQL verifies that a diskless topic with
// export_enabled and a typed schema is exported to Parquet and queryable via SQL.
func TestIntegrationDisklessTypedTopicExportSQL(t *testing.T) {
	enabled := true
	env := camutest.New(t, camutest.WithInstances(1), camutest.WithConfigMutator(func(cfg *config.Config) {
		cfg.Coordination.HeartbeatInterval = "500ms"
		cfg.Segments.MaxAge = "1s"
		cfg.SQL.Enabled = &enabled
		cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "cache")
		cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "tmp")
	}))
	defer env.Cleanup()
	c := env.Client()
	topic := "diskless-typed-orders"
	body, _ := json.Marshal(map[string]any{"name": topic, "partitions": 1, "retention": "1h", "export_enabled": true, "storage_mode": "diskless", "schema": map[string]any{"encoding": "json", "fields": []map[string]any{{"name": "id", "type": "int64", "path": "$.id"}}}})
	resp, err := http.Post(c.BaseURL()+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create diskless typed topic status %d, want 201", resp.StatusCode)
	}
	if _, err := c.ProduceToPartition(topic, 0, []camutest.ProduceMessage{{Key: "a", Value: `{"id":7}`}}); err != nil {
		t.Fatal(err)
	}
	// A malformed typed produce must be rejected even for diskless topics.
	bad, _ := json.Marshal([]camutest.ProduceMessage{{Key: "bad", Value: `{"id":"not-an-int"}`}})
	badResp, err := http.Post(c.BaseURL()+"/v1/topics/"+topic+"/partitions/0/messages", "application/json", bytes.NewReader(bad))
	if err != nil {
		t.Fatal(err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed diskless typed produce status %d, want 400", badResp.StatusCode)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, e := c.SQLQuery(camutest.SQLQueryRequest{SQL: `select id from "` + topic + `"`, Topics: []string{topic}})
		if e == nil && len(r.Rows) == 1 && len(r.Columns) == 1 && r.Columns[0].Name == "id" && r.Columns[0].Type != "" {
			if got, ok := r.Rows[0][0].(float64); !ok || got != 7 {
				t.Fatalf("diskless typed id = %#v, want 7", r.Rows[0][0])
			}
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("diskless typed record was not exported and queryable")
}
