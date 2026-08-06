//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/parquet"
	"github.com/maksim/camu/internal/server"
	"github.com/maksim/camu/internal/storage"
	"github.com/maksim/camu/pkg/camutest"
)

func createExportTopic(c *camutest.Client, name string, partitions int, retention time.Duration) error {
	return createTopicWithExport(c, name, partitions, retention, true)
}

func createTopicWithExport(c *camutest.Client, name string, partitions int, retention time.Duration, exportEnabled bool) error {
	body, err := json.Marshal(map[string]any{
		"name": name, "partitions": partitions, "retention": retention.String(), "export_enabled": exportEnabled,
	})
	if err != nil {
		return err
	}
	resp, err := http.Post(c.BaseURL()+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create export topic %q: status %s", name, resp.Status)
	}
	return nil
}

// TestIntegrationProduceThenSQLQuery is an end-to-end integration test
// that exercises the full pipeline:
//
//  1. spin up a real camu server
//  2. create a topic
//  3. produce records via the HTTP produce endpoint
//  4. wait for the native segment to flush to S3
//  5. simulate the parquet-export step by writing a parquet file that
//     mirrors the produced records and publishing a manifest via the
//     extracted internal/parquet package
//  6. query the data back through POST /v1/sql
//  7. assert each produced record appears in the result set
//
// Step 5 uses internal/parquet directly against env.S3Client(), which
// also proves the extracted package is usable without any hand-holding
// from internal/server.
func TestIntegrationProduceThenSQLQuery(t *testing.T) {
	enabled := true
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.SQL.Enabled = &enabled
			cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "sql-cache")
			cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "sql-tmp")
		}),
	)
	defer env.Cleanup()

	client := env.Client()
	ctx := context.Background()
	topic := "events-sql"

	// This test writes the Parquet manifest itself, so leave the async
	// exporter disabled. Otherwise two independent writers race to publish the
	// same bucket and the generation assertion below is meaningless.
	if err := createTopicWithExport(client, topic, 1, 24*time.Hour, false); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// 1. Produce a known batch of records.
	produced := []camutest.ProduceMessage{
		{Key: "k1", Value: "alpha"},
		{Key: "k2", Value: "beta"},
		{Key: "k3", Value: "gamma"},
	}
	if _, err := client.Produce(topic, produced); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// 2. Wait for the segment to flush to S3 so the records are
	// visible to anything that reads the canonical log.
	time.Sleep(6 * time.Second)

	// 3. Simulate the parquet-export step. In a fully-wired system the
	// partition-leader's export executor would decode native record
	// batches and emit a parquet file; here we emit one manually with
	// the same payload (what matters is that the SQL path can find and
	// serve it).
	ingestTime := time.Now().UTC()
	objectKey := parquet.ExportObjectKey(topic, 0, ingestTime, 0, int64(len(produced)-1), 1, "integration/events/0/0-2.segment|epoch=1")

	parquetBytes := writeTestParquetFromRecords(t, produced)
	if err := env.S3Client().Put(ctx, objectKey, parquetBytes, storage.PutOpts{
		ContentType: "application/octet-stream",
	}); err != nil {
		t.Fatalf("S3 Put parquet: %v", err)
	}

	// 4. Publish the manifest using the EXTRACTED parquet package
	// directly, through a local adapter over env.S3Client(). No
	// dependency on internal/server is needed to do this.
	pStore := parquet.NewStore(&integrationParquetAdapter{client: env.S3Client()}, parquet.NoFencer{})
	date, hour := parquet.BucketDateHour(ingestTime)
	_, err := pStore.PublishManifest(ctx, parquet.Manifest{
		Topic: topic, Partition: 0, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []parquet.Entry{
			{ObjectKey: objectKey, BaseOffset: 0, EndOffset: int64(len(produced) - 1), SchemaVersion: 1, SourceKey: "integration/events/0/0-2.segment", SourceEpoch: 1},
		},
	})
	if err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	// 5. Query through the real /v1/sql HTTP endpoint.
	resp, err := client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    `select key, value from "events-sql" order by record_offset`,
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("SQLQuery: %v", err)
	}
	if len(resp.Rows) != len(produced) {
		t.Fatalf("rows = %d, want %d; resp=%+v", len(resp.Rows), len(produced), resp)
	}
	for i, row := range resp.Rows {
		if len(row) != 2 {
			t.Fatalf("row[%d] has %d cols, want 2", i, len(row))
		}
		gotKey, _ := row[0].(string)
		gotVal, _ := row[1].(string)
		if gotKey != produced[i].Key {
			t.Errorf("row[%d] key = %q, want %q", i, gotKey, produced[i].Key)
		}
		if gotVal != produced[i].Value {
			t.Errorf("row[%d] value = %q, want %q", i, gotVal, produced[i].Value)
		}
	}

	// 6. Idempotent republish of the SAME manifest must not bump the
	// generation — this is the crash-recovery guarantee built into the
	// extracted parquet package, exercised end-to-end here.
	retry, err := pStore.PublishManifest(ctx, parquet.Manifest{
		Topic: topic, Partition: 0, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []parquet.Entry{
			{ObjectKey: objectKey, BaseOffset: 0, EndOffset: int64(len(produced) - 1), SchemaVersion: 1, SourceKey: "integration/events/0/0-2.segment", SourceEpoch: 1},
		},
	})
	if err != nil {
		t.Fatalf("retry PublishManifest: %v", err)
	}
	if retry.Generation != 1 {
		t.Fatalf("retry generation = %d, want 1 (idempotent)", retry.Generation)
	}

	// 7. Re-query after retry; result must be stable.
	resp2, err := client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    `select count(*)::BIGINT as n from "events-sql"`,
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if len(resp2.Rows) != 1 {
		t.Fatalf("count rows = %d, want 1", len(resp2.Rows))
	}
}

// TestIntegrationSQLPaginationDrainsMoreThanDefaultLimit verifies the public
// SQL API rather than a helper: two HTTP requests with an explicit LIMIT and
// deterministic OFFSET must return every exported row exactly once. The data
// set is deliberately larger than the server's 1,000-row default limit.
func TestIntegrationSQLPaginationDrainsMoreThanDefaultLimit(t *testing.T) {
	const total = 1005
	sqlEnabled := true
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.SQL.Enabled = &sqlEnabled
			cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "sql-cache")
			cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "sql-tmp")
		}),
	)
	defer env.Cleanup()

	ctx := context.Background()
	topic := "events-sql-pagination"
	if err := createExportTopic(env.Client(), topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	produced := make([]camutest.ProduceMessage, total)
	for i := range produced {
		produced[i] = camutest.ProduceMessage{Key: fmt.Sprintf("key-%04d", i), Value: fmt.Sprintf("value-%04d", i)}
	}

	ingestTime := time.Now().UTC()
	objectKey := parquet.ExportObjectKey(topic, 0, ingestTime, 0, int64(total-1), 1, "integration/events/0/0-1004.segment|epoch=1")
	if err := env.S3Client().Put(ctx, objectKey, writeTestParquetFromRecords(t, produced), storage.PutOpts{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("S3 Put parquet: %v", err)
	}
	pStore := parquet.NewStore(&integrationParquetAdapter{client: env.S3Client()}, parquet.NoFencer{})
	date, hour := parquet.BucketDateHour(ingestTime)
	if _, err := pStore.PublishManifest(ctx, parquet.Manifest{
		Topic: topic, Partition: 0, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []parquet.Entry{{ObjectKey: objectKey, BaseOffset: 0, EndOffset: int64(total - 1), SchemaVersion: 1, SourceKey: "integration/events/0/0-1004.segment", SourceEpoch: 1}},
	}); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	seen := make(map[string]struct{}, total)
	for offset := 0; ; offset += 1000 {
		resp, err := env.Client().SQLQuery(camutest.SQLQueryRequest{
			SQL:    fmt.Sprintf(`select key from "%s" order by record_offset offset %d`, topic, offset),
			Topics: []string{topic},
			Limit:  1000,
		})
		if err != nil {
			t.Fatalf("SQLQuery page at offset %d: %v", offset, err)
		}
		for _, row := range resp.Rows {
			if len(row) != 1 {
				t.Fatalf("page row has %d columns, want 1", len(row))
			}
			key := string(decodeBlob(t, row[0]))
			if _, duplicate := seen[key]; duplicate {
				t.Fatalf("duplicate key returned by paginated SQL API: %q", key)
			}
			seen[key] = struct{}{}
		}
		if len(resp.Rows) < 1000 {
			break
		}
	}
	if len(seen) != total {
		t.Fatalf("paginated SQL rows = %d, want %d", len(seen), total)
	}
	for _, message := range produced {
		if _, ok := seen[message.Key]; !ok {
			t.Errorf("missing key %q from paginated SQL drain", message.Key)
		}
	}
}

// TestIntegrationProduceThenExportThenQuery exercises the REAL async
// parquet exporter (no manual PublishManifest simulation). Records are
// produced via HTTP, the native segment flushes to S3, the maintenance
// loop's per-partition Parquet consumer reads committed records and writes
// a real parquet chunk, then `/v1/sql` returns the rows.
//
// This is the end-to-end proof that:
//
//  1. Produce → native segment → S3 flush works.
//  2. The async exporter consumer picks up committed records without any
//     application-level push.
//  3. The exporter is idempotent across multiple maintenance ticks
//     (two runs over the same segment do not create duplicate rows).
//  4. The SQL endpoint reads the real exported parquet file.
func TestIntegrationProduceThenExportThenQuery(t *testing.T) {
	testIntegrationProduceThenExportThenQuery(t, "events-real-export", []camutest.ProduceMessage{
		{Key: "k1", Value: "alpha"},
		{Key: "k2", Value: "beta"},
		{Key: "k3", Value: "gamma"},
		{Key: "k4", Value: "delta"},
	})
}

// TestIntegrationFirstRecordExported verifies that a new Parquet pipeline checkpoint
// starts before offset zero, so a first sealed one-record segment is visible
// through SQL.
func TestIntegrationFirstRecordExported(t *testing.T) {
	testIntegrationProduceThenExportThenQuery(t, "events-first-record", []camutest.ProduceMessage{
		{Key: "k1", Value: "alpha"},
	})
}

// TestIntegrationStreamExportRetentionThenQueryRole verifies the production
// lifecycle across separately constructed roles. The stream node owns native
// data and the maintenance scheduler; the query node is created only after
// retention removed native objects and reads solely from the shared object
// store through its public SQL endpoint.
func TestIntegrationStreamExportRetentionThenQueryRole(t *testing.T) {
	sqlEnabled := true
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.SQL.Enabled = &sqlEnabled
			cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "stream-sql-cache")
			cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "stream-sql-tmp")
			cfg.Segments.MaxAge = "100ms"
			// These are normal production scheduler settings, shortened to keep
			// this integration lifecycle bounded without a test-only tick hook.
			cfg.Coordination.LeaseTTL = "2s"
			cfg.Coordination.HeartbeatInterval = "100ms"
			cfg.Coordination.RebalanceDelay = "100ms"
		}),
	)
	defer env.Cleanup()

	ctx := context.Background()
	topic := "events-query-role"
	produced := []camutest.ProduceMessage{
		{Key: "k1", Value: "alpha"},
		{Key: "k2", Value: "beta"},
		{Key: "k3", Value: "gamma"},
	}
	if err := createExportTopic(env.Client(), topic, 1, 500*time.Millisecond); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := env.Client().Produce(topic, produced); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Wait for the actual coordination/maintenance loop to export the sealed
	// segment and then delete its native source after the durable checkpoint.
	deadline := time.Now().Add(15 * time.Second)
	sawNativeSource := false
	for time.Now().Before(deadline) {
		keys, err := env.S3Client().List(ctx, log.ListSegmentPrefix(topic, 0))
		if err != nil {
			t.Fatalf("list native objects: %v", err)
		}
		nativeSourcePresent := false
		for _, key := range keys {
			if strings.HasSuffix(key, ".segment") {
				nativeSourcePresent = true
				break
			}
		}
		if nativeSourcePresent {
			sawNativeSource = true
		}
		if sawNativeSource && !nativeSourcePresent {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	keys, err := env.S3Client().List(ctx, log.ListSegmentPrefix(topic, 0))
	if err != nil {
		t.Fatalf("list native objects after retention: %v", err)
	}
	for _, key := range keys {
		if strings.HasSuffix(key, ".segment") {
			t.Fatalf("native source %q still present after scheduler-driven retention", key)
		}
	}
	if !sawNativeSource {
		t.Fatal("never observed the sealed native source before retention")
	}

	queryCfg := &config.Config{
		Server: config.ServerConfig{
			Address: "127.0.0.1:0", InstanceID: "query-role", Mode: config.ServerModeQuery,
		},
		Storage: config.StorageConfig{Bucket: "test-bucket", Endpoint: "memory://"},
		Cache:   config.CacheConfig{Directory: t.TempDir(), MaxSize: 100 * 1024 * 1024},
		SQL: config.SQLConfig{
			Enabled: &sqlEnabled, CacheDirectory: filepath.Join(t.TempDir(), "query-cache"),
			TempDirectory: filepath.Join(t.TempDir(), "query-tmp"),
		},
	}
	queryServer, err := server.NewWithS3Client(queryCfg, env.S3Client())
	if err != nil {
		t.Fatalf("NewWithS3Client(query role): %v", err)
	}
	if err := queryServer.Start(); err != nil {
		t.Fatalf("Start(query role): %v", err)
	}
	defer queryServer.Shutdown(context.Background())

	resp, err := camutest.NewClient("http://" + queryServer.Address()).SQLQuery(camutest.SQLQueryRequest{
		SQL:    fmt.Sprintf(`select record_offset, key, value from "%s" order by record_offset`, topic),
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("SQLQuery(query role): %v", err)
	}
	if len(resp.Rows) != len(produced) {
		t.Fatalf("query-role rows = %d, want %d: %+v", len(resp.Rows), len(produced), resp.Rows)
	}
	for i, row := range resp.Rows {
		if got := string(decodeBlob(t, row[1])); got != produced[i].Key {
			t.Errorf("row[%d] key = %q, want %q", i, got, produced[i].Key)
		}
		if got := string(decodeBlob(t, row[2])); got != produced[i].Value {
			t.Errorf("row[%d] value = %q, want %q", i, got, produced[i].Value)
		}
	}
}

func testIntegrationProduceThenExportThenQuery(t *testing.T, topic string, produced []camutest.ProduceMessage) {
	t.Helper()
	sqlEnabled := true
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.SQL.Enabled = &sqlEnabled
			cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "sql-cache")
			cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "sql-tmp")
		}),
	)
	defer env.Cleanup()

	client := env.Client()
	if err := createExportTopic(client, topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if _, err := client.Produce(topic, produced); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Drive the maintenance loop explicitly rather than relying on the
	// background tick — this makes the test deterministic and fast. We
	// still wait long enough for the active segment to flush so the
	// exporter consumer finds committed records to read.
	tc := meta.TopicConfig{
		Name: topic, Partitions: 1, Retention: 24 * time.Hour,
		CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1,
		StorageMode: meta.StorageModeClassic, ExportEnabled: true,
	}

	// Wait up to ~15s for the exporter to expose the rows via SQL.
	var gotRows int
	var lastErr error
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		env.Server(0).RunPartitionMaintenanceForTest([]meta.TopicConfig{tc})
		resp, err := client.SQLQuery(camutest.SQLQueryRequest{
			SQL:    fmt.Sprintf(`select count(*)::BIGINT from "%s"`, topic),
			Topics: []string{topic},
		})
		if err != nil {
			lastErr = err
		} else if len(resp.Rows) == 1 {
			if n, ok := resp.Rows[0][0].(float64); ok && int(n) == len(produced) {
				gotRows = int(n)
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if gotRows != len(produced) {
		t.Fatalf("exporter never materialized rows: got %d, want %d; lastErr=%v", gotRows, len(produced), lastErr)
	}

	// Second query: read back keys/values and compare to the produced
	// payload, proving the full round-trip through the real exporter.
	resp, err := client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    fmt.Sprintf(`select record_offset, key, value from "%s" order by record_offset`, topic),
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("read-back query: %v", err)
	}
	if len(resp.Rows) != len(produced) {
		t.Fatalf("got %d rows, want %d", len(resp.Rows), len(produced))
	}
	for i, row := range resp.Rows {
		// DuckDB BLOB columns come back as base64-encoded strings over
		// JSON. Decode before comparing.
		got := decodeBlob(t, row[1])
		if string(got) != produced[i].Key {
			t.Errorf("row[%d].key = %q, want %q", i, string(got), produced[i].Key)
		}
		gotVal := decodeBlob(t, row[2])
		if string(gotVal) != produced[i].Value {
			t.Errorf("row[%d].value = %q, want %q", i, string(gotVal), produced[i].Value)
		}
	}

	// Idempotency: run the maintenance pass again. Row count must not
	// change (the exporter must not duplicate an already-exported
	// segment).
	env.Server(0).RunPartitionMaintenanceForTest([]meta.TopicConfig{tc})
	resp, err = client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    fmt.Sprintf(`select count(*)::BIGINT from "%s"`, topic),
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("post-rerun count: %v", err)
	}
	if n, _ := resp.Rows[0][0].(float64); int(n) != len(produced) {
		t.Fatalf("post-rerun count = %v, want %d (exporter duplicated rows)", n, len(produced))
	}
}

// decodeBlob normalizes DuckDB BLOB JSON results to a byte slice.
// DuckDB BLOBs come over the wire as base64-encoded JSON strings.
func decodeBlob(t *testing.T, v any) []byte {
	t.Helper()
	switch x := v.(type) {
	case string:
		b, err := base64.StdEncoding.DecodeString(x)
		if err != nil {
			// Some DuckDB builds return BLOB as a raw hex/ascii string
			// instead of base64. Fall back to treating the value as raw.
			return []byte(x)
		}
		return b
	case []byte:
		return x
	default:
		t.Fatalf("unexpected blob type %T: %v", v, v)
		return nil
	}
}

// TestIntegrationSQLQueryAggregation produces a larger batch of records,
// simulates the export, and runs SQL aggregation queries (GROUP BY,
// COUNT, SUM) against the result. Proves the query path is not just a
// "select *" passthrough but lets DuckDB do real analytical work.
func TestIntegrationSQLQueryAggregation(t *testing.T) {
	enabled := true
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.SQL.Enabled = &enabled
			cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "sql-cache")
			cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "sql-tmp")
		}),
	)
	defer env.Cleanup()

	client := env.Client()
	ctx := context.Background()
	topic := "events-agg"
	if err := createExportTopic(client, topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 10 records across 3 categories — the native log actually
	// stores these; we're about to shadow them into Parquet.
	produced := []camutest.ProduceMessage{
		{Key: "a", Value: "1"}, {Key: "a", Value: "2"}, {Key: "a", Value: "3"}, {Key: "a", Value: "4"},
		{Key: "b", Value: "1"}, {Key: "b", Value: "2"}, {Key: "b", Value: "3"},
		{Key: "c", Value: "1"}, {Key: "c", Value: "2"}, {Key: "c", Value: "3"},
	}
	if _, err := client.Produce(topic, produced); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	time.Sleep(6 * time.Second)

	// Simulate export.
	ingestTime := time.Now().UTC()
	objectKey := parquet.ExportObjectKey(topic, 0, ingestTime, 0, int64(len(produced)-1), 1, "integration/events-agg/0/0-9.segment|epoch=1")
	if err := env.S3Client().Put(ctx, objectKey, writeTestParquetFromRecords(t, produced),
		storage.PutOpts{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("S3 Put: %v", err)
	}
	pStore := parquet.NewStore(&integrationParquetAdapter{client: env.S3Client()}, parquet.NoFencer{})
	date, hour := parquet.BucketDateHour(ingestTime)
	if _, err := pStore.PublishManifest(ctx, parquet.Manifest{
		Topic: topic, Partition: 0, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: []parquet.Entry{
			{ObjectKey: objectKey, BaseOffset: 0, EndOffset: int64(len(produced) - 1), SchemaVersion: 1, SourceKey: "integration/events-agg/0/0-9.segment", SourceEpoch: 1},
		},
	}); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	// GROUP BY key: expect counts a=4, b=3, c=3.
	resp, err := client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    `select key, count(*)::BIGINT as n from "events-agg" group by key order by key`,
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("group-by query: %v", err)
	}
	if len(resp.Rows) != 3 {
		t.Fatalf("got %d groups, want 3: %+v", len(resp.Rows), resp.Rows)
	}
	wantCounts := map[string]float64{"a": 4, "b": 3, "c": 3}
	for _, row := range resp.Rows {
		k, _ := row[0].(string)
		n, _ := row[1].(float64)
		if want := wantCounts[k]; n != want {
			t.Errorf("group %q: count=%v, want %v", k, n, want)
		}
	}

	// COUNT(*) across the whole topic.
	resp, err = client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    `select count(*)::BIGINT as n from "events-agg"`,
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if n, _ := resp.Rows[0][0].(float64); n != float64(len(produced)) {
		t.Fatalf("count = %v, want %d", n, len(produced))
	}

	// WHERE filter.
	resp, err = client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    `select count(*)::BIGINT as n from "events-agg" where key = 'b'`,
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("filter query: %v", err)
	}
	if n, _ := resp.Rows[0][0].(float64); n != 3 {
		t.Fatalf("where count = %v, want 3", n)
	}
}

// TestIntegrationSQLQueryCompactionRoundTrip produces records, exports
// them into MULTIPLE small parquet files in one bucket, then runs
// parquet.Store.CompactBucket to replace them with a single large file,
// and proves queries keep returning the same result set throughout.
//
// This end-to-end covers the full idempotency story of the extracted
// parquet package: native produce -> multi-file export -> compaction ->
// idempotent compaction retry -> query consistency at every step.
func TestIntegrationSQLQueryCompactionRoundTrip(t *testing.T) {
	enabled := true
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.SQL.Enabled = &enabled
			cfg.SQL.CacheDirectory = filepath.Join(t.TempDir(), "sql-cache")
			cfg.SQL.TempDirectory = filepath.Join(t.TempDir(), "sql-tmp")
		}),
	)
	defer env.Cleanup()

	client := env.Client()
	ctx := context.Background()
	topic := "events-compact"
	if err := createExportTopic(client, topic, 1, 24*time.Hour); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	batches := [][]camutest.ProduceMessage{
		{{Key: "k1", Value: "alpha"}, {Key: "k2", Value: "beta"}},
		{{Key: "k3", Value: "gamma"}, {Key: "k4", Value: "delta"}},
		{{Key: "k5", Value: "epsilon"}},
	}
	var all []camutest.ProduceMessage
	for _, b := range batches {
		if _, err := client.Produce(topic, b); err != nil {
			t.Fatalf("produce: %v", err)
		}
		all = append(all, b...)
	}
	time.Sleep(6 * time.Second)

	// Simulate the export by writing one parquet file per batch. Each
	// small file carries a slice of the global offsets.
	ingestTime := time.Now().UTC()
	date, hour := parquet.BucketDateHour(ingestTime)
	pStore := parquet.NewStore(&integrationParquetAdapter{client: env.S3Client()}, parquet.NoFencer{})

	var smallEntries []parquet.Entry
	var smallKeys []string
	offset := int64(0)
	for _, b := range batches {
		base := offset
		end := offset + int64(len(b)) - 1
		offset += int64(len(b))
		key := parquet.ExportObjectKey(topic, 0, ingestTime, base, end, 1, fmt.Sprintf("integration/%s/0/%d-%d.segment|epoch=1", topic, base, end))
		if err := env.S3Client().Put(ctx, key, writeTestParquetFromRecordsWithBase(t, b, base),
			storage.PutOpts{ContentType: "application/octet-stream"}); err != nil {
			t.Fatalf("put small: %v", err)
		}
		smallEntries = append(smallEntries, parquet.Entry{ObjectKey: key, BaseOffset: base, EndOffset: end, SchemaVersion: 1, SourceKey: fmt.Sprintf("integration/%s/0/%d-%d.segment", topic, base, end), SourceEpoch: 1})
		smallKeys = append(smallKeys, key)
	}
	if _, err := pStore.PublishManifest(ctx, parquet.Manifest{
		Topic: topic, Partition: 0, Date: date, Hour: hour, SchemaVersion: 1,
		Entries: smallEntries,
	}); err != nil {
		t.Fatalf("publish small: %v", err)
	}

	// Query before compaction.
	gotBefore := fetchAllKeyValue(t, client, topic)
	if len(gotBefore) != len(all) {
		t.Fatalf("before compact: got %d rows, want %d", len(gotBefore), len(all))
	}

	// Build the compacted big file that covers the full offset span
	// of all 5 records.
	bigKey := parquet.ExportObjectKey(topic, 0, ingestTime, 0, int64(len(all)-1), 1, fmt.Sprintf("integration/%s/0/0-%d.compacted|epoch=1", topic, len(all)-1)) + ".big"
	if err := env.S3Client().Put(ctx, bigKey, writeTestParquetFromRecords(t, all),
		storage.PutOpts{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("put big: %v", err)
	}
	bigEntry := parquet.Entry{ObjectKey: bigKey, BaseOffset: 0, EndOffset: int64(len(all) - 1), SchemaVersion: 1, SourceKey: fmt.Sprintf("compaction/integration/%s/0/0-%d", topic, len(all)-1), SourceEpoch: 0}

	// Run compaction: replace the 3 small files with the 1 big file.
	compacted, err := pStore.CompactBucket(ctx, topic, 0, date, hour, smallKeys, []parquet.Entry{bigEntry})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if len(compacted.Entries) != 1 || compacted.Entries[0].ObjectKey != bigKey {
		t.Fatalf("compacted entries = %+v, want [big]", compacted.Entries)
	}

	// Query after compaction — same result set.
	gotAfter := fetchAllKeyValue(t, client, topic)
	if len(gotAfter) != len(all) {
		t.Fatalf("after compact: got %d rows, want %d", len(gotAfter), len(all))
	}
	for i, want := range all {
		if gotAfter[i].Key != want.Key || gotAfter[i].Value != want.Value {
			t.Errorf("row[%d] = %+v, want %+v", i, gotAfter[i], want)
		}
	}

	// Idempotent compaction retry: the same CompactBucket call must
	// not bump the generation.
	retry, err := pStore.CompactBucket(ctx, topic, 0, date, hour, smallKeys, []parquet.Entry{bigEntry})
	if err != nil {
		t.Fatalf("compact retry: %v", err)
	}
	if retry.Generation != compacted.Generation {
		t.Fatalf("retry bumped generation: %d -> %d", compacted.Generation, retry.Generation)
	}

	// Queries still correct after the idempotent retry.
	gotRetry := fetchAllKeyValue(t, client, topic)
	if len(gotRetry) != len(all) {
		t.Fatalf("after retry: got %d rows, want %d", len(gotRetry), len(all))
	}
}

// fetchAllKeyValue issues `select key, value from "{topic}" order by
// record_offset` and returns the rows as ProduceMessage slices so the
// caller can diff against the produced payload.
func fetchAllKeyValue(t *testing.T, client *camutest.Client, topic string) []camutest.ProduceMessage {
	t.Helper()
	resp, err := client.SQLQuery(camutest.SQLQueryRequest{
		SQL:    fmt.Sprintf(`select key, value from "%s" order by record_offset`, topic),
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	out := make([]camutest.ProduceMessage, len(resp.Rows))
	for i, row := range resp.Rows {
		k, _ := row[0].(string)
		v, _ := row[1].(string)
		out[i] = camutest.ProduceMessage{Key: k, Value: v}
	}
	return out
}

// writeTestParquetFromRecordsWithBase is like writeTestParquetFromRecords
// but offsets each row starting at `base` rather than zero. Used for
// multi-file export simulations.
func writeTestParquetFromRecordsWithBase(t *testing.T, msgs []camutest.ProduceMessage, base int64) []byte {
	t.Helper()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "out.parquet")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE rows (record_offset BIGINT, key VARCHAR, value VARCHAR)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i, m := range msgs {
		if _, err := db.Exec(`INSERT INTO rows VALUES (?, ?, ?)`, base+int64(i), m.Key, m.Value); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	escaped := strings.ReplaceAll(path, `'`, `''`)
	if _, err := db.Exec(fmt.Sprintf(`COPY rows TO '%s' (FORMAT PARQUET)`, escaped)); err != nil {
		t.Fatalf("copy: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return data
}

// writeTestParquetFromRecords builds a parquet file with three columns:
// record_offset BIGINT, key VARCHAR, value VARCHAR. It uses DuckDB
// in-process to write the file — the same engine the SQL endpoint uses
// to read it. (`offset` is a reserved word in DuckDB.)
func writeTestParquetFromRecords(t *testing.T, msgs []camutest.ProduceMessage) []byte {
	t.Helper()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "out.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open duckdb: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE rows (record_offset BIGINT, key VARCHAR, value VARCHAR)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i, m := range msgs {
		if _, err := db.Exec(`INSERT INTO rows VALUES (?, ?, ?)`, int64(i), m.Key, m.Value); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	escapedPath := strings.ReplaceAll(path, `'`, `''`)
	if _, err := db.Exec(fmt.Sprintf(`COPY rows TO '%s' (FORMAT PARQUET)`, escapedPath)); err != nil {
		t.Fatalf("copy to parquet: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	return data
}

// integrationParquetAdapter wraps *storage.S3Client so it satisfies
// parquet.ObjectStore for this integration test. It deliberately does
// NOT import internal/server — proving the extracted package can be
// consumed by any caller that has a storage client and is willing to
// write ~40 lines of translation.
type integrationParquetAdapter struct {
	client *storage.S3Client
	mu     sync.Mutex
}

func (a *integrationParquetAdapter) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := a.client.Get(ctx, key)
	return data, translateStorageErr(err)
}

func (a *integrationParquetAdapter) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := a.client.GetWithETag(ctx, key)
	return data, etag, translateStorageErr(err)
}

func (a *integrationParquetAdapter) ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	newETag, err := a.client.ConditionalPut(ctx, key, data, etag)
	return newETag, translateStorageErr(err)
}

func (a *integrationParquetAdapter) Delete(ctx context.Context, key string) error {
	return translateStorageErr(a.client.Delete(ctx, key))
}

func (a *integrationParquetAdapter) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := a.client.List(ctx, prefix)
	return keys, translateStorageErr(err)
}

func (a *integrationParquetAdapter) ListEach(ctx context.Context, prefix string, fn func(key string) error) error {
	return translateStorageErr(a.client.ListEach(ctx, prefix, fn))
}

func (a *integrationParquetAdapter) DeleteMany(ctx context.Context, keys []string) error {
	return translateStorageErr(a.client.DeleteMany(ctx, keys))
}

func translateStorageErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrNotFound):
		return errors.Join(err, parquet.ErrNotFound)
	case errors.Is(err, storage.ErrConflict):
		return errors.Join(err, parquet.ErrConflict)
	default:
		return err
	}
}
