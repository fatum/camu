package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTargetRecordCount(t *testing.T) {
	for _, tc := range []struct{ target, size, want int64 }{{1, 1, 1}, {1025, 1024, 2}, {2048, 1024, 2}} {
		got := (tc.target + tc.size - 1) / tc.size
		if got != tc.want {
			t.Fatalf("count(%d,%d)=%d, want %d", tc.target, tc.size, got, tc.want)
		}
	}
}

func TestKafkaConsumeProgressLogsAtBoundedCadence(t *testing.T) {
	var expected [2]hashState
	for sequence := int64(0); sequence < 3; sequence++ {
		expected[0].add(typedValue{Sequence: sequence, PayloadBytes: 1})
	}
	for sequence := int64(3); sequence < 5; sequence++ {
		expected[1].add(typedValue{Sequence: sequence, PayloadBytes: 1})
	}
	var lines []string
	started := time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)
	reporter := newKafkaConsumeProgress(config{KafkaBrokers: []string{"broker-a:9092", "broker-b:9092"}, Topic: "events", Partitions: 2}, expected[:], 5, started, func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	})
	reporter.startup()
	if len(lines) != 3 || !strings.Contains(lines[0], "brokers=broker-a:9092,broker-b:9092") || !strings.Contains(lines[0], "start_offset=0") {
		t.Fatalf("startup lines = %v", lines)
	}
	for _, line := range lines[1:] {
		if !strings.Contains(line, "start_offset=0") {
			t.Fatalf("startup partition line lacks start_offset=0: %q", line)
		}
	}

	reporter.beginPoll()
	reporter.record(0, 11)
	reporter.record(0, 12)
	reporter.poll(started.Add(500*time.Millisecond), 10*time.Millisecond, 2, 2048, []int64{2, 0})
	if len(lines) != 3 {
		t.Fatalf("logs before cadence interval = %v", lines)
	}
	reporter.beginPoll()
	reporter.record(0, 11)
	reporter.record(0, 12)
	reporter.poll(started.Add(time.Second), 10*time.Millisecond, 2, 2048, []int64{2, 0})
	if len(lines) != 5 || !strings.Contains(lines[3], "records=2/5 bytes=2048") || !strings.Contains(lines[4], "partition=0") || !strings.Contains(lines[4], "consumed_through=12 fetch_records=2") {
		t.Fatalf("progress lines = %v", lines)
	}
	reporter.beginPoll()
	reporter.poll(started.Add(2*time.Second), 120*time.Millisecond, 2, 2048, []int64{2, 0})
	if len(lines) != 7 || !strings.Contains(lines[6], "empty poll") {
		t.Fatalf("empty-poll lines = %v", lines)
	}
	reporter.beginPoll()
	reporter.record(1, 4)
	reporter.poll(started.Add(3*time.Second), kafkaConsumeSlowPollThreshold, 3, 3072, []int64{2, 1})
	if len(lines) != 10 || !strings.Contains(lines[9], "slow poll") || !strings.Contains(lines[9], "fetch_records=1") {
		t.Fatalf("slow-poll lines = %v", lines)
	}
}

func TestHashStateDeterministic(t *testing.T) {
	var s hashState
	s.add(typedValue{ID: 0, Payload: "x", PayloadBytes: 1, Sequence: 0})
	s.add(typedValue{ID: 1, Payload: "x", PayloadBytes: 1, Sequence: 1})
	n, b, digest, err := s.result()
	if err != nil || n != 2 || b != 2 {
		t.Fatalf("result=(%d,%d,%v)", n, b, err)
	}
	if raw, e := hex.DecodeString(digest); e != nil || len(raw) != 32 {
		t.Fatalf("digest=%q", digest)
	}

}

func TestParseByteSize(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want int64
	}{
		{name: "bytes", raw: "1073741824", want: 1073741824},
		{name: "kibibytes", raw: "2KiB", want: 2 * 1024},
		{name: "mebibytes", raw: "3MiB", want: 3 * 1024 * 1024},
		{name: "gibibytes", raw: "1GiB", want: 1024 * 1024 * 1024},
		{name: "case and whitespace", raw: " 4gIb ", want: 4 * 1024 * 1024 * 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TARGET_BYTES", test.raw)
			got, err := parseByteSize("TARGET_BYTES", 1)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("parseByteSize() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestParseByteSizeRejectsInvalidValue(t *testing.T) {
	for _, raw := range []string{"0", "-1", "1GB", "1GiBB", "9000000TiB"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("TARGET_BYTES", raw)
			if _, err := parseByteSize("TARGET_BYTES", 1); err == nil {
				t.Fatalf("parseByteSize(%q) succeeded", raw)
			}
		})
	}
}

func TestDisklessSkipsClusterReadinessAndReplicationWait(t *testing.T) {
	// An unreachable base proves no network request is made for diskless topics.
	cfg := config{StorageMode: "diskless"}
	c := client{base: "http://127.0.0.1:1", http: &http.Client{}, requestTimeout: time.Second}
	if err := c.waitClusterReady(context.Background(), cfg); err != nil {
		t.Fatalf("waitClusterReady() error = %v, want nil for diskless", err)
	}
	if err := c.waitForReplication(context.Background(), cfg); err != nil {
		t.Fatalf("waitForReplication() error = %v, want nil for diskless", err)
	}
}

func TestDetectTopicStorageMode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/topics/disk":
			_, _ = w.Write([]byte(`{"name":"disk","storage_mode":"diskless"}`))
		case "/v1/topics/classic":
			_, _ = w.Write([]byte(`{"name":"classic"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	c := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}
	ctx := context.Background()

	mode, err := c.detectTopicStorageMode(ctx, config{Topic: "disk"})
	if err != nil || mode != "diskless" {
		t.Fatalf("diskless mode = %q, err = %v, want diskless", mode, err)
	}
	mode, err = c.detectTopicStorageMode(ctx, config{Topic: "classic"})
	if err != nil || mode != "" {
		t.Fatalf("classic mode = %q, err = %v, want empty", mode, err)
	}
	mode, err = c.detectTopicStorageMode(ctx, config{Topic: "missing"})
	if err != nil || mode != "" {
		t.Fatalf("missing mode = %q, err = %v, want empty, nil", mode, err)
	}
}

// TestRunSingleOperationConsumeSkipsClusterReadinessForDiskless verifies that a
// consume run against an existing diskless topic never waits on /v1/cluster/ready
// even when STORAGE_MODE is not set: the topic's storage mode is detected and
// readiness is skipped.
func TestRunSingleOperationConsumeSkipsClusterReadinessForDiskless(t *testing.T) {
	seenReady := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/topics/bench-diskless":
			_, _ = w.Write([]byte(`{"name":"bench-diskless","partitions":4,"storage_mode":"diskless"}`))
		case r.URL.Path == "/v1/cluster/ready":
			seenReady = true
			w.WriteHeader(http.StatusInternalServerError)
		case strings.HasPrefix(r.URL.Path, "/v1/topics/bench-diskless/partitions/") && r.Method == http.MethodGet:
			w.Header().Set("X-High-Watermark", "0")
			_, _ = w.Write([]byte(`{"messages":[],"next_offset":0}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	c := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}
	cfg := config{API: "http", Operation: "consume", Topic: "bench-diskless", BaseURL: server.URL, Partitions: 4, MessageBytes: 1024, TargetBytes: 1024, ConsumeTimeout: time.Second, RequestTimeout: time.Second}
	res := result{Topic: cfg.Topic, Operation: cfg.Operation}
	runSingleOperation(context.Background(), c, cfg, &res)
	if seenReady {
		t.Fatal("benchmark polled /v1/cluster/ready for a diskless topic")
	}
	if res.Integrity.Error != "" {
		t.Fatalf("consume failed: %s", res.Integrity.Error)
	}
}

func TestNodeClientRoundRobin(t *testing.T) {
	cfg := config{NodeURLs: []string{"http://n0:8080", "http://n1:8080", "http://n2:8080"}}
	c := client{base: "http://default:8080"}
	seen := map[string]bool{}
	for i := 0; i < 6; i++ {
		seen[c.nodeClient(cfg).base] = true
	}
	for _, url := range cfg.NodeURLs {
		if !seen[url] {
			t.Fatalf("node %s never selected in round-robin", url)
		}
	}
	if c.base != "http://default:8080" {
		t.Fatal("nodeClient must not mutate the receiver")
	}
}

func TestLoadConfigExportEnabled(t *testing.T) {
	t.Setenv("EXPORT_ENABLED", "false")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExportEnabled {
		t.Fatal("ExportEnabled = true, want false")
	}
}

func TestLoadConfigRejectsSQLWithoutExport(t *testing.T) {
	t.Setenv("EXPORT_ENABLED", "false")
	t.Setenv("BENCHMARK_OPERATION", "sql")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() succeeded, want an error")
	}
}

func TestLoadConfigDisklessRequiresNoExport(t *testing.T) {
	t.Setenv("STORAGE_MODE", "diskless")
	t.Setenv("EXPORT_ENABLED", "true")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() succeeded with diskless + export, want an error")
	}
	t.Setenv("EXPORT_ENABLED", "false")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StorageMode != "diskless" {
		t.Fatalf("StorageMode = %q, want diskless", cfg.StorageMode)
	}
}

func TestLoadConfigRejectsInvalidStorageMode(t *testing.T) {
	t.Setenv("STORAGE_MODE", "bogus")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() succeeded with invalid storage mode, want an error")
	}
}

func TestCreateOmitsSchemaForDisklessTopic(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/topics/events/routing" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"partitions":{"0":{"replicas":[1]},"1":{"replicas":[1]}}}`))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/topics/events/partitions/") && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-High-Watermark", "0")
			_, _ = w.Write([]byte(`{"messages":[],"next_offset":0}`))
			return
		}
		if r.URL.Path == "/v1/topics" && r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			return
		}
		t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	c := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}
	cfg := config{Topic: "events", Partitions: 2, ReplicationFactor: 1, MinInSyncReplicas: 1, ExportEnabled: false, StorageMode: "diskless"}
	if err := c.create(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if got["storage_mode"] != "diskless" {
		t.Fatalf("storage_mode = %v, want diskless", got["storage_mode"])
	}
	if _, ok := got["schema"]; ok {
		t.Fatal("diskless topic must not carry a schema")
	}
	if got["export_enabled"] != false {
		t.Fatalf("export_enabled = %v, want false", got["export_enabled"])
	}

	// Exported classic topics still carry the schema.
	if err := c.create(context.Background(), config{Topic: "events", Partitions: 2, ReplicationFactor: 1, MinInSyncReplicas: 1, ExportEnabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["schema"]; !ok {
		t.Fatal("exported topic must carry a schema")
	}
}

func TestClientRequestUsesRequestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	c := client{base: server.URL, http: &http.Client{}, requestTimeout: 20 * time.Millisecond}
	started := time.Now()
	err := c.request(context.Background(), http.MethodGet, "/", nil, nil)
	if err == nil {
		t.Fatal("request succeeded, want timeout")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("request timeout took %s", elapsed)
	}
}

func TestFirstSequenceForPartitionContinuesAnAppend(t *testing.T) {
	for partition, want := range []int64{8, 9, 10, 7} {
		if got := firstSequenceForPartition(7, partition, 4); got != want {
			t.Fatalf("partition %d first sequence = %d, want %d", partition, got, want)
		}
	}
}

func TestRunSequenceValidatorRejectsGapsAndReordering(t *testing.T) {
	cfg := config{Partitions: 4, MessageBytes: 1}
	validator := runSequenceValidator{}
	if err := validator.validate(cfg, 1, typedValue{RunID: "run-a", ID: 1, Sequence: 1, Payload: "x", PayloadBytes: 1}); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	if err := validator.validate(cfg, 1, typedValue{RunID: "run-b", ID: 1, Sequence: 1, Payload: "x", PayloadBytes: 1}); err != nil {
		t.Fatalf("concurrent run rejected: %v", err)
	}
	if err := validator.validate(cfg, 1, typedValue{RunID: "run-a", ID: 5, Sequence: 5, Payload: "x", PayloadBytes: 1}); err != nil {
		t.Fatalf("continued run rejected: %v", err)
	}
	for _, tc := range []struct {
		name  string
		value typedValue
	}{
		{name: "sequence gap", value: typedValue{RunID: "run-a", ID: 13, Sequence: 13, Payload: "x", PayloadBytes: 1}},
		{name: "invalid payload", value: typedValue{RunID: "run-a", ID: 9, Sequence: 9, Payload: "bad", PayloadBytes: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validator.validate(cfg, 1, tc.value); err == nil {
				t.Fatal("validator succeeded, want error")
			}
		})
	}
	if err := validator.validate(cfg, 1, typedValue{ID: 9, Sequence: 9, Payload: "x", PayloadBytes: 1}); err != nil {
		t.Fatalf("legacy record rejected: %v", err)
	}
}

func TestKafkaPartitionsCompleteRequiresEveryPartition(t *testing.T) {
	if kafkaPartitionsComplete([]int64{2, 1}, []int64{2, 2}) {
		t.Fatal("partitions complete despite missing record")
	}
	if !kafkaPartitionsComplete([]int64{2, 2}, []int64{2, 2}) {
		t.Fatal("partitions not complete")
	}
}

func TestHTTPConsumeRejectsOffsetAdvancePastRecords(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"messages":[{"offset":0,"value":"{\"run_id\":\"run-a\",\"id\":0,\"payload\":\"x\",\"payload_bytes\":1,\"sequence\":0}"}],"next_offset":2}`))
	}))
	defer server.Close()
	cfg := config{BaseURL: server.URL, Topic: "events", Partitions: 1, MessageBytes: 1, ConsumeTimeout: time.Second, RequestTimeout: time.Second}
	expected := expectedStatesFor(cfg, 1)
	actual := make([]hashState, 1)
	_, err := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}.consume(context.Background(), cfg, expected, actual, 1, func(int64) {})
	if err == nil || !strings.Contains(err.Error(), "next offset gap or reordering") {
		t.Fatalf("consume error = %v, want next-offset validation failure", err)
	}
}

func TestHTTPConsumeIgnoresRecordsAppendedAfterSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"messages":[{"offset":0,"value":"{\"run_id\":\"run-a\",\"id\":0,\"payload\":\"x\",\"payload_bytes\":1,\"sequence\":0}"},{"offset":1,"value":"{\"run_id\":\"run-a\",\"id\":1,\"payload\":\"x\",\"payload_bytes\":1,\"sequence\":1}"}],"next_offset":2}`))
	}))
	defer server.Close()
	cfg := config{BaseURL: server.URL, Topic: "events", Partitions: 1, MessageBytes: 1, ConsumeTimeout: time.Second, RequestTimeout: time.Second}
	expected := expectedStatesFor(cfg, 1)
	actual := make([]hashState, 1)
	result, err := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}.consume(context.Background(), cfg, expected, actual, 1, func(int64) {})
	if err != nil {
		t.Fatal(err)
	}
	if result.Records != 1 || actual[0].recordsSnapshot() != 1 {
		t.Fatalf("consumed records = %d/%d, want 1/1", result.Records, actual[0].recordsSnapshot())
	}
}

func TestCommittedRecordCountUsesEachPartitionHighWatermark(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/topics/events/partitions/0/messages":
			w.Header().Set("X-High-Watermark", "3")
		case "/v1/topics/events/partitions/1/messages":
			w.Header().Set("X-High-Watermark", "2")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"messages":[],"next_offset":0}`))
	}))
	defer server.Close()
	c := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}
	got, err := c.committedRecordCount(context.Background(), config{Topic: "events", Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("committedRecordCount() = %d, want 5", got)
	}
}

func TestExpectedStatesForPartitionOffsets(t *testing.T) {
	cfg := config{Partitions: 4, SequenceStart: 0, MessageBytes: 1}
	states, err := expectedStatesForPartitionOffsets(cfg, []int64{3, 2, 3, 2})
	if err != nil {
		t.Fatal(err)
	}
	for partition, want := range []int64{3, 2, 3, 2} {
		if got := states[partition].recordsSnapshot(); got != want {
			t.Fatalf("partition %d expected records = %d, want %d", partition, got, want)
		}
	}
}

func TestRetryProduceRetriesTransientFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attempts := 0
	err := retryProduce(ctx, "test produce", func() error {
		attempts++
		if attempts == 1 {
			return errors.New("POST /messages: 503 Service Unavailable")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestRetryProduceRejectsPermanentFailure(t *testing.T) {
	err := retryProduce(context.Background(), "test produce", func() error {
		return errors.New("POST /messages: 400 Bad Request")
	})
	if err == nil || !strings.Contains(err.Error(), "400 Bad Request") {
		t.Fatalf("retryProduce error = %v, want bad request", err)
	}
}
