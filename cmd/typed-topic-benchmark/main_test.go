package main

import (
	"context"
	"encoding/hex"
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
	if len(lines) != 3 || !strings.Contains(lines[0], "brokers=broker-a:9092,broker-b:9092") {
		t.Fatalf("startup lines = %v", lines)
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
	if len(lines) != 5 || !strings.Contains(lines[3], "records=2/5 bytes=2048") || !strings.Contains(lines[4], "partition=0") || !strings.Contains(lines[4], "last_offset=12 fetch_records=2") {
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

func TestHashStateDeterministicAndOrdering(t *testing.T) {
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

	var bad hashState
	bad.add(typedValue{Sequence: 2})
	bad.add(typedValue{Sequence: 1})
	if _, _, _, err := bad.result(); err == nil {
		t.Fatal("expected ordering error")
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

func TestValidateKafkaRecordRejectsGapsAndReordering(t *testing.T) {
	cfg := config{Partitions: 4, SequenceStart: 0}
	if err := validateKafkaRecord(cfg, 1, 2, 2, typedValue{Sequence: 9}); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		offset int64
		value  typedValue
	}{
		{name: "offset gap", offset: 3, value: typedValue{Sequence: 9}},
		{name: "sequence gap", offset: 2, value: typedValue{Sequence: 13}},
		{name: "sequence reordering", offset: 2, value: typedValue{Sequence: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateKafkaRecord(cfg, 1, 2, tc.offset, tc.value); err == nil {
				t.Fatal("validateKafkaRecord succeeded, want error")
			}
		})
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
		_, _ = w.Write([]byte(`{"messages":[{"offset":0,"value":"{\"id\":0,\"payload\":\"x\",\"payload_bytes\":1,\"sequence\":0}"}],"next_offset":2}`))
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
