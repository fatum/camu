package main

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
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
