package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWebAnalyticsSchemaFields(t *testing.T) {
	fields := webAnalyticsSchemaFields()
	if len(fields) != 15 {
		t.Fatalf("web analytics schema has %d fields, want 15", len(fields))
	}
	seen := map[string]bool{}
	fixed := map[string]bool{"record_offset": true, "record_timestamp": true, "key": true, "value": true, "headers": true}
	for _, f := range fields {
		name, _ := f["name"].(string)
		typ, _ := f["type"].(string)
		path, _ := f["path"].(string)
		if name == "" || seen[strings.ToLower(name)] || fixed[strings.ToLower(name)] {
			t.Fatalf("field %q is empty, duplicate, or collides with a fixed column", name)
		}
		seen[strings.ToLower(name)] = true
		switch typ {
		case "string", "int64", "float64", "bool", "timestamp":
		default:
			t.Fatalf("field %q has unsupported type %q", name, typ)
		}
		if len(path) < 3 || !strings.HasPrefix(path, "$.") {
			t.Fatalf("field %q path %q must start with $.", name, path)
		}
	}
}

func TestWebAnalyticsEventDeterministic(t *testing.T) {
	cfg := config{ExportEnabled: true, MessageBytes: 1024}
	base := typedValue{RunID: "run-a", ID: 42, Payload: payload(1024), PayloadBytes: 1024, Sequence: 42}
	first := benchmarkEvent(cfg, base).(webAnalyticsEvent)
	second := benchmarkEvent(cfg, base).(webAnalyticsEvent)
	if first != second {
		t.Fatal("web analytics event is not deterministic")
	}
	if _, err := time.Parse(time.RFC3339, first.EventTime); err != nil {
		t.Fatalf("event_time %q is not RFC3339: %v", first.EventTime, err)
	}
	if first.EventType != "add_to_cart" { // 42 % 4 == 2 is "add_to_cart"
		t.Fatalf("event_type = %q, want add_to_cart", first.EventType)
	}
	if first.EventID != 42 || first.UserID != "user-42" || first.DurationMS != 43 {
		t.Fatalf("unexpected event fields: %+v", first)
	}
}

func TestWebAnalyticsEventRoundTripsThroughTypedValue(t *testing.T) {
	cfg := config{ExportEnabled: true, MessageBytes: 1}
	original := typedValue{RunID: "run-b", ID: 7, Payload: "x", PayloadBytes: 1, Sequence: 7}
	encoded, err := json.Marshal(benchmarkEvent(cfg, original))
	if err != nil {
		t.Fatal(err)
	}
	var decoded typedValue
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal web analytics event into typedValue: %v", err)
	}
	if decoded != original {
		t.Fatalf("round trip = %+v, want %+v", decoded, original)
	}
}

func TestWebAnalyticsDimensionsAreSchemaColumns(t *testing.T) {
	fields := map[string]bool{}
	for _, f := range webAnalyticsSchemaFields() {
		fields[f["name"].(string)] = true
	}
	for _, dim := range webAnalyticsDimensions {
		if !fields[dim.Column] {
			t.Fatalf("dimension column %q is not a schema column — the SQL GROUP BY check would fail", dim.Column)
		}
		if len(dim.pool) == 0 {
			t.Fatalf("dimension %q has an empty value pool", dim.Column)
		}
	}
}

func TestVerifyWebAnalyticsSQL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if strings.Contains(req.SQL, "distinct event_id") {
			_, _ = w.Write([]byte(`{"rows":[[1000,1000]]}`))
			return
		}
		_, _ = w.Write([]byte(webAnalyticsGroupByRows(t, req.SQL, 1000)))
	}))
	defer server.Close()
	cfg := config{ExportEnabled: true, Topic: "events"}
	c := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}
	if err := verifyWebAnalyticsSQL(context.Background(), c, cfg, 1000); err != nil {
		t.Fatalf("verifyWebAnalyticsSQL() error = %v", err)
	}
}

func TestVerifyWebAnalyticsSQLRejectsDuplicates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"rows":[[1000,999]]}`))
	}))
	defer server.Close()
	cfg := config{ExportEnabled: true, Topic: "events"}
	c := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}
	err := verifyWebAnalyticsSQL(context.Background(), c, cfg, 1000)
	if err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("verifyWebAnalyticsSQL() error = %v, want duplicate detection", err)
	}
}

func TestVerifyWebAnalyticsSQLRejectsUnexpectedDimensionValue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if strings.Contains(req.SQL, "distinct event_id") {
			_, _ = w.Write([]byte(`{"rows":[[1000,1000]]}`))
			return
		}
		if strings.Contains(req.SQL, "event_type") {
			_, _ = w.Write([]byte(`{"rows":[["bogus",1000]]}`))
			return
		}
		_, _ = w.Write([]byte(webAnalyticsGroupByRows(t, req.SQL, 1000)))
	}))
	defer server.Close()
	cfg := config{ExportEnabled: true, Topic: "events"}
	c := client{base: server.URL, http: &http.Client{}, requestTimeout: time.Second}
	err := verifyWebAnalyticsSQL(context.Background(), c, cfg, 1000)
	if err == nil || !strings.Contains(err.Error(), "unexpected value") {
		t.Fatalf("verifyWebAnalyticsSQL() error = %v, want unexpected-value rejection", err)
	}
}

// webAnalyticsGroupByRows returns the JSON rows a correct /v1/sql GROUP BY
// response would contain for the given query and record count, mirroring the
// deterministic sequence-derived distribution.
func webAnalyticsGroupByRows(t *testing.T, query string, count int64) string {
	t.Helper()
	body := strings.TrimPrefix(query, "SELECT ")
	col := strings.TrimSpace(strings.SplitN(body, ",", 2)[0])
	for _, dim := range webAnalyticsDimensions {
		if dim.Column == col {
			poolSize := int64(len(dim.pool))
			q, rem := count/poolSize, count%poolSize
			rows := [][]any{}
			for i, value := range dim.pool {
				n := q
				if int64(i) < rem {
					n++
				}
				if n > 0 {
					rows = append(rows, []any{value, n})
				}
			}
			b, err := json.Marshal(map[string]any{"rows": rows})
			if err != nil {
				t.Fatalf("marshal rows: %v", err)
			}
			return string(b)
		}
	}
	t.Fatalf("unexpected dimension column %q in query: %s", col, query)
	return ""
}

func TestBenchmarkEventDefaultsToTypedValue(t *testing.T) {
	cfg := config{ExportEnabled: false}
	value := typedValue{RunID: "run", ID: 1, Payload: "p", PayloadBytes: 1, Sequence: 1}
	if got, ok := benchmarkEvent(cfg, value).(typedValue); !ok || got != value {
		t.Fatalf("benchmarkEvent() with default config = %#v, want typedValue", benchmarkEvent(cfg, value))
	}
}

func TestBenchmarkSchemaFieldsByExport(t *testing.T) {
	exported := benchmarkSchemaFields(config{ExportEnabled: true})
	if len(exported) != 15 {
		t.Fatalf("exported topic schema has %d fields, want 15", len(exported))
	}
	plain := benchmarkSchemaFields(config{ExportEnabled: false})
	if len(plain) != 4 {
		t.Fatalf("non-exported topic schema has %d fields, want 4", len(plain))
	}
}
