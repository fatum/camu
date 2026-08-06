package main

import (
	"fmt"
	"time"
)

// webAnalyticsEvent is the message value used when WEB_ANALYTICS is enabled. It
// embeds the integrity fields consumed by typedValue plus fifteen typed
// web-analytics columns exported to Parquet as Iceberg table columns. All
// fields are derived deterministically from Sequence so produce and consume
// agree on the payload digest without extra hashing.
type webAnalyticsEvent struct {
	RunID        string  `json:"run_id"`
	ID           int64   `json:"id"`
	Payload      string  `json:"payload"`
	PayloadBytes int64   `json:"payload_bytes"`
	Sequence     int64   `json:"sequence"`
	EventID      int64   `json:"event_id"`
	UserID       string  `json:"user_id"`
	SessionID    string  `json:"session_id"`
	PageURL      string  `json:"page_url"`
	Referrer     string  `json:"referrer"`
	UserAgent    string  `json:"user_agent"`
	DeviceType   string  `json:"device_type"`
	Browser      string  `json:"browser"`
	OS           string  `json:"os"`
	Country      string  `json:"country"`
	City         string  `json:"city"`
	EventType    string  `json:"event_type"`
	EventTime    string  `json:"event_time"`
	DurationMS   int64   `json:"duration_ms"`
	Revenue      float64 `json:"revenue"`
}

var webAnalyticsPages = []string{"/", "/product", "/checkout", "/cart", "/login", "/search", "/blog", "/pricing", "/docs", "/about"}

var webAnalyticsReferrers = []string{"https://google.com", "https://twitter.com", "https://github.com", "https://news.ycombinator.com", "", "https://facebook.com", "https://instagram.com"}

var webAnalyticsUserAgents = []string{
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

var webAnalyticsDeviceTypes = []string{"desktop", "mobile", "tablet", "tv"}

var webAnalyticsBrowsers = []string{"chrome", "safari", "firefox", "edge"}

var webAnalyticsOSes = []string{"macos", "ios", "linux", "windows", "android"}

var webAnalyticsCountries = []string{"US", "GB", "DE", "FR", "JP", "BR", "IN", "CA"}

var webAnalyticsCities = []string{"New York", "London", "Berlin", "Paris", "Tokyo", "Sao Paulo", "Mumbai", "Toronto"}

var webAnalyticsEventTypes = []string{"page_view", "click", "add_to_cart", "purchase"}

// benchmarkSchemaFields returns the topic schema fields for the selected
// benchmark profile. Exported topics use the web-analytics profile so the
// typed columns actually reach Parquet and are queryable through SQL; the
// legacy profile (matching the plain typedValue fields) is used for pure
// throughput runs without export.
func benchmarkSchemaFields(cfg config) []map[string]any {
	if cfg.ExportEnabled {
		return webAnalyticsSchemaFields()
	}
	return []map[string]any{
		{"name": "id", "type": "int64", "path": "$.id"},
		{"name": "payload", "type": "string", "path": "$.payload"},
		{"name": "payload_bytes", "type": "int64", "path": "$.payload_bytes"},
		{"name": "sequence", "type": "int64", "path": "$.sequence"},
	}
}

func webAnalyticsSchemaFields() []map[string]any {
	return []map[string]any{
		{"name": "event_id", "type": "int64", "path": "$.event_id"},
		{"name": "user_id", "type": "string", "path": "$.user_id"},
		{"name": "session_id", "type": "string", "path": "$.session_id"},
		{"name": "page_url", "type": "string", "path": "$.page_url"},
		{"name": "referrer", "type": "string", "path": "$.referrer"},
		{"name": "user_agent", "type": "string", "path": "$.user_agent"},
		{"name": "device_type", "type": "string", "path": "$.device_type"},
		{"name": "browser", "type": "string", "path": "$.browser"},
		{"name": "os", "type": "string", "path": "$.os"},
		{"name": "country", "type": "string", "path": "$.country"},
		{"name": "city", "type": "string", "path": "$.city"},
		{"name": "event_type", "type": "string", "path": "$.event_type"},
		{"name": "event_time", "type": "timestamp", "path": "$.event_time"},
		{"name": "duration_ms", "type": "int64", "path": "$.duration_ms"},
		{"name": "revenue", "type": "float64", "path": "$.revenue"},
	}
}

// benchmarkEvent returns the JSON-marshalable message value for a benchmark
// record. Exported topics widen the value with the web-analytics columns while
// keeping the typedValue fields intact so the integrity digest is unchanged.
func benchmarkEvent(cfg config, value typedValue) any {
	if cfg.ExportEnabled {
		return webAnalyticsEventValue(value)
	}
	return value
}

func webAnalyticsEventValue(value typedValue) webAnalyticsEvent {
	seq := value.Sequence
	return webAnalyticsEvent{
		RunID:        value.RunID,
		ID:           value.ID,
		Payload:      value.Payload,
		PayloadBytes: value.PayloadBytes,
		Sequence:     value.Sequence,
		EventID:      seq,
		UserID:       fmt.Sprintf("user-%d", seq%10000),
		SessionID:    fmt.Sprintf("session-%d", (seq/3)%50000),
		PageURL:      webAnalyticsPages[int(seq%int64(len(webAnalyticsPages)))],
		Referrer:     webAnalyticsReferrers[int(seq%int64(len(webAnalyticsReferrers)))],
		UserAgent:    webAnalyticsUserAgents[int(seq%int64(len(webAnalyticsUserAgents)))],
		DeviceType:   webAnalyticsDeviceTypes[int(seq%int64(len(webAnalyticsDeviceTypes)))],
		Browser:      webAnalyticsBrowsers[int(seq%int64(len(webAnalyticsBrowsers)))],
		OS:           webAnalyticsOSes[int(seq%int64(len(webAnalyticsOSes)))],
		Country:      webAnalyticsCountries[int(seq%int64(len(webAnalyticsCountries)))],
		City:         webAnalyticsCities[int(seq%int64(len(webAnalyticsCities)))],
		EventType:    webAnalyticsEventTypes[int(seq%int64(len(webAnalyticsEventTypes)))],
		EventTime:    time.Unix(1700000000+seq, 0).UTC().Format(time.RFC3339),
		DurationMS:   seq%60000 + 1,
		Revenue:      float64(seq%1000) / 100,
	}
}
