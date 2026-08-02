package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestRegistryRendersStablePrometheusSamples(t *testing.T) {
	r := NewRegistry()
	r.Inc("camu_requests_total", "Requests", map[string]string{"status": "200", "method": "GET"})
	r.Inc("camu_requests_total", "Requests", map[string]string{"method": "GET", "status": "200"})
	r.Observe("camu_request_duration", "Request duration", nil, 25*time.Millisecond)

	got := r.Handler()
	for _, want := range []string{
		"# TYPE camu_requests_total counter",
		`camu_requests_total{method="GET",status="200"} 2`,
		"camu_request_duration_seconds_count 1",
		"camu_request_duration_seconds_sum 0.025",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, got)
		}
	}
}
