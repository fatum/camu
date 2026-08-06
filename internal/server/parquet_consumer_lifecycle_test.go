package server

import (
	"testing"
	"time"
)

// TestParquetConsumerPollIntervalFor verifies the export consumer backs off to
// the idle poll interval when a pass exports nothing (checkpoint unchanged) and
// returns to the fast interval as soon as the checkpoint advances. This keeps
// idle export polling (which re-reads the committed head / clones the index)
// from burning CPU while new data still gets exported with low latency.
func TestParquetConsumerPollIntervalFor(t *testing.T) {
	if got := parquetConsumerPollIntervalFor(100, 100); got != parquetConsumerIdlePollInterval {
		t.Fatalf("idle interval = %v, want %v", got, parquetConsumerIdlePollInterval)
	}
	if got := parquetConsumerPollIntervalFor(100, 150); got != parquetConsumerPollInterval {
		t.Fatalf("active interval = %v, want %v", got, parquetConsumerPollInterval)
	}
	if parquetConsumerIdlePollInterval <= parquetConsumerPollInterval {
		t.Fatalf("idle interval %v must exceed poll interval %v", parquetConsumerIdlePollInterval, parquetConsumerPollInterval)
	}
	if parquetConsumerIdlePollInterval > 5*time.Second {
		t.Fatalf("idle interval %v is too large for acceptable export latency", parquetConsumerIdlePollInterval)
	}
}
