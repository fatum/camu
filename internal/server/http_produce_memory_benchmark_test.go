//go:build benchmark

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

// BenchmarkHTTPProduceMemoryPressure measures the complete high-level HTTP
// produce path. It is opt-in and excluded from normal local and CI tests.
func BenchmarkHTTPProduceMemoryPressure(b *testing.B) {
	s := newTestServer(b)
	setupTestTopicAndOwnership(b, s)
	handler := s.publicRoutes()
	body := httpProduceBenchmarkBody(500, 1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/test-topic/messages", bytes.NewReader(body))
		req.SetPathValue("topic", "test-topic")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
		}
		runtime.KeepAlive(rec)
	}
}

func httpProduceBenchmarkBody(records, payloadBytes int) []byte {
	payload := strings.Repeat("x", payloadBytes)
	messages := make([]produceMessageRequest, records)
	for i := range messages {
		messages[i] = produceMessageRequest{
			Key:   "benchmark",
			Value: payload,
		}
	}
	body, err := json.Marshal(messages)
	if err != nil {
		panic(err)
	}
	return body
}
