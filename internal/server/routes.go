package server

import (
	"log/slog"
	"net/http"
	"time"
)

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/topics", s.handleCreateTopic)
	mux.HandleFunc("GET /v1/topics", s.handleListTopics)
	mux.HandleFunc("GET /v1/topics/{topic}", s.handleGetTopic)
	mux.HandleFunc("DELETE /v1/topics/{topic}", s.handleDeleteTopic)
	mux.HandleFunc("GET /v1/ready", s.handleReady)
	mux.HandleFunc("GET /v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("GET /v1/topics/{topic}/routing", s.handleRouting)
	mux.HandleFunc("POST /v1/topics/{topic}/messages", s.handleProduceHighLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/partitions/{id}/messages", s.handleProduceLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/messages", s.handleConsumeLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/stream", s.handleStreamLowLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/offsets/{consumer_id}", s.handleCommitConsumerOffsets)
	mux.HandleFunc("GET /v1/topics/{topic}/offsets/{consumer_id}", s.handleGetConsumerOffsets)
	mux.HandleFunc("POST /v1/groups/{group_id}/commit", s.handleCommitOffsets)
	mux.HandleFunc("GET /v1/groups/{group_id}/offsets", s.handleGetOffsets)
	mux.HandleFunc("GET /v1/internal/replicate/{topic}/{pid}", s.handleReplicaFetch)
	return s.withMiddleware(mux)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Camu-Instance-ID", s.instanceID)
		s.requestLogger(next).ServeHTTP(w, r)
	})
}

func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip noisy health/status endpoints — log them at Debug level only.
		isNoise := r.URL.Path == "/v1/cluster/status" || r.URL.Path == "/v1/ready"

		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)

		if isNoise {
			slog.Debug("http_request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"instance_id", s.instanceID,
			)
			return
		}

		slog.Info("http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"instance_id", s.instanceID,
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush passes through to the underlying ResponseWriter (needed for SSE).
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
