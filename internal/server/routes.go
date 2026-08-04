package server

import (
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"
)

func (s *Server) publicRoutes() http.Handler {
	return s.withMiddleware(s.publicAPIHandler())
}

func (s *Server) publicAPIHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	if s.cfg.Server.HeapProfileEnabled && s.cfg.Server.AuthToken != "" {
		mux.Handle("GET /v1/debug/heap", s.requireBearerAuth(http.HandlerFunc(s.handleHeapProfile), "camu-debug"))
	}
	if s.cfg.SQL.EnabledValue(s.isQueryMode()) {
		mux.Handle("POST /v1/sql", s.requireBearerAuth(http.HandlerFunc(s.handleSQLQuery), "camu-sql"))
	}
	if s.isQueryMode() {
		mux.HandleFunc("GET /v1/ready", s.handleReady)
		mux.HandleFunc("GET /v1/cluster/status", s.handleClusterStatus)
		mux.HandleFunc("GET /v1/cluster/ready", s.handleClusterReady)
		return mux
	}
	mux.HandleFunc("POST /v1/topics", s.handleCreateTopic)
	mux.HandleFunc("GET /v1/topics", s.handleListTopics)
	mux.HandleFunc("GET /v1/topics/{topic}", s.handleGetTopic)
	mux.HandleFunc("DELETE /v1/topics/{topic}", s.handleDeleteTopic)
	mux.HandleFunc("GET /v1/ready", s.handleReady)
	mux.HandleFunc("GET /v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("GET /v1/cluster/ready", s.handleClusterReady)
	mux.HandleFunc("GET /v1/topics/{topic}/routing", s.handleRouting)
	mux.HandleFunc("POST /v1/topics/{topic}/messages", s.handleProduceHighLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/partitions/{id}/messages", s.handleProduceLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/messages", s.handleConsumeLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/stream", s.handleStreamLowLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/offsets/{consumer_id}", s.handleCommitConsumerOffsets)
	mux.HandleFunc("GET /v1/topics/{topic}/offsets/{consumer_id}", s.handleGetConsumerOffsets)
	mux.HandleFunc("POST /v1/groups/{group_id}/commit", s.handleCommitOffsets)
	mux.HandleFunc("GET /v1/groups/{group_id}/offsets", s.handleGetOffsets)
	mux.HandleFunc("POST /v1/producers/init", s.handleInitProducer)
	return mux
}

func (s *Server) requireBearerAuth(next http.Handler, realm string) http.Handler {
	token := s.cfg.Server.AuthToken
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		auth := r.Header.Get("Authorization")
		if len(auth) <= len(prefix) || auth[:len(prefix)] != prefix || auth[len(prefix):] != token {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+realm+`"`)
			writeError(w, http.StatusUnauthorized, "valid bearer token required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHeapProfile(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="heap.pb.gz"`)
	runtime.GC()
	if err := pprof.WriteHeapProfile(w); err != nil {
		slog.Error("heap_profile_failed", "error", err)
	}
}

// PublicHandler returns the server's public API handler.
func (s *Server) PublicHandler() http.Handler {
	return s.publicRoutes()
}

// PublicAPIHandler returns the bare public API mux without middleware.
// Benchmarks can use this to avoid measuring logging and wrapper overhead.
func (s *Server) PublicAPIHandler() http.Handler {
	return s.publicAPIHandler()
}

func (s *Server) internalRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/ready", s.handleReady)
	mux.HandleFunc("GET /v1/internal/readiness", s.handleInternalReadiness)
	if s.isQueryMode() {
		return s.withMiddleware(mux)
	}
	// Produce endpoints are registered here so proxied requests from
	// non-leader nodes can be handled by the leader's internal server.
	mux.HandleFunc("POST /v1/topics/{topic}/messages", s.handleProduceHighLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/partitions/{id}/messages", s.handleProduceLowLevel)

	// Controller coordination endpoints.
	mux.HandleFunc("POST /v1/internal/report-failure", s.handleReportFailure)
	mux.HandleFunc("POST /v1/internal/report-isr", s.handleReportISR)
	mux.HandleFunc("POST /v1/internal/report-hw", s.handleReportHW)
	mux.HandleFunc("GET /v1/internal/assignments", s.handleGetAssignments)
	mux.HandleFunc("POST /v1/internal/push-assignments", s.handlePushAssignment)

	return s.withMiddleware(mux)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Camu-Instance-ID", s.instanceID)
		if r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		}
		handler := next
		if os.Getenv("CAMU_REQUEST_LOG") != "0" {
			handler = s.requestLogger(next)
		}
		handler.ServeHTTP(sw, r)
		path := r.Pattern
		if path == "" {
			path = "unmatched"
		}
		s.metricInc("camu_http_requests_total", "HTTP requests handled", map[string]string{"method": r.Method, "path": path, "status": strconv.Itoa(sw.status)})
		s.metricObserve("camu_http_request_duration", "HTTP request duration", map[string]string{"method": r.Method, "path": path}, time.Since(start))
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.metricSet("camu_runtime_heap_alloc_bytes", "Go heap bytes allocated by Camu", nil, float64(mem.HeapAlloc))
	s.metricSet("camu_runtime_heap_inuse_bytes", "Go heap bytes in use by Camu", nil, float64(mem.HeapInuse))
	s.metricSet("camu_runtime_memory_sys_bytes", "Go runtime memory obtained from the operating system", nil, float64(mem.Sys))
	s.metricSet("camu_runtime_goroutines", "Current number of Camu goroutines", nil, float64(runtime.NumGoroutine()))
	s.metricSet("camu_runtime_gc_cycles", "Completed Go garbage-collection cycles", nil, float64(mem.NumGC))
	_, _ = w.Write([]byte(s.metrics.Handler()))
}

func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip noisy health/status endpoints — log them at Debug level only.
		isNoise := r.URL.Path == "/metrics" || r.URL.Path == "/v1/cluster/status" || r.URL.Path == "/v1/cluster/ready" || r.URL.Path == "/v1/ready" || r.URL.Path == "/v1/internal/readiness"

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
