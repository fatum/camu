package server

import "net/http"

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/topics", s.handleCreateTopic)
	mux.HandleFunc("GET /v1/topics", s.handleListTopics)
	mux.HandleFunc("GET /v1/topics/{topic}", s.handleGetTopic)
	mux.HandleFunc("DELETE /v1/topics/{topic}", s.handleDeleteTopic)
	mux.HandleFunc("GET /v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("POST /v1/topics/{topic}/messages", s.handleProduceHighLevel)
	mux.HandleFunc("POST /v1/topics/{topic}/partitions/{id}/messages", s.handleProduceLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{id}/messages", s.handleConsumeLowLevel)
	mux.HandleFunc("GET /v1/topics/{topic}/consume", s.handleConsumeHighLevel)
	return s.withMiddleware(mux)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Camu-Instance-ID", s.instanceID)
		next.ServeHTTP(w, r)
	})
}
