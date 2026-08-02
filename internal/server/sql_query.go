package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/maksim/camu/internal/storage"
)

type sqlQueryRequest struct {
	SQL       string        `json:"sql"`
	Params    []any         `json:"params"`
	Topics    []string      `json:"topics"`
	TimeRange *sqlTimeRange `json:"time_range,omitempty"`
	Limit     int           `json:"limit,omitempty"`
}

type sqlTimeRange struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type sqlQueryScope struct {
	Topics    []string
	From      time.Time
	To        time.Time
	Manifests map[string][]ParquetManifest
}

func (s *Server) handleSQLQuery(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	s.metricInc("camu_sql_queries_total", "SQL queries", map[string]string{"result": "started"})
	defer func() {
		s.metricObserve("camu_sql_query_duration", "SQL query duration", nil, time.Since(started))
	}()
	var req sqlQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.metricInc("camu_sql_queries_total", "SQL queries", map[string]string{"result": "error"})
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	ctx, cancel, err := s.sqlRequestContext(r.Context())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer cancel()

	select {
	case s.sqlLimiter <- struct{}{}:
		defer func() { <-s.sqlLimiter }()
	case <-ctx.Done():
		writeError(w, http.StatusBadRequest, ctx.Err().Error())
		return
	}

	scope, err := s.resolveSQLQueryScope(ctx, req)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrNotFound):
			writeError(w, http.StatusNotFound, "topic not found")
		default:
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	resp, err := s.executeSQLQuery(ctx, req, scope)
	if err != nil {
		s.metricInc("camu_sql_queries_total", "SQL queries", map[string]string{"result": "error"})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.metricInc("camu_sql_queries_total", "SQL queries", map[string]string{"result": "success"})
	s.metricAdd("camu_sql_rows_returned_total", "SQL rows returned", nil, float64(len(resp.Rows)))
	writeJSON(w, http.StatusOK, resp)
}

// sqlRequestContext applies the lifetime limits that cover all SQL request
// work, including topic and manifest resolution. sqlCtx is cancelled by
// Shutdown, so object-store calls made before DuckDB execution are cancelled
// alongside an in-flight query.
func (s *Server) sqlRequestContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	timeout, err := s.cfg.SQL.QueryTimeoutDuration()
	if err != nil {
		return nil, nil, fmt.Errorf("invalid sql.query_timeout: %w", err)
	}

	ctx, cancel := context.WithCancel(parent)
	stopServerCancel := func() {}
	if s.sqlCtx != nil {
		stop := context.AfterFunc(s.sqlCtx, cancel)
		stopServerCancel = func() { _ = stop() }
	}
	if timeout <= 0 {
		return ctx, func() {
			stopServerCancel()
			cancel()
		}, nil
	}
	timedCtx, timeoutCancel := context.WithTimeout(ctx, timeout)
	return timedCtx, func() {
		stopServerCancel()
		timeoutCancel()
		cancel()
	}, nil
}

func (s *Server) resolveSQLQueryScope(ctx context.Context, req sqlQueryRequest) (sqlQueryScope, error) {
	if req.SQL == "" {
		return sqlQueryScope{}, fmt.Errorf("sql is required")
	}
	if len(req.Topics) == 0 {
		return sqlQueryScope{}, fmt.Errorf("at least one topic is required")
	}
	if req.Limit < 0 {
		return sqlQueryScope{}, fmt.Errorf("limit must be >= 0")
	}
	from, to, err := parseSQLTimeRange(req.TimeRange)
	if err != nil {
		return sqlQueryScope{}, err
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return sqlQueryScope{}, fmt.Errorf("time_range.to must be >= time_range.from")
	}

	topics := append([]string(nil), req.Topics...)
	slices.Sort(topics)
	topics = slices.Compact(topics)
	for _, topic := range topics {
		if err := validateTopicName(topic); err != nil {
			return sqlQueryScope{}, err
		}
	}

	scope := sqlQueryScope{
		Topics:    topics,
		From:      from,
		To:        to,
		Manifests: make(map[string][]ParquetManifest, len(topics)),
	}
	for _, topic := range topics {
		if _, err := s.topicStore.Get(ctx, topic); err != nil {
			return sqlQueryScope{}, err
		}
		manifests, err := s.listParquetManifestsForTopic(ctx, topic, from, to)
		if err != nil {
			return sqlQueryScope{}, err
		}
		scope.Manifests[topic] = manifests
	}
	return scope, nil
}

func parseSQLTimeRange(tr *sqlTimeRange) (time.Time, time.Time, error) {
	if tr == nil {
		return time.Time{}, time.Time{}, nil
	}
	var from time.Time
	var to time.Time
	var err error
	if tr.From != "" {
		from, err = time.Parse(time.RFC3339, tr.From)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid time_range.from")
		}
	}
	if tr.To != "" {
		to, err = time.Parse(time.RFC3339, tr.To)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid time_range.to")
		}
	}
	return from.UTC(), to.UTC(), nil
}
