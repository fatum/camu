package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandleUpdateTopicSchema verifies the full schema-evolution path: POST
// /v1/topics/{topic}/schema returns 200, updates the topic configuration, and
// advances the Iceberg table schema (the endpoint previously always failed
// with 500 because TopicStore.Update rejected schema changes).
func TestHandleUpdateTopicSchema(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.registry.Register(ctx); err != nil {
		t.Fatalf("register instance: %v", err)
	}

	create := httptest.NewRequest(http.MethodPost, "/v1/topics", strings.NewReader(`{
"name":"orders","partitions":1,
"schema":{"encoding":"json","fields":[{"name":"id","type":"int64","path":"$.id"}]}
}`))
	rec := httptest.NewRecorder()
	s.PublicHandler().ServeHTTP(rec, create)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/topics status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}

	update := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/schema", strings.NewReader(`{
"schema":{"encoding":"json","fields":[
{"name":"id","type":"int64","path":"$.id"},
{"name":"note","type":"string","path":"$.note","nullable":true}
]}
}`))
	rec = httptest.NewRecorder()
	s.PublicHandler().ServeHTTP(rec, update)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/topics/orders/schema status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Version != 1 {
		t.Fatalf("response version = %d, want 1", resp.Version)
	}

	tc, err := s.topicStore.Get(ctx, "orders")
	if err != nil {
		t.Fatalf("topicStore.Get() error = %v", err)
	}
	if tc.Schema == nil || tc.Schema.Version != 1 || len(tc.Schema.Fields) != 2 {
		t.Fatalf("topic schema after update = %+v, want version 1 with 2 fields", tc.Schema)
	}

	md, err := s.icebergTableStoreFor().Load(ctx, "orders")
	if err != nil {
		t.Fatalf("iceberg table load: %v", err)
	}
	if md.CurrentSchemaID != 1 {
		t.Fatalf("iceberg current-schema-id = %d, want 1 (table advanced on schema update)", md.CurrentSchemaID)
	}
}

// TestHandleUpdateTopicSchemaMissingTopic verifies the endpoint fails with 404
// and does not register a schema version for a topic that does not exist.
func TestHandleUpdateTopicSchemaMissingTopic(t *testing.T) {
	s := newTestServer(t)
	update := httptest.NewRequest(http.MethodPost, "/v1/topics/nope/schema", strings.NewReader(`{
"schema":{"encoding":"json","fields":[{"name":"id","type":"int64","path":"$.id"}]}
}`))
	rec := httptest.NewRecorder()
	s.PublicHandler().ServeHTTP(rec, update)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("POST schema for missing topic status = %d, want 404; body=%s", rec.Code, rec.Body.String())
	}
}
