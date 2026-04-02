//go:build integration

package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maksim/camu/internal/server"
	"github.com/maksim/camu/pkg/camutest"
)

type apiBenchClient struct {
	t       testing.TB
	handler http.Handler
}

func newAPIBenchClient(t testing.TB, srv *server.Server) *apiBenchClient {
	t.Helper()
	return &apiBenchClient{
		t:       t,
		handler: srv.PublicAPIHandler(),
	}
}

func (c *apiBenchClient) createTopic(name string, partitions int, retention time.Duration) error {
	c.t.Helper()
	body, err := json.Marshal(map[string]any{
		"name":       name,
		"partitions": partitions,
		"retention":  retention.String(),
	})
	if err != nil {
		return err
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		return fmt.Errorf("create topic: status %d: %s", rec.Code, rec.Body.String())
	}
	return nil
}

func (c *apiBenchClient) produce(topic string, msgs []camutest.ProduceMessage) (*camutest.ProduceResponse, error) {
	return c.produceToPartition(topic, -1, msgs)
}

func (c *apiBenchClient) produceToPartition(topic string, partition int, msgs []camutest.ProduceMessage) (*camutest.ProduceResponse, error) {
	c.t.Helper()
	body, err := json.Marshal(msgs)
	if err != nil {
		return nil, err
	}
	rec := httptest.NewRecorder()
	path := "/v1/topics/" + topic + "/messages"
	if partition >= 0 {
		path = fmt.Sprintf("/v1/topics/%s/partitions/%d/messages", topic, partition)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("topic", topic)
	if partition >= 0 {
		req.SetPathValue("id", fmt.Sprintf("%d", partition))
	}
	c.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return nil, fmt.Errorf("produce: status %d: %s", rec.Code, rec.Body.String())
	}
	var resp camutest.ProduceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *apiBenchClient) consume(topic string, partition int, offset uint64, limit int) (*camutest.ConsumeResponse, error) {
	c.t.Helper()
	path := fmt.Sprintf("/v1/topics/%s/partitions/%d/messages?offset=%d&limit=%d", topic, partition, offset, limit)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.SetPathValue("topic", topic)
	req.SetPathValue("id", fmt.Sprintf("%d", partition))
	c.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return nil, fmt.Errorf("consume: status %d: %s", rec.Code, rec.Body.String())
	}
	var resp camutest.ConsumeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
