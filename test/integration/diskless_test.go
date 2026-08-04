//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/pkg/camutest"
)

// createDisklessTopic creates a topic with storage_mode "diskless" via raw HTTP POST.
func createDisklessTopic(t *testing.T, client *camutest.Client, name string, partitions int) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"name":         name,
		"partitions":   partitions,
		"retention":    "24h",
		"storage_mode": "diskless",
	})
	if err != nil {
		t.Fatalf("marshal create topic request: %v", err)
	}

	resp, err := http.Post(client.BaseURL()+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/topics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var ae struct{ Error string }
		json.NewDecoder(resp.Body).Decode(&ae)
		t.Fatalf("create diskless topic %q: status %d: %s", name, resp.StatusCode, ae.Error)
	}
}

func streamDisklessSSE(t *testing.T, client *camutest.Client, topic string, partition int, offset uint64, lastEventID string, maxEvents int, timeout time.Duration) []camutest.ConsumedMessage {
	t.Helper()

	url := fmt.Sprintf("%s/v1/topics/%s/partitions/%d/stream?offset=%d", client.BaseURL(), topic, partition, offset)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new SSE request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("do SSE request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae struct{ Error string }
		_ = json.NewDecoder(resp.Body).Decode(&ae)
		t.Fatalf("SSE status %d: %s", resp.StatusCode, ae.Error)
	}

	scanner := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(timeout)
	var events []camutest.ConsumedMessage
	var currentID string
	var currentData string

	for time.Now().Before(deadline) && len(events) < maxEvents {
		type scanResult struct {
			text string
			ok   bool
		}
		ch := make(chan scanResult, 1)
		go func() {
			ok := scanner.Scan()
			ch <- scanResult{text: scanner.Text(), ok: ok}
		}()

		var line string
		select {
		case res := <-ch:
			if !res.ok {
				return events
			}
			line = res.text
		case <-time.After(time.Until(deadline)):
			return events
		}

		switch {
		case strings.HasPrefix(line, "id: "):
			currentID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			currentData = strings.TrimPrefix(line, "data: ")
		case line == "":
			if currentData == "" {
				currentID = ""
				continue
			}
			var msg camutest.ConsumedMessage
			if err := json.Unmarshal([]byte(currentData), &msg); err != nil {
				t.Fatalf("unmarshal SSE data: %v", err)
			}
			if currentID != "" {
				if _, err := fmt.Sscan(currentID, &msg.Offset); err != nil {
					t.Fatalf("parse SSE id %q: %v", currentID, err)
				}
			}
			events = append(events, msg)
			currentID = ""
			currentData = ""
		}
	}

	return events
}

func TestDiskless_HTTPProduceAndConsume(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-http-e2e"
	createDisklessTopic(t, client, topic, 1)
	waitForPartitionProduceReady(t, client, topic, 0)

	// waitForPartitionProduceReady produced a warmup message at offset 0.	// Our messages will start at offset 1.
	msgs := []camutest.ProduceMessage{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
		{Key: "k3", Value: "v3"},
	}
	_, err := client.ProduceToPartition(topic, 0, msgs)
	if err != nil {
		t.Fatalf("ProduceToPartition: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Consume from offset 1 to skip the warmup message.
	resp, err := client.Consume(topic, 0, 1, 100)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if len(resp.Messages) != 3 {
		t.Fatalf("got %d messages, want 3", len(resp.Messages))
	}

	for i, msg := range resp.Messages {
		wantOffset := uint64(i + 1)
		if msg.Offset != wantOffset {
			t.Errorf("message[%d].Offset = %d, want %d", i, msg.Offset, wantOffset)
		}
	}
	if resp.Messages[0].Key != "k1" || resp.Messages[0].Value != "v1" {
		t.Errorf("message[0] = %q/%q, want k1/v1", resp.Messages[0].Key, resp.Messages[0].Value)
	}
	if resp.Messages[1].Key != "k2" || resp.Messages[1].Value != "v2" {
		t.Errorf("message[1] = %q/%q, want k2/v2", resp.Messages[1].Key, resp.Messages[1].Value)
	}
	if resp.Messages[2].Key != "k3" || resp.Messages[2].Value != "v3" {
		t.Errorf("message[2] = %q/%q, want k3/v3", resp.Messages[2].Key, resp.Messages[2].Value)
	}
}

// TestDiskless_S3MetaStoreProduceConsume runs the full diskless HTTP path with
// the S3-backed metastore, verifying offset allocation and segment catalog
// queries work against object storage without DynamoDB.
func TestDiskless_S3MetaStoreProduceConsume(t *testing.T) {
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Diskless.MetaStore = "s3"
		}),
	)
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-s3-metastore"
	createDisklessTopic(t, client, topic, 1)
	waitForPartitionProduceReady(t, client, topic, 0)

	// waitForPartitionProduceReady produced a warmup message at offset 0.
	msgs := []camutest.ProduceMessage{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
	}
	if _, err := client.ProduceToPartition(topic, 0, msgs); err != nil {
		t.Fatalf("ProduceToPartition: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	resp, err := client.Consume(topic, 0, 0, 100)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("got %d messages, want 3 (warmup + 2)", len(resp.Messages))
	}
	for i, msg := range resp.Messages {
		if msg.Offset != uint64(i) {
			t.Errorf("message[%d].Offset = %d, want %d (contiguous)", i, msg.Offset, i)
		}
	}
	if resp.Messages[1].Key != "k1" || resp.Messages[2].Key != "k2" {
		t.Errorf("messages = %q, want [warmup k1 k2]", []string{resp.Messages[0].Key, resp.Messages[1].Key, resp.Messages[2].Key})
	}
}

func TestDiskless_MultipleFlushes(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-multi-flush"
	createDisklessTopic(t, client, topic, 1)
	waitForPartitionProduceReady(t, client, topic, 0)

	// Warmup took offset 0. Our messages start at offset 1.
	_, err := client.ProduceToPartition(topic, 0, []camutest.ProduceMessage{
		{Key: "a", Value: "first"},
	})
	if err != nil {
		t.Fatalf("first produce: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	_, err = client.ProduceToPartition(topic, 0, []camutest.ProduceMessage{
		{Key: "b", Value: "second"},
	})
	if err != nil {
		t.Fatalf("second produce: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Consume from offset 1 to skip warmup.
	resp, err := client.Consume(topic, 0, 1, 100)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if len(resp.Messages) != 2 {
		t.Fatalf("got %d messages, want 2", len(resp.Messages))
	}
	if resp.Messages[0].Offset != 1 {
		t.Errorf("message[0].Offset = %d, want 1", resp.Messages[0].Offset)
	}
	if resp.Messages[1].Offset != 2 {
		t.Errorf("message[1].Offset = %d, want 2", resp.Messages[1].Offset)
	}
	if resp.Messages[0].Value != "first" {
		t.Errorf("message[0].Value = %q, want %q", resp.Messages[0].Value, "first")
	}
	if resp.Messages[1].Value != "second" {
		t.Errorf("message[1].Value = %q, want %q", resp.Messages[1].Value, "second")
	}
}

func TestDiskless_HighLevelProduce(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-highlevel"
	createDisklessTopic(t, client, topic, 4)
	for p := 0; p < 4; p++ {
		waitForPartitionProduceReady(t, client, topic, p)
	}

	// Each partition has 1 warmup message at offset 0.
	msgs := []camutest.ProduceMessage{
		{Key: "a", Value: "v1"},
		{Key: "b", Value: "v2"},
		{Key: "c", Value: "v3"},
		{Key: "d", Value: "v4"},
		{Key: "e", Value: "v5"},
	}
	_, err := client.Produce(topic, msgs)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Count only non-warmup messages (offset > 0).
	total := 0
	for p := 0; p < 4; p++ {
		resp, err := client.Consume(topic, p, 0, 100)
		if err != nil {
			t.Fatalf("Consume partition %d: %v", p, err)
		}
		for _, m := range resp.Messages {
			if m.Offset > 0 {
				total++
			}
		}
	}

	if total != 5 {
		t.Fatalf("total non-warmup messages across partitions = %d, want 5", total)
	}
}

func TestDiskless_HTTPConsumeHonorsMessageLimit(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-consume-limit"
	createDisklessTopic(t, client, topic, 1)
	waitForPartitionProduceReady(t, client, topic, 0)

	largeValue := strings.Repeat("x", 1500)
	for i := 0; i < 3; i++ {
		_, err := client.ProduceToPartition(topic, 0, []camutest.ProduceMessage{
			{Key: fmt.Sprintf("k%d", i+1), Value: fmt.Sprintf("%s-%d", largeValue, i+1)},
		})
		if err != nil {
			t.Fatalf("produce large message %d: %v", i+1, err)
		}
		time.Sleep(700 * time.Millisecond)
	}

	resp, err := client.Consume(topic, 0, 1, 2)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if len(resp.Messages) != 2 {
		t.Fatalf("got %d messages, want 2", len(resp.Messages))
	}
	if resp.Messages[0].Offset != 1 {
		t.Fatalf("first offset = %d, want 1", resp.Messages[0].Offset)
	}
	if resp.Messages[1].Offset != 2 {
		t.Fatalf("second offset = %d, want 2", resp.Messages[1].Offset)
	}
	if resp.NextOffset != 3 {
		t.Fatalf("NextOffset = %d, want 3", resp.NextOffset)
	}
}

func TestDiskless_SSELastEventIDResumesAfterSeenEvent(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-sse-resume"
	createDisklessTopic(t, client, topic, 1)
	waitForPartitionProduceReady(t, client, topic, 0)

	for i := 0; i < 3; i++ {
		_, err := client.ProduceToPartition(topic, 0, []camutest.ProduceMessage{
			{Key: fmt.Sprintf("k%d", i+1), Value: fmt.Sprintf("msg-%d", i+1)},
		})
		if err != nil {
			t.Fatalf("produce message %d: %v", i+1, err)
		}
		time.Sleep(700 * time.Millisecond)
	}

	first := streamDisklessSSE(t, client, topic, 0, 1, "", 2, 10*time.Second)
	if len(first) != 2 {
		t.Fatalf("first stream got %d events, want 2", len(first))
	}
	if first[0].Offset != 1 || first[0].Value != "msg-1" {
		t.Fatalf("first event = offset %d value %q, want offset 1 value %q", first[0].Offset, first[0].Value, "msg-1")
	}
	if first[1].Offset != 2 || first[1].Value != "msg-2" {
		t.Fatalf("second event = offset %d value %q, want offset 2 value %q", first[1].Offset, first[1].Value, "msg-2")
	}

	resumed := streamDisklessSSE(t, client, topic, 0, 0, "2", 1, 10*time.Second)
	if len(resumed) != 1 {
		t.Fatalf("resumed stream got %d events, want 1", len(resumed))
	}
	if resumed[0].Offset != 3 || resumed[0].Value != "msg-3" {
		t.Fatalf("resumed event = offset %d value %q, want offset 3 value %q", resumed[0].Offset, resumed[0].Value, "msg-3")
	}
}

func TestDiskless_ConsumeEmptyTopic(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-empty-topic"
	createDisklessTopic(t, client, topic, 1)

	resp, err := client.Consume(topic, 0, 0, 10)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(resp.Messages) != 0 {
		t.Fatalf("got %d messages, want 0", len(resp.Messages))
	}
	if resp.NextOffset != 0 {
		t.Fatalf("NextOffset = %d, want 0", resp.NextOffset)
	}
}

func TestDiskless_ConsumeBeyondEndReturnsRequestedOffset(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-beyond-end"
	createDisklessTopic(t, client, topic, 1)
	waitForPartitionProduceReady(t, client, topic, 0)

	_, err := client.ProduceToPartition(topic, 0, []camutest.ProduceMessage{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
	})
	if err != nil {
		t.Fatalf("ProduceToPartition: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	resp, err := client.Consume(topic, 0, 10, 10)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(resp.Messages) != 0 {
		t.Fatalf("got %d messages, want 0", len(resp.Messages))
	}
	if resp.NextOffset != 10 {
		t.Fatalf("NextOffset = %d, want 10", resp.NextOffset)
	}
}

func TestDiskless_SSEStreaming(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()

	client := env.Client()
	const topic = "diskless-sse-basic"
	createDisklessTopic(t, client, topic, 1)
	waitForPartitionProduceReady(t, client, topic, 0)

	for i := 0; i < 2; i++ {
		_, err := client.ProduceToPartition(topic, 0, []camutest.ProduceMessage{
			{Key: fmt.Sprintf("k%d", i+1), Value: fmt.Sprintf("msg-%d", i+1)},
		})
		if err != nil {
			t.Fatalf("produce message %d: %v", i+1, err)
		}
		time.Sleep(700 * time.Millisecond)
	}

	events := streamDisklessSSE(t, client, topic, 0, 1, "", 2, 10*time.Second)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Offset != 1 || events[0].Value != "msg-1" {
		t.Fatalf("first event = offset %d value %q, want offset 1 value %q", events[0].Offset, events[0].Value, "msg-1")
	}
	if events[1].Offset != 2 || events[1].Value != "msg-2" {
		t.Fatalf("second event = offset %d value %q, want offset 2 value %q", events[1].Offset, events[1].Value, "msg-2")
	}
}
