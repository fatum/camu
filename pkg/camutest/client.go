package camutest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// TopicInfo holds topic metadata returned by the API.
type TopicInfo struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Retention  string `json:"retention"`
}

// ClusterStatusResponse holds the cluster status response.
type ClusterStatusResponse struct {
	Instances []InstanceInfo `json:"instances"`
}

// InstanceInfo holds info about a single server instance.
type InstanceInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// Client is an HTTP test client for the camu API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new test client pointed at the given base URL.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

type apiError struct {
	Error string `json:"error"`
}

// CreateTopic creates a topic via the API.
func (c *Client) CreateTopic(name string, partitions int, retention time.Duration) error {
	body, _ := json.Marshal(map[string]any{
		"name":       name,
		"partitions": partitions,
		"retention":  retention.String(),
	})
	resp, err := c.httpClient.Post(c.baseURL+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("CreateTopic request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return fmt.Errorf("CreateTopic: status %d: %s", resp.StatusCode, ae.Error)
	}
	return nil
}

// GetTopic retrieves a topic by name.
func (c *Client) GetTopic(name string) (*TopicInfo, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/v1/topics/" + name)
	if err != nil {
		return nil, fmt.Errorf("GetTopic request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("topic %q not found", name)
	}
	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("GetTopic: status %d: %s", resp.StatusCode, ae.Error)
	}

	var info TopicInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("GetTopic decode: %w", err)
	}
	return &info, nil
}

// ListTopics lists all topics.
func (c *Client) ListTopics() ([]TopicInfo, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/v1/topics")
	if err != nil {
		return nil, fmt.Errorf("ListTopics request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("ListTopics: status %d: %s", resp.StatusCode, ae.Error)
	}

	var topics []TopicInfo
	if err := json.NewDecoder(resp.Body).Decode(&topics); err != nil {
		return nil, fmt.Errorf("ListTopics decode: %w", err)
	}
	return topics, nil
}

// DeleteTopic deletes a topic by name.
func (c *Client) DeleteTopic(name string) error {
	req, err := http.NewRequest(http.MethodDelete, c.baseURL+"/v1/topics/"+name, nil)
	if err != nil {
		return fmt.Errorf("DeleteTopic request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("DeleteTopic: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("topic %q not found", name)
	}
	if resp.StatusCode != http.StatusNoContent {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return fmt.Errorf("DeleteTopic: status %d: %s", resp.StatusCode, ae.Error)
	}
	return nil
}

// ProduceMessage is a message to produce.
type ProduceMessage struct {
	Key     string            `json:"key,omitempty"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

// ProduceResponse holds the response from a produce request.
type ProduceResponse struct {
	Offsets []OffsetInfo `json:"offsets"`
}

// OffsetInfo holds partition and offset for a produced message.
type OffsetInfo struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// Produce sends messages to a topic.
func (c *Client) Produce(topic string, msgs []ProduceMessage) (*ProduceResponse, error) {
	body, err := json.Marshal(msgs)
	if err != nil {
		return nil, fmt.Errorf("Produce marshal: %w", err)
	}
	resp, err := c.httpClient.Post(c.baseURL+"/v1/topics/"+topic+"/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("Produce request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("Produce: status %d: %s", resp.StatusCode, ae.Error)
	}

	var pr ProduceResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("Produce decode: %w", err)
	}
	return &pr, nil
}

// ConsumeResponse holds the response from a consume request.
type ConsumeResponse struct {
	Messages   []ConsumedMessage `json:"messages"`
	NextOffset uint64            `json:"next_offset"`
}

// ConsumedMessage holds a single consumed message.
type ConsumedMessage struct {
	Offset    uint64            `json:"offset"`
	Timestamp int64             `json:"timestamp"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// Consume reads messages from a topic partition starting at the given offset.
func (c *Client) Consume(topic string, partition int, offset uint64, limit int) (*ConsumeResponse, error) {
	url := fmt.Sprintf("%s/v1/topics/%s/partitions/%d/messages?offset=%d&limit=%d",
		c.baseURL, topic, partition, offset, limit)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("Consume request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("Consume: status %d: %s", resp.StatusCode, ae.Error)
	}

	var cr ConsumeResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, fmt.Errorf("Consume decode: %w", err)
	}
	return &cr, nil
}

// StreamSSE opens an SSE connection and reads up to maxEvents events or until timeout.
func (c *Client) StreamSSE(topic string, partition int, offset uint64, maxEvents int, timeout time.Duration) ([]ConsumedMessage, error) {
	url := fmt.Sprintf("%s/v1/topics/%s/partitions/%d/stream?offset=%d",
		c.baseURL, topic, partition, offset)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("StreamSSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	// Use a client without a global timeout so we can read the stream.
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("StreamSSE do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("StreamSSE: status %d: %s", resp.StatusCode, ae.Error)
	}

	var events []ConsumedMessage
	scanner := bufio.NewScanner(resp.Body)

	// Deadline for the whole read loop.
	deadline := time.Now().Add(timeout)

	var currentID string
	var currentData string

	for time.Now().Before(deadline) && len(events) < maxEvents {
		// Use a channel to drive the scanner with a timeout.
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
				return events, nil
			}
			line = res.text
		case <-time.After(time.Until(deadline)):
			return events, nil
		}

		switch {
		case strings.HasPrefix(line, "id: "):
			currentID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			currentData = strings.TrimPrefix(line, "data: ")
		case line == "":
			// Blank line = end of event.
			if currentData != "" {
				var msg ConsumedMessage
				if err := json.Unmarshal([]byte(currentData), &msg); err == nil {
					// Override offset from id field if present.
					if currentID != "" {
						var id uint64
						fmt.Sscan(currentID, &id)
						msg.Offset = id
					}
					events = append(events, msg)
				}
			}
			currentID = ""
			currentData = ""
		}
	}

	return events, nil
}

// CommitOffsets commits offsets for a consumer group.
func (c *Client) CommitOffsets(groupID string, offsets map[int]uint64) error {
	strOffsets := make(map[string]uint64, len(offsets))
	for k, v := range offsets {
		strOffsets[fmt.Sprintf("%d", k)] = v
	}
	body, _ := json.Marshal(map[string]any{"offsets": strOffsets})
	resp, err := c.httpClient.Post(c.baseURL+"/v1/groups/"+groupID+"/commit", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("CommitOffsets request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return fmt.Errorf("CommitOffsets: status %d: %s", resp.StatusCode, ae.Error)
	}
	return nil
}

// GetOffsets retrieves committed offsets for a consumer group.
func (c *Client) GetOffsets(groupID string) (map[int]uint64, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/v1/groups/" + groupID + "/offsets")
	if err != nil {
		return nil, fmt.Errorf("GetOffsets request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("GetOffsets: status %d: %s", resp.StatusCode, ae.Error)
	}

	var result struct {
		Offsets map[string]uint64 `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("GetOffsets decode: %w", err)
	}

	offsets := make(map[int]uint64, len(result.Offsets))
	for k, v := range result.Offsets {
		var pid int
		fmt.Sscanf(k, "%d", &pid)
		offsets[pid] = v
	}
	return offsets, nil
}

// CommitConsumerOffsets commits offsets for a standalone consumer.
func (c *Client) CommitConsumerOffsets(topic, consumerID string, offsets map[int]uint64) error {
	strOffsets := make(map[string]uint64, len(offsets))
	for k, v := range offsets {
		strOffsets[fmt.Sprintf("%d", k)] = v
	}
	body, _ := json.Marshal(map[string]any{"offsets": strOffsets})
	resp, err := c.httpClient.Post(
		fmt.Sprintf("%s/v1/topics/%s/offsets/%s", c.baseURL, topic, consumerID),
		"application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("CommitConsumerOffsets request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return fmt.Errorf("CommitConsumerOffsets: status %d: %s", resp.StatusCode, ae.Error)
	}
	return nil
}

// GetConsumerOffsets retrieves committed offsets for a standalone consumer.
func (c *Client) GetConsumerOffsets(topic, consumerID string) (map[int]uint64, error) {
	resp, err := c.httpClient.Get(
		fmt.Sprintf("%s/v1/topics/%s/offsets/%s", c.baseURL, topic, consumerID))
	if err != nil {
		return nil, fmt.Errorf("GetConsumerOffsets request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("GetConsumerOffsets: status %d: %s", resp.StatusCode, ae.Error)
	}

	var result struct {
		Offsets map[string]uint64 `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("GetConsumerOffsets decode: %w", err)
	}

	offsets := make(map[int]uint64, len(result.Offsets))
	for k, v := range result.Offsets {
		var pid int
		fmt.Sscanf(k, "%d", &pid)
		offsets[pid] = v
	}
	return offsets, nil
}

// RoutingResponse holds the routing response for a topic.
type RoutingResponse struct {
	Partitions map[string]RoutingPartitionInfo `json:"partitions"`
}

// RoutingPartitionInfo holds routing info for a single partition.
type RoutingPartitionInfo struct {
	InstanceID string `json:"instance_id"`
	Address    string `json:"address"`
}

// GetRouting returns the partition routing map for a topic.
func (c *Client) GetRouting(topic string) (*RoutingResponse, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/v1/topics/" + topic + "/routing")
	if err != nil {
		return nil, fmt.Errorf("GetRouting request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("topic %q not found", topic)
	}
	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("GetRouting: status %d: %s", resp.StatusCode, ae.Error)
	}

	var routing RoutingResponse
	if err := json.NewDecoder(resp.Body).Decode(&routing); err != nil {
		return nil, fmt.Errorf("GetRouting decode: %w", err)
	}
	return &routing, nil
}

// ClusterStatus returns the cluster status.
func (c *Client) ClusterStatus() (*ClusterStatusResponse, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/v1/cluster/status")
	if err != nil {
		return nil, fmt.Errorf("ClusterStatus request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return nil, fmt.Errorf("ClusterStatus: status %d: %s", resp.StatusCode, ae.Error)
	}

	var status ClusterStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("ClusterStatus decode: %w", err)
	}
	return &status, nil
}
