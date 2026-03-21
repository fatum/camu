package camutest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
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
