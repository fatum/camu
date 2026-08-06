// Command typed-topic-benchmark exercises the typed HTTP topic, consume, and
// Parquet/Iceberg paths without retaining the generated dataset.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	BaseURL, Topic, Output, API, Operation           string
	NodeURLs                                         []string
	KafkaBrokers                                     []string
	TargetBytes, MessageBytes                        int64
	Partitions, ReplicationFactor, MinInSyncReplicas int
	BatchMessages, ProducerConcurrency               int
	ExportEnabled                                    bool
	ConsumeTimeout                                   time.Duration
	RequestTimeout                                   time.Duration
	SequenceStart                                    int64
	RunID                                            string
	StorageMode                                      string
}

type result struct {
	Operation     string                 `json:"operation"`
	Topic         string                 `json:"topic"`
	ExportEnabled bool                   `json:"export_enabled"`
	Expected      int64                  `json:"expected_records,omitempty"`
	Produced      int64                  `json:"produced_records,omitempty"`
	Consumed      int64                  `json:"consumed_records,omitempty"`
	ExpectedBytes int64                  `json:"expected_bytes,omitempty"`
	ConsumedBytes int64                  `json:"consumed_bytes,omitempty"`
	Producer      phaseResult            `json:"producer"`
	Consumer      phaseResult            `json:"consumer"`
	Throughput    throughputResult       `json:"throughput"`
	Integrity     integrityResult        `json:"integrity"`
	Cleanup       bool                   `json:"cleanup"`
	Cluster       clusterReadinessResult `json:"cluster_readiness"`
}
type clusterStatus struct {
	Ready                 bool     `json:"ready"`
	Status                string   `json:"status"`
	ActiveInstances       int      `json:"active_instances"`
	ReadyInstances        int      `json:"ready_instances"`
	AssignedPartitions    int      `json:"assigned_partitions"`
	InitializedPartitions int      `json:"initialized_partitions"`
	ExpectedPartitions    int      `json:"expected_partitions"`
	Reasons               []string `json:"reasons"`
}
type clusterStatusSample struct {
	At     time.Time     `json:"at"`
	Status clusterStatus `json:"status"`
	Error  string        `json:"error,omitempty"`
}
type clusterReadinessResult struct {
	Samples []clusterStatusSample `json:"samples"`
	Final   clusterStatus         `json:"final"`
	Lost    bool                  `json:"lost"`
}
type phaseResult struct {
	Records          int64   `json:"records"`
	Bytes            int64   `json:"bytes"`
	SerializedBytes  int64   `json:"serialized_bytes"`
	DurationSeconds  float64 `json:"duration_seconds"`
	RecordsPerSecond float64 `json:"records_per_second"`
	BytesPerSecond   float64 `json:"bytes_per_second"`
	Digest           string  `json:"digest,omitempty"`
}
type throughputResult struct {
	WriteBytesPerSecond float64 `json:"write_bytes_per_second"`
	ReadBytesPerSecond  float64 `json:"read_bytes_per_second"`
}
type integrityResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}
type typedValue struct {
	RunID        string `json:"run_id"`
	ID           int64  `json:"id"`
	Payload      string `json:"payload"`
	PayloadBytes int64  `json:"payload_bytes"`
	Sequence     int64  `json:"sequence"`
}

type idempotentProduceRequest struct {
	ProducerID uint64           `json:"producer_id"`
	Sequence   uint64           `json:"sequence"`
	Messages   []map[string]any `json:"messages"`
}

type initBenchmarkProducerResponse struct {
	ProducerID uint64 `json:"producer_id"`
}
type message struct {
	Offset uint64 `json:"offset"`
	Value  string `json:"value"`
}
type consumeResponse struct {
	Messages   []message `json:"messages"`
	NextOffset uint64    `json:"next_offset"`
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func benchmarkLog(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[benchmark] %s %s\n", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf(format, args...))
}
func parsePositiveInt(key string, def int64) (int64, error) {
	raw := strings.TrimSpace(env(key, strconv.FormatInt(def, 10)))
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", key, raw)
	}
	return v, nil
}

func parseByteSize(key string, def int64) (int64, error) {
	raw := strings.TrimSpace(env(key, strconv.FormatInt(def, 10)))
	lower := strings.ToLower(raw)
	multiplier := int64(1)
	for _, unit := range []struct {
		suffix     string
		multiplier int64
	}{
		{"kib", 1 << 10},
		{"mib", 1 << 20},
		{"gib", 1 << 30},
		{"tib", 1 << 40},
	} {
		if strings.HasSuffix(lower, unit.suffix) {
			lower = strings.TrimSpace(strings.TrimSuffix(lower, unit.suffix))
			multiplier = unit.multiplier
			break
		}
	}
	v, err := strconv.ParseInt(lower, 10, 64)
	if err != nil || v <= 0 || v > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("%s must be a positive byte count (for example 1073741824 or 1GiB), got %q", key, raw)
	}
	return v * multiplier, nil
}

func parseIntOption(key string, def int64) (int, error) {
	v, err := parsePositiveInt(key, def)
	if err != nil {
		return 0, err
	}
	maxInt := int64(^uint(0) >> 1)
	if v > maxInt {
		return 0, fmt.Errorf("%s exceeds int range", key)
	}
	return int(v), nil
}

func loadConfig() (config, error) {
	d, err := time.ParseDuration(env("QUERY_INTERVAL", "5s"))
	if err != nil || d <= 0 {
		return config{}, fmt.Errorf("QUERY_INTERVAL must be a positive duration")
	}
	topic := env("TOPIC", "benchmark-typed")
	if topic == "" || strings.IndexByte(topic, 0) >= 0 || strings.ContainsAny(topic, " \t\r\n\"'") {
		return config{}, fmt.Errorf("TOPIC contains unsafe characters")
	}
	consumeTimeout, err := time.ParseDuration(env("CONSUME_TIMEOUT", "10m"))
	if err != nil || consumeTimeout <= 0 {
		return config{}, fmt.Errorf("CONSUME_TIMEOUT must be a positive duration")
	}
	requestTimeout, err := time.ParseDuration(env("REQUEST_TIMEOUT", "30s"))
	if err != nil || requestTimeout <= 0 {
		return config{}, fmt.Errorf("REQUEST_TIMEOUT must be a positive duration")
	}
	nodeURLs := []string{}
	if raw := env("NODE_URLS", ""); raw != "" {
		for _, nodeURL := range strings.Split(raw, ",") {
			if nodeURL = strings.TrimSpace(nodeURL); nodeURL != "" {
				nodeURLs = append(nodeURLs, strings.TrimRight(nodeURL, "/"))
			}
		}
	}
	api := strings.ToLower(env("BENCHMARK_API", "http"))
	if api != "http" && api != "kafka" {
		return config{}, fmt.Errorf("BENCHMARK_API must be http or kafka")
	}
	kafkaBrokers := []string{}
	for _, broker := range strings.Split(env("KAFKA_BROKERS", ""), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			kafkaBrokers = append(kafkaBrokers, broker)
		}
	}
	if api == "kafka" && len(kafkaBrokers) == 0 {
		return config{}, fmt.Errorf("KAFKA_BROKERS is required when BENCHMARK_API=kafka")
	}
	operation := strings.ToLower(env("BENCHMARK_OPERATION", "all"))
	if operation != "all" && operation != "produce" && operation != "consume" {
		return config{}, fmt.Errorf("BENCHMARK_OPERATION must be all, produce, or consume")
	}
	exportEnabled, err := strconv.ParseBool(env("EXPORT_ENABLED", "true"))
	if err != nil {
		return config{}, fmt.Errorf("EXPORT_ENABLED must be true or false")
	}
	targetBytes, err := parseByteSize("TARGET_BYTES", 5*1024*1024*1024)
	if err != nil {
		return config{}, err
	}
	messageBytes, err := parseByteSize("MESSAGE_BYTES", 1024)
	if err != nil {
		return config{}, err
	}
	partitions, err := parseIntOption("PARTITIONS", 12)
	if err != nil {
		return config{}, err
	}
	replicationFactor, err := parseIntOption("REPLICATION_FACTOR", 1)
	if err != nil {
		return config{}, err
	}
	minInSyncReplicas, err := parseIntOption("MIN_IN_SYNC_REPLICAS", 1)
	if err != nil {
		return config{}, err
	}
	batchMessages, err := parseIntOption("BATCH_MESSAGES", 500)
	if err != nil {
		return config{}, err
	}
	producerConcurrency, err := parseIntOption("PRODUCER_CONCURRENCY", 4)
	if err != nil {
		return config{}, err
	}
	runID := env("BENCHMARK_RUN_ID", "")
	if runID == "" {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return config{}, fmt.Errorf("generate benchmark run ID: %w", err)
		}
		runID = hex.EncodeToString(token[:])
	}
	storageMode := strings.ToLower(env("STORAGE_MODE", ""))
	if storageMode != "" && storageMode != "classic" && storageMode != "diskless" {
		return config{}, fmt.Errorf("STORAGE_MODE must be classic or diskless")
	}
	return config{BaseURL: strings.TrimRight(env("CAMU_URL", "http://127.0.0.1:8080"), "/"), Topic: topic, Output: env("OUTPUT", "typed-topic-benchmark.json"), API: api, Operation: operation, NodeURLs: nodeURLs, KafkaBrokers: kafkaBrokers, TargetBytes: targetBytes, MessageBytes: messageBytes, Partitions: partitions, ReplicationFactor: replicationFactor, MinInSyncReplicas: minInSyncReplicas, BatchMessages: batchMessages, ProducerConcurrency: producerConcurrency, ExportEnabled: exportEnabled, ConsumeTimeout: consumeTimeout, RequestTimeout: requestTimeout, RunID: runID, StorageMode: storageMode}, nil
}

type client struct {
	base           string
	http           *http.Client
	token          string
	requestTimeout time.Duration
}

// nodeRoundRobin rotates which node each benchmark request targets so produce
// and topic-setup traffic is spread round-robin across every node instead of
// pinning a partition to one node or depending on a single CAMU_URL endpoint.
var nodeRoundRobin atomic.Uint64

// nodeClient returns a copy of c pointed at the next node from cfg.NodeURLs.
func (c client) nodeClient(cfg config) client {
	if len(cfg.NodeURLs) == 0 {
		return c
	}
	c.base = cfg.NodeURLs[int(nodeRoundRobin.Add(1)-1)%len(cfg.NodeURLs)]
	return c
}

func (c client) request(ctx context.Context, method, path string, body any, out any) error {
	_, err := c.requestHeaders(ctx, method, path, body, out)
	return err
}

func (c client) requestHeaders(ctx context.Context, method, path string, body any, out any) (http.Header, error) {
	timeout := c.requestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return nil, fmt.Errorf("%s %s: read body: %w", method, path, err)
		}
		return nil, fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return nil, err
		}
	}
	return resp.Header, nil
}

func (c client) clusterStatus(ctx context.Context) (clusterStatus, error) {
	var status clusterStatus
	err := c.request(ctx, http.MethodGet, "/v1/cluster/status", nil, &status)
	return status, err
}

// waitClusterReady blocks until the full cluster reports ready. Diskless topics
// are served by any node's engine plus the shared metastore, so they skip the
// cluster-wide wait and can produce/consume as soon as a node is up.
func (c client) waitClusterReady(ctx context.Context, cfg config) error {
	if cfg.StorageMode == "diskless" {
		return nil
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		var status clusterStatus
		err := c.request(ctx, http.MethodGet, "/v1/cluster/ready", nil, &status)
		if err == nil && status.Ready {
			return nil
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("timed out waiting for cluster readiness")
}

func (c client) deleteAndWait(ctx context.Context, topic string) error {
	if err := c.request(ctx, http.MethodDelete, "/v1/topics/"+url.PathEscape(topic), nil, nil); err != nil && !strings.Contains(err.Error(), "404") {
		return fmt.Errorf("delete topic: %w", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		err := c.request(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(topic), nil, nil)
		if err != nil && strings.Contains(err.Error(), "404") {
			return nil
		}
		if err != nil && !strings.Contains(err.Error(), "404") {
			return fmt.Errorf("check topic deletion: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("topic deletion timeout")
}

func (c client) create(ctx context.Context, cfg config) error {
	body := map[string]any{"name": cfg.Topic, "partitions": cfg.Partitions, "replication_factor": cfg.ReplicationFactor, "min_insync_replicas": cfg.MinInSyncReplicas, "retention": "24h", "export_enabled": cfg.ExportEnabled}
	if cfg.StorageMode != "" {
		body["storage_mode"] = cfg.StorageMode
		if cfg.StorageMode == "diskless" {
			// Diskless topics do not replicate; the server ignores these, so
			// request the single-leader default explicitly.
			body["replication_factor"] = 1
			body["min_insync_replicas"] = 1
		}
	}
	body["schema"] = map[string]any{"encoding": "json", "fields": benchmarkSchemaFields(cfg)}
	if err := c.request(ctx, http.MethodPost, "/v1/topics", body, nil); err != nil {
		return err
	}
	return c.waitForReplication(ctx, cfg)
}

func (c client) initBenchmarkProducer(ctx context.Context, cfg config) (uint64, error) {
	nodes := cfg.NodeURLs
	if len(nodes) == 0 {
		nodes = []string{c.base}
	}
	start := int(nodeRoundRobin.Add(1)-1) % len(nodes)
	var lastErr error
	for i := 0; i < len(nodes); i++ {
		nodeClient := c
		nodeClient.base = nodes[(start+i)%len(nodes)]
		var response initBenchmarkProducerResponse
		if err := retryProduce(ctx, "initialize HTTP producer", func() error {
			response = initBenchmarkProducerResponse{}
			return nodeClient.request(ctx, http.MethodPost, "/v1/producers/init", nil, &response)
		}); err != nil {
			lastErr = err
			continue
		}
		if response.ProducerID == 0 {
			lastErr = errors.New("initialize HTTP producer: server returned producer_id 0")
			continue
		}
		return response.ProducerID, nil
	}
	return 0, fmt.Errorf("initialize HTTP producer on all %d nodes: %w", len(nodes), lastErr)
}

type benchmarkTopic struct {
	Partitions    int    `json:"partitions"`
	ExportEnabled bool   `json:"export_enabled"`
	StorageMode   string `json:"storage_mode,omitempty"`
}

// ensureTopic checks whether the topic exists (creating it if absent) by trying
// each node in round-robin order so setup does not depend on a single endpoint.
func (c client) ensureTopic(ctx context.Context, cfg config) (bool, error) {
	nodes := cfg.NodeURLs
	if len(nodes) == 0 {
		nodes = []string{c.base}
	}
	start := int(nodeRoundRobin.Add(1)-1) % len(nodes)
	var lastErr error
	for i := 0; i < len(nodes); i++ {
		nodeClient := c
		nodeClient.base = nodes[(start+i)%len(nodes)]
		existing, err := nodeClient.ensureTopicOnNode(ctx, cfg)
		if err == nil {
			return existing, nil
		}
		benchmarkLog("topic setup via %s failed: %v", nodeClient.base, err)
		lastErr = err
	}
	return false, fmt.Errorf("topic setup failed on all %d nodes: %w", len(nodes), lastErr)
}

func (c client) ensureTopicOnNode(ctx context.Context, cfg config) (bool, error) {
	var topic benchmarkTopic
	err := c.request(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(cfg.Topic), nil, &topic)
	if err != nil {
		if !strings.Contains(err.Error(), "404") {
			return false, err
		}
		if err := c.create(ctx, cfg); err != nil {
			return false, err
		}
		return false, nil
	}
	if topic.Partitions != cfg.Partitions {
		return false, fmt.Errorf("existing topic has %d partitions, benchmark requires %d", topic.Partitions, cfg.Partitions)
	}
	if topic.ExportEnabled != cfg.ExportEnabled {
		return false, fmt.Errorf("existing topic export_enabled=%t, benchmark requires %t", topic.ExportEnabled, cfg.ExportEnabled)
	}
	if cfg.StorageMode != "" && topic.StorageMode != "" && topic.StorageMode != cfg.StorageMode {
		return false, fmt.Errorf("existing topic storage_mode=%q, benchmark requires %q", topic.StorageMode, cfg.StorageMode)
	}
	if err := c.waitForReplication(ctx, cfg); err != nil {
		return false, err
	}
	return true, nil
}

func (c client) committedRecordCount(ctx context.Context, cfg config) (int64, error) {
	offsets, err := c.committedPartitionOffsets(ctx, cfg)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, offset := range offsets {
		if total > math.MaxInt64-offset {
			return 0, errors.New("committed record count exceeds int64")
		}
		total += offset
	}
	return total, nil
}

// committedPartitionOffsets snapshots the current readable end offset for each
// partition. Consumers use this fixed boundary so concurrent appends are not
// mistaken for records belonging to the verification run.
func (c client) committedPartitionOffsets(ctx context.Context, cfg config) ([]int64, error) {
	offsets := make([]int64, cfg.Partitions)
	for partition := 0; partition < cfg.Partitions; partition++ {
		partitionClient := c
		if len(cfg.NodeURLs) > 0 {
			partitionClient.base = cfg.NodeURLs[partition%len(cfg.NodeURLs)]
		}
		var page consumeResponse
		headers, err := partitionClient.requestHeaders(ctx, http.MethodGet, fmt.Sprintf("/v1/topics/%s/partitions/%d/messages?offset=0&limit=1", url.PathEscape(cfg.Topic), partition), nil, &page)
		if err != nil {
			return nil, fmt.Errorf("read partition %d high watermark: %w", partition, err)
		}
		hw, err := strconv.ParseUint(headers.Get("X-High-Watermark"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("read partition %d high watermark: missing or invalid response header", partition)
		}
		if hw > math.MaxInt64 {
			return nil, fmt.Errorf("read partition %d high watermark exceeds int64", partition)
		}
		offsets[partition] = int64(hw)
	}
	return offsets, nil
}

func (c client) waitForReplication(ctx context.Context, cfg config) error {
	if cfg.StorageMode == "diskless" {
		// Diskless topics do not replicate; there are no replica assignments to
		// wait for.
		return nil
	}
	type routingPartition struct {
		Replicas []any `json:"replicas"`
	}
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var resp struct {
			Partitions map[string]routingPartition `json:"partitions"`
		}
		if err := c.request(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(cfg.Topic)+"/routing", nil, &resp); err == nil {
			ready := len(resp.Partitions) == cfg.Partitions
			for partition := range resp.Partitions {
				if len(resp.Partitions[partition].Replicas) < cfg.ReplicationFactor {
					ready = false
					break
				}
			}
			if ready {
				for partition := 0; partition < cfg.Partitions; partition++ {
					partitionClient := c
					if len(cfg.NodeURLs) > 0 {
						partitionClient.base = cfg.NodeURLs[partition%len(cfg.NodeURLs)]
					}
					var messages consumeResponse
					probePath := fmt.Sprintf("/v1/topics/%s/partitions/%d/messages?offset=0&limit=1", url.PathEscape(cfg.Topic), partition)
					if err := partitionClient.request(ctx, http.MethodGet, probePath, nil, &messages); err != nil {
						ready = false
						break
					}
				}
				if ready {
					return nil
				}
			}
		} else {
			lastErr = err
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if lastErr != nil {
		return fmt.Errorf("wait for replicated topic: %w", lastErr)
	}
	return errors.New("timed out waiting for replicated topic assignments")
}

func payload(n int64) string { return strings.Repeat("x", int(n)) }
func targetCount(target, messageBytes int64) (int64, error) {
	if target <= 0 || messageBytes <= 0 || target > math.MaxInt64-(messageBytes-1) {
		return 0, errors.New("TARGET_BYTES/MESSAGE_BYTES overflow")
	}
	count := (target + messageBytes - 1) / messageBytes
	if count > math.MaxInt64/messageBytes {
		return 0, errors.New("target byte total overflow")
	}
	return count, nil
}

func expectedStatesFor(cfg config, count int64) []hashState {
	expected := make([]hashState, cfg.Partitions)
	payloadText := payload(cfg.MessageBytes)
	for i := int64(0); i < count; i++ {
		expected[int(i%int64(cfg.Partitions))].add(typedValue{RunID: cfg.RunID, ID: i, Payload: payloadText, PayloadBytes: cfg.MessageBytes, Sequence: i})
	}
	return expected
}

func expectedStatesForPartitionOffsets(cfg config, endOffsets []int64) ([]hashState, error) {
	if len(endOffsets) != cfg.Partitions {
		return nil, fmt.Errorf("partition end offsets = %d, want %d", len(endOffsets), cfg.Partitions)
	}
	expected := make([]hashState, cfg.Partitions)
	for partition, endOffset := range endOffsets {
		if endOffset < 0 {
			return nil, fmt.Errorf("partition %d end offset is negative: %d", partition, endOffset)
		}
		expected[partition].records = endOffset
		expected[partition].bytes = endOffset * cfg.MessageBytes
	}
	return expected, nil
}

func (c client) produce(ctx context.Context, cfg config, count int64, expected []hashState, progress func(int64)) (phaseResult, error) {
	start := time.Now()
	producerID, err := c.initBenchmarkProducer(ctx, cfg)
	if err != nil {
		return phaseResult{}, err
	}
	var total int64
	var serialized int64
	payloadText := payload(cfg.MessageBytes)
	var wg sync.WaitGroup
	errs := make(chan error, cfg.Partitions)
	jobs := make(chan int)
	workers := cfg.ProducerConcurrency
	if workers > cfg.Partitions {
		workers = cfg.Partitions
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				sequence := uint64(0)
				for first := firstSequenceForPartition(cfg.SequenceStart, p, cfg.Partitions); first < cfg.SequenceStart+count; first += int64(cfg.Partitions * cfg.BatchMessages) {
					batch := make([]map[string]any, 0, cfg.BatchMessages)
					for i := first; i < cfg.SequenceStart+count && len(batch) < cfg.BatchMessages; i += int64(cfg.Partitions) {
						v := typedValue{RunID: cfg.RunID, ID: i, Payload: payloadText, PayloadBytes: cfg.MessageBytes, Sequence: i}
						batch = append(batch, map[string]any{"key": cfg.RunID + ":" + strconv.FormatInt(i, 10), "value": string(mustJSON(benchmarkEvent(cfg, v)))})
						expected[p].add(v)
					}
					atomic.AddInt64(&serialized, int64(len(mustJSON(batch))))
					path := fmt.Sprintf("/v1/topics/%s/partitions/%d/messages", url.PathEscape(cfg.Topic), p)
					partitionClient := c.nodeClient(cfg)
					request := idempotentProduceRequest{ProducerID: producerID, Sequence: sequence, Messages: batch}
					if err := retryProduce(ctx, fmt.Sprintf("produce HTTP partition %d sequence %d", p, sequence), func() error {
						return partitionClient.request(ctx, http.MethodPost, path, request, nil)
					}); err != nil {
						errs <- err
						return
					}
					sequence += uint64(len(batch))
					atomic.AddInt64(&total, int64(len(batch)))
					progress(int64(len(batch)))
				}
			}
		}()
	}
	for p := 0; p < cfg.Partitions; p++ {
		jobs <- p
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return phaseResult{}, err
		}
	}
	d := time.Since(start)
	return phaseResult{Records: total, Bytes: total * cfg.MessageBytes, SerializedBytes: serialized, DurationSeconds: d.Seconds(), RecordsPerSecond: float64(total) / d.Seconds(), BytesPerSecond: float64(total*cfg.MessageBytes) / d.Seconds()}, nil
}

const (
	produceRetryInitialBackoff = 250 * time.Millisecond
	produceRetryMaxBackoff     = 5 * time.Second
)

// retryProduce retains a batch in the caller until a transient node failure
// clears. The request payload and idempotency sequence remain unchanged across
// retries, so an accepted request whose response was lost cannot be appended
// twice.
func retryProduce(ctx context.Context, operation string, attempt func() error) error {
	backoff := produceRetryInitialBackoff
	for {
		err := attempt()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isRetryableProduceError(err) {
			return fmt.Errorf("%s: %w", operation, err)
		}
		benchmarkLog("%s failed; retaining batch for retry in %s: %v", operation, backoff, err)
		select {
		case <-time.After(backoff):
			if backoff < produceRetryMaxBackoff {
				backoff *= 2
				if backoff > produceRetryMaxBackoff {
					backoff = produceRetryMaxBackoff
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func isRetryableProduceError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		" 421 ", " 429 ", " 500 ", " 502 ", " 503 ", " 504 ",
		"connection refused", "connection reset", "broken pipe", "eof", "timeout",
		"not leader", "leader not available", "partition not ready", "not initialized",
		"network", "unknown producer", "not_leader", "leader_not_available",
		"broker_not_available", "unknown_topic_or_partition",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func firstSequenceForPartition(start int64, partition, partitions int) int64 {
	return start + int64((partition-int(start%int64(partitions))+partitions)%partitions)
}
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

type hashState struct {
	mu             sync.Mutex
	h              hash.Hash
	records, bytes int64
}

func (s *hashState) add(v typedValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.h == nil {
		s.h = sha256.New()
	}
	b := mustJSON(v)
	_, _ = s.h.Write(b)
	s.records++
	s.bytes += v.PayloadBytes
}
func (s *hashState) result() (int64, int64, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.h == nil {
		empty := sha256.Sum256(nil)
		return 0, 0, hex.EncodeToString(empty[:]), nil
	}
	return s.records, s.bytes, hex.EncodeToString(s.h.Sum(nil)), nil
}

// runSequenceValidator allows concurrent producer runs to interleave in a
// partition while ensuring each run remains gapless and ordered.
type runSequenceValidator struct{ next map[string]int64 }

func (v *runSequenceValidator) validate(cfg config, partition int, value typedValue) error {
	if value.ID != value.Sequence {
		return fmt.Errorf("record ID %d does not match sequence %d", value.ID, value.Sequence)
	}
	if value.PayloadBytes != cfg.MessageBytes || value.Payload != payload(cfg.MessageBytes) {
		return fmt.Errorf("record sequence %d has an invalid payload", value.Sequence)
	}
	if value.RunID == "" {
		// Legacy benchmark records did not identify their producer run. Their
		// Kafka offsets and record contents remain valid, but concurrent runs
		// cannot be distinguished well enough for per-run sequence checks.
		return nil
	}
	if v.next == nil {
		v.next = make(map[string]int64)
	}
	expected, ok := v.next[value.RunID]
	if !ok {
		expected = firstSequenceForPartition(0, partition, cfg.Partitions)
	}
	if value.Sequence != expected {
		return fmt.Errorf("run %q partition %d sequence gap or reordering: got %d, want %d", value.RunID, partition, value.Sequence, expected)
	}
	v.next[value.RunID] = expected + int64(cfg.Partitions)
	return nil
}

func (c client) consume(ctx context.Context, cfg config, expected []hashState, actual []hashState, count int64, progress func(int64)) (phaseResult, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.ConsumeTimeout)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, cfg.Partitions)
	validators := make([]runSequenceValidator, cfg.Partitions)
	for p := 0; p < cfg.Partitions; p++ {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			var off uint64
			partitionClient := c
			if len(cfg.NodeURLs) > 0 {
				partitionClient.base = cfg.NodeURLs[p%len(cfg.NodeURLs)]
			}
			benchmarkLog("consume partition=%d endpoint=%s expected_records=%d start_offset=0", p, partitionClient.base, expected[p].recordsSnapshot())
			for {
				var resp consumeResponse
				started := time.Now()
				err := partitionClient.request(ctx, http.MethodGet, fmt.Sprintf("/v1/topics/%s/partitions/%d/messages?offset=%d&limit=1000", url.PathEscape(cfg.Topic), p, off), nil, &resp)
				if err != nil {
					benchmarkLog("consume partition=%d endpoint=%s next_offset=%d failed after=%s error=%v", p, partitionClient.base, off, time.Since(started), err)
					errs <- err
					return
				}
				if len(resp.Messages) == 0 {
					if actual[p].recordsSnapshot() >= expected[p].recordsSnapshot() {
						benchmarkLog("consume partition=%d complete records=%d next_offset=%d", p, actual[p].recordsSnapshot(), off)
						return
					}
					benchmarkLog("consume partition=%d endpoint=%s next_offset=%d empty records=%d expected=%d duration=%s", p, partitionClient.base, off, actual[p].recordsSnapshot(), expected[p].recordsSnapshot(), time.Since(started))
					time.Sleep(100 * time.Millisecond)
					continue
				}
				ignoredLiveSuffix := false
				for _, m := range resp.Messages {
					if actual[p].recordsSnapshot() >= expected[p].recordsSnapshot() {
						// The snapshot boundary may fall inside an HTTP page while
						// producers continue appending. Ignore the newer suffix.
						ignoredLiveSuffix = true
						break
					}
					var v typedValue
					if err := json.Unmarshal([]byte(m.Value), &v); err != nil {
						errs <- err
						return
					}
					if m.Offset > math.MaxInt64 {
						errs <- fmt.Errorf("consume HTTP: partition %d offset %d exceeds int64", p, m.Offset)
						return
					}
					if err := validateKafkaRecord(cfg, p, actual[p].recordsSnapshot(), int64(m.Offset), v); err != nil {
						errs <- fmt.Errorf("%s", strings.Replace(err.Error(), "consume Kafka:", "consume HTTP:", 1))
						return
					}
					if err := validators[p].validate(cfg, p, v); err != nil {
						errs <- fmt.Errorf("consume HTTP: %w", err)
						return
					}
					actual[p].add(v)
					progress(1)
				}
				if ignoredLiveSuffix {
					benchmarkLog("consume partition=%d complete records=%d next_offset=%d", p, actual[p].recordsSnapshot(), off)
					return
				}
				if resp.NextOffset != uint64(actual[p].recordsSnapshot()) {
					errs <- fmt.Errorf("consume HTTP: partition %d next offset gap or reordering: got %d, want %d", p, resp.NextOffset, actual[p].recordsSnapshot())
					return
				}
				off = resp.NextOffset
				benchmarkLog("consume partition=%d endpoint=%s records=%d next_offset=%d total_records=%d duration=%s", p, partitionClient.base, len(resp.Messages), off, actual[p].recordsSnapshot(), time.Since(started))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return phaseResult{}, e
		}
	}
	var records, bytesN int64
	h := sha256.New()
	for p := range actual {
		r, b, d, e := actual[p].result()
		if e != nil {
			return phaseResult{}, e
		}
		records += r
		bytesN += b
		db, err := hex.DecodeString(d)
		if err != nil {
			return phaseResult{}, err
		}
		h.Write(db)
	}
	d := time.Since(start)
	return phaseResult{Records: records, Bytes: bytesN, DurationSeconds: d.Seconds(), RecordsPerSecond: float64(records) / d.Seconds(), BytesPerSecond: float64(bytesN) / d.Seconds(), Digest: hex.EncodeToString(h.Sum(nil))}, nil
}
func (s *hashState) recordsSnapshot() int64 { s.mu.Lock(); defer s.mu.Unlock(); return s.records }

func verifyConsumeStates(expected, actual []hashState) bool {
	for p := range expected {
		er, eb, ed, ee := expected[p].result()
		ar, ab, ad, ae := actual[p].result()
		if ee != nil || ae != nil || er != ar || eb != ab || ed != ad {
			return false
		}
	}
	return true
}

// detectTopicStorageMode reports the storage mode of an existing topic so a
// consume run against a diskless topic skips the classic cluster readiness
// wait even when STORAGE_MODE is not set. A missing topic returns "".
func (c client) detectTopicStorageMode(ctx context.Context, cfg config) (string, error) {
	var topic benchmarkTopic
	err := c.request(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(cfg.Topic), nil, &topic)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return "", nil
		}
		return "", err
	}
	return topic.StorageMode, nil
}

func runSingleOperation(ctx context.Context, c client, cfg config, res *result) {
	if cfg.StorageMode == "" {
		// An existing diskless topic is served without cluster-wide readiness;
		// detect it so consume runs do not wait on /v1/cluster/ready, which
		// never reports ready for diskless partitions.
		mode, err := c.detectTopicStorageMode(ctx, cfg)
		if err != nil {
			res.Integrity.Error = "read topic storage mode: " + err.Error()
			benchmarkLog("read topic storage mode failed: %v", err)
			return
		}
		if mode != "" {
			cfg.StorageMode = mode
		}
	}
	if err := c.waitClusterReady(ctx, cfg); err != nil {
		res.Integrity.Error = "cluster readiness: " + err.Error()
		benchmarkLog("cluster readiness failed: %v", err)
		return
	}
	count, err := targetCount(cfg.TargetBytes, cfg.MessageBytes)
	if err != nil {
		res.Integrity.Error = err.Error()
		return
	}
	res.Expected, res.ExpectedBytes = count, count*cfg.MessageBytes
	expected := expectedStatesFor(cfg, count)
	produce := c.produce
	consume := c.consume
	if cfg.API == "kafka" {
		produce, consume = produceKafka, consumeKafka
	}
	switch cfg.Operation {
	case "produce":
		benchmarkLog("creating or reusing topic %q", cfg.Topic)
		existing, err := c.ensureTopic(ctx, cfg)
		if err != nil {
			res.Integrity.Error = "ensure topic: " + err.Error()
			benchmarkLog("topic setup failed: %v", err)
			return
		}
		if existing {
			benchmarkLog("appending to topic %q with run_id=%s", cfg.Topic, cfg.RunID)
		}
		var produced int64
		pr, err := produce(ctx, cfg, count, expected, func(n int64) {
			if total := atomic.AddInt64(&produced, n); total%(int64(cfg.BatchMessages)*10) == 0 {
				benchmarkLog("produce progress records=%d/%d", total, count)
			}
		})
		if err != nil {
			res.Integrity.Error = "produce: " + err.Error()
			benchmarkLog("produce failed: %v", err)
			return
		}
		res.Producer, res.Produced = pr, pr.Records
		res.Integrity.OK = pr.Records == count
		benchmarkLog("produce complete: records=%d bytes=%d duration=%.3fs rate=%.2f records/s %.2f bytes/s", pr.Records, pr.Bytes, pr.DurationSeconds, pr.RecordsPerSecond, pr.BytesPerSecond)
	case "consume":
		endOffsets, err := c.committedPartitionOffsets(ctx, cfg)
		if err != nil {
			res.Integrity.Error = "read existing topic: " + err.Error()
			benchmarkLog("read existing topic failed: %v", err)
			return
		}
		count = 0
		for _, endOffset := range endOffsets {
			count += endOffset
		}
		res.Expected, res.ExpectedBytes = count, count*cfg.MessageBytes
		expected, err = expectedStatesForPartitionOffsets(cfg, endOffsets)
		if err != nil {
			res.Integrity.Error = "build expected state: " + err.Error()
			benchmarkLog("build expected state failed: %v", err)
			return
		}
		benchmarkLog("verifying existing topic %q records=%d", cfg.Topic, count)
		benchmarkLog("starting consumer verification")
		actual := make([]hashState, cfg.Partitions)
		cr, err := consume(ctx, cfg, expected, actual, count, func(int64) {})
		if err != nil {
			res.Integrity.Error = "consume: " + err.Error()
			benchmarkLog("consume failed: %v", err)
			return
		}
		res.Consumer, res.Consumed, res.ConsumedBytes = cr, cr.Records, cr.Bytes
		res.Throughput.ReadBytesPerSecond = cr.BytesPerSecond
		res.Integrity.OK = cr.Records == count && cr.Bytes == res.ExpectedBytes
		if !res.Integrity.OK {
			res.Integrity.Error = "consume integrity mismatch"
		}
		benchmarkLog("consume complete: records=%d bytes=%d duration=%.3fs rate=%.2f records/s %.2f bytes/s integrity_ok=%t", cr.Records, cr.Bytes, cr.DurationSeconds, cr.RecordsPerSecond, cr.BytesPerSecond, res.Integrity.OK)
	}
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[benchmark] configuration error: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	c := client{base: cfg.BaseURL, http: &http.Client{}, token: os.Getenv("CAMU_AUTH_TOKEN"), requestTimeout: cfg.RequestTimeout}
	res := result{Topic: cfg.Topic, Operation: cfg.Operation, ExportEnabled: cfg.ExportEnabled}
	cleanup := env("CLEANUP", "0") == "1"
	var readinessDone chan struct{}
	var readinessMu sync.Mutex
	var readinessLost atomic.Bool
	defer func() {
		res.Cluster.Lost = readinessLost.Load()
		if readinessDone != nil {
			statusCtx, statusCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.RequestTimeout)
			status, err := c.clusterStatus(statusCtx)
			statusCancel()
			sample := clusterStatusSample{At: time.Now(), Status: status}
			if err != nil {
				sample.Error = err.Error()
			}
			readinessMu.Lock()
			res.Cluster.Samples = append(res.Cluster.Samples, sample)
			res.Cluster.Final = status
			readinessMu.Unlock()
		}
		cancel()
		if readinessDone != nil {
			cancel()
			<-readinessDone
		}
		if cleanup {
			benchmarkLog("cleanup: deleting topic %q", cfg.Topic)
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			err := c.deleteAndWait(cleanupCtx, cfg.Topic)
			cleanupCancel()
			if err == nil {
				res.Cleanup = true
				benchmarkLog("cleanup: topic %q deleted", cfg.Topic)
			} else if res.Integrity.Error == "" {
				res.Integrity.Error = "cleanup: " + err.Error()
				benchmarkLog("cleanup failed: %v", err)
			}
		}
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(cfg.Output, b, 0644); err != nil {
			benchmarkLog("write result failed: %v", err)
		} else {
			benchmarkLog("result written to %s", cfg.Output)
		}
	}()
	benchmarkLog("configuration: operation=%s api=%s endpoint=%s topic=%s run_id=%s target_bytes=%d message_bytes=%d partitions=%d replication_factor=%d export_enabled=%t batch_messages=%d producer_concurrency=%d storage_mode=%s", cfg.Operation, cfg.API, cfg.BaseURL, cfg.Topic, cfg.RunID, cfg.TargetBytes, cfg.MessageBytes, cfg.Partitions, cfg.ReplicationFactor, cfg.ExportEnabled, cfg.BatchMessages, cfg.ProducerConcurrency, cfg.StorageMode)
	if cleanup {
		benchmarkLog("cleanup is enabled; topic %q will be deleted after the run", cfg.Topic)
	} else {
		benchmarkLog("cleanup is disabled; topic %q will be retained", cfg.Topic)
	}
	if cfg.Operation != "all" {
		runSingleOperation(ctx, c, cfg, &res)
		return
	}
	benchmarkLog("creating topic %q", cfg.Topic)
	if err := c.create(ctx, cfg); err != nil {
		benchmarkLog("topic creation failed: %v", err)
		return
	}
	benchmarkLog("topic %q created and partition assignments are ready", cfg.Topic)
	if err := c.waitClusterReady(ctx, cfg); err != nil {
		res.Integrity.Error = "cluster readiness: " + err.Error()
		benchmarkLog("cluster readiness failed: %v", err)
		return
	}
	benchmarkLog("cluster is ready; starting producer and visibility sampling")
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	if cfg.StorageMode != "diskless" {
		// Diskless runs do not depend on cluster-wide readiness, so the run is
		// not canceled when the aggregate cluster status reports not ready.
		readinessDone = make(chan struct{})
		go func() {
			defer close(readinessDone)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			poll := func() {
				status, err := c.clusterStatus(runCtx)
				sample := clusterStatusSample{At: time.Now(), Status: status}
				if err != nil {
					sample.Error = err.Error()
					readinessLost.Store(true)
				} else if !status.Ready {
					readinessLost.Store(true)
				}
				if readinessLost.Load() {
					runCancel()
				}
				readinessMu.Lock()
				res.Cluster.Samples = append(res.Cluster.Samples, sample)
				res.Cluster.Final = status
				readinessMu.Unlock()
			}
			poll()
			for {
				select {
				case <-ticker.C:
					poll()
				case <-runCtx.Done():
					return
				}
			}
		}()
	}
	count, countErr := targetCount(cfg.TargetBytes, cfg.MessageBytes)
	if countErr != nil {
		panic(countErr)
	}
	res.Expected = count
	res.ExpectedBytes = count * cfg.MessageBytes
	expectedStates := make([]hashState, cfg.Partitions)
	var produced int64
	produce := c.produce
	consume := c.consume
	if cfg.API == "kafka" {
		produce = produceKafka
		consume = consumeKafka
	}
	pr, err := produce(runCtx, cfg, count, expectedStates, func(n int64) {
		atomic.AddInt64(&produced, n)
		if produced%(int64(cfg.BatchMessages)*10) == 0 {
			fmt.Printf("produced %d/%d\n", produced, count)
		}
	})
	if err != nil {
		res.Integrity.Error = "produce: " + err.Error()
		benchmarkLog("produce failed: %v", err)
		return
	}
	res.Producer = pr
	res.Produced = pr.Records
	benchmarkLog("produce complete: records=%d bytes=%d duration=%.3fs rate=%.2f records/s %.2f bytes/s", pr.Records, pr.Bytes, pr.DurationSeconds, pr.RecordsPerSecond, pr.BytesPerSecond)
	benchmarkLog("starting consumer verification")
	actualStates := make([]hashState, cfg.Partitions)
	cr, err := consume(runCtx, cfg, expectedStates, actualStates, count, func(n int64) {})
	if err != nil {
		res.Integrity.Error = "consume: " + err.Error()
		benchmarkLog("consume failed: %v", err)
		return
	}
	res.Consumer = cr
	res.Consumed = cr.Records
	res.ConsumedBytes = cr.Bytes
	res.Throughput = throughputResult{WriteBytesPerSecond: pr.BytesPerSecond, ReadBytesPerSecond: cr.BytesPerSecond}
	benchmarkLog("consume complete: records=%d bytes=%d duration=%.3fs rate=%.2f records/s %.2f bytes/s", cr.Records, cr.Bytes, cr.DurationSeconds, cr.RecordsPerSecond, cr.BytesPerSecond)
	if readinessLost.Load() {
		res.Integrity = integrityResult{Error: "cluster readiness became false during benchmark"}
		benchmarkLog("benchmark failed: cluster readiness was lost")
		return
	}
	ok := cr.Records == count && cr.Bytes == res.ExpectedBytes && verifyConsumeStates(expectedStates, actualStates)
	res.Integrity = integrityResult{OK: ok}
	benchmarkLog("benchmark complete: integrity_ok=%t export_enabled=%t", ok, cfg.ExportEnabled)
}
