// Command typed-topic-benchmark exercises the typed HTTP topic, consume, and
// Parquet/SQL paths without retaining the generated dataset.
package main

import (
	"bytes"
	"context"
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
	QueryInterval                                    time.Duration
	ConsumeTimeout                                   time.Duration
	RequestTimeout                                   time.Duration
	SequenceStart                                    int64
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
	SQL           sqlResult              `json:"sql"`
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
type sqlSample struct {
	At           time.Time `json:"at"`
	LatencyMS    float64   `json:"latency_ms"`
	ExecutionMS  float64   `json:"execution_time_ms"`
	Visible      int64     `json:"visible"`
	MinSequence  int64     `json:"min_sequence"`
	MaxSequence  int64     `json:"max_sequence"`
	PayloadBytes int64     `json:"payload_bytes"`
	Error        string    `json:"error,omitempty"`
}
type sqlResult struct {
	Samples           []sqlSample `json:"samples"`
	FinalLatencyMS    float64     `json:"final_latency_ms"`
	FinalExecutionMS  float64     `json:"final_execution_time_ms"`
	FinalVisible      int64       `json:"final_visible"`
	FinalMinSequence  int64       `json:"final_min_sequence"`
	FinalMaxSequence  int64       `json:"final_max_sequence"`
	FinalPayloadBytes int64       `json:"final_payload_bytes"`
	Regression        bool        `json:"regression"`
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
	ID           int64  `json:"id"`
	Payload      string `json:"payload"`
	PayloadBytes int64  `json:"payload_bytes"`
	Sequence     int64  `json:"sequence"`
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
	if operation != "all" && operation != "produce" && operation != "consume" && operation != "sql" {
		return config{}, fmt.Errorf("BENCHMARK_OPERATION must be all, produce, consume, or sql")
	}
	exportEnabled, err := strconv.ParseBool(env("EXPORT_ENABLED", "true"))
	if err != nil {
		return config{}, fmt.Errorf("EXPORT_ENABLED must be true or false")
	}
	if !exportEnabled && operation == "sql" {
		return config{}, errors.New("sql operation requires EXPORT_ENABLED=true")
	}
	targetBytes, err := parseByteSize("TARGET_BYTES", 5*1024*1024*1024)
	if err != nil {
		return config{}, err
	}
	messageBytes, err := parseByteSize("MESSAGE_BYTES", 1024)
	if err != nil {
		return config{}, err
	}
	partitions, err := parseIntOption("PARTITIONS", 4)
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
	return config{BaseURL: strings.TrimRight(env("CAMU_URL", "http://127.0.0.1:8080"), "/"), Topic: topic, Output: env("OUTPUT", "typed-topic-benchmark.json"), API: api, Operation: operation, NodeURLs: nodeURLs, KafkaBrokers: kafkaBrokers, TargetBytes: targetBytes, MessageBytes: messageBytes, Partitions: partitions, ReplicationFactor: replicationFactor, MinInSyncReplicas: minInSyncReplicas, BatchMessages: batchMessages, ProducerConcurrency: producerConcurrency, ExportEnabled: exportEnabled, QueryInterval: d, ConsumeTimeout: consumeTimeout, RequestTimeout: requestTimeout}, nil
}

type client struct {
	base           string
	http           *http.Client
	token          string
	requestTimeout time.Duration
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
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
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

func (c client) waitClusterReady(ctx context.Context) error {
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
	fields := []map[string]any{{"name": "id", "type": "int64", "path": "$.id"}, {"name": "payload", "type": "string", "path": "$.payload"}, {"name": "payload_bytes", "type": "int64", "path": "$.payload_bytes"}, {"name": "sequence", "type": "int64", "path": "$.sequence"}}
	body := map[string]any{"name": cfg.Topic, "partitions": cfg.Partitions, "replication_factor": cfg.ReplicationFactor, "min_insync_replicas": cfg.MinInSyncReplicas, "retention": "24h", "export_enabled": cfg.ExportEnabled, "schema": map[string]any{"encoding": "json", "fields": fields}}
	if err := c.request(ctx, http.MethodPost, "/v1/topics", body, nil); err != nil {
		return err
	}
	return c.waitForReplication(ctx, cfg)
}

type benchmarkTopic struct {
	Partitions    int  `json:"partitions"`
	ExportEnabled bool `json:"export_enabled"`
}

func (c client) ensureTopic(ctx context.Context, cfg config) (bool, error) {
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
	if err := c.waitForReplication(ctx, cfg); err != nil {
		return false, err
	}
	return true, nil
}

func (c client) committedRecordCount(ctx context.Context, cfg config) (int64, error) {
	var total uint64
	for partition := 0; partition < cfg.Partitions; partition++ {
		partitionClient := c
		if len(cfg.NodeURLs) > 0 {
			partitionClient.base = cfg.NodeURLs[partition%len(cfg.NodeURLs)]
		}
		var page consumeResponse
		headers, err := partitionClient.requestHeaders(ctx, http.MethodGet, fmt.Sprintf("/v1/topics/%s/partitions/%d/messages?offset=0&limit=1", url.PathEscape(cfg.Topic), partition), nil, &page)
		if err != nil {
			return 0, fmt.Errorf("read partition %d high watermark: %w", partition, err)
		}
		hw, err := strconv.ParseUint(headers.Get("X-High-Watermark"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("read partition %d high watermark: missing or invalid response header", partition)
		}
		if total > math.MaxInt64-hw {
			return 0, errors.New("committed record count exceeds int64")
		}
		total += hw
	}
	return int64(total), nil
}

func (c client) waitForReplication(ctx context.Context, cfg config) error {
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

func payload(n int64) string          { return strings.Repeat("x", int(n)) }
func digestFor(v typedValue) [32]byte { b, _ := json.Marshal(v); return sha256.Sum256(b) }
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
		expected[int(i%int64(cfg.Partitions))].add(typedValue{ID: i, Payload: payloadText, PayloadBytes: cfg.MessageBytes, Sequence: i})
	}
	return expected
}

func (c client) produce(ctx context.Context, cfg config, count int64, expected []hashState, progress func(int64)) (phaseResult, error) {
	start := time.Now()
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
				for first := firstSequenceForPartition(cfg.SequenceStart, p, cfg.Partitions); first < cfg.SequenceStart+count; first += int64(cfg.Partitions * cfg.BatchMessages) {
					batch := make([]map[string]any, 0, cfg.BatchMessages)
					for i := first; i < cfg.SequenceStart+count && len(batch) < cfg.BatchMessages; i += int64(cfg.Partitions) {
						v := typedValue{ID: i, Payload: payloadText, PayloadBytes: cfg.MessageBytes, Sequence: i}
						batch = append(batch, map[string]any{"key": strconv.FormatInt(i, 10), "value": string(mustJSON(v))})
						expected[p].add(v)
					}
					var out any
					atomic.AddInt64(&serialized, int64(len(mustJSON(batch))))
					path := fmt.Sprintf("/v1/topics/%s/partitions/%d/messages", url.PathEscape(cfg.Topic), p)
					partitionClient := c
					if len(cfg.NodeURLs) > 0 {
						partitionClient.base = cfg.NodeURLs[p%len(cfg.NodeURLs)]
					}
					for {
						err := partitionClient.request(ctx, http.MethodPost, path, batch, &out)
						if err == nil {
							break
						}
						if !strings.Contains(err.Error(), "partition not ready for replicated writes") && !strings.Contains(err.Error(), "partition "+strconv.Itoa(p)+" not initialized for topic") && !strings.Contains(err.Error(), "421 Misdirected Request") {
							errs <- err
							return
						}
						select {
						case <-time.After(250 * time.Millisecond):
						case <-ctx.Done():
							errs <- ctx.Err()
							return
						}
					}
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
	last           int64
	bad            error
}

func (s *hashState) add(v typedValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.h == nil {
		s.h = sha256.New()
		s.last = -1
	}
	if v.Sequence <= s.last && s.bad == nil {
		s.bad = fmt.Errorf("out-of-order sequence %d after %d", v.Sequence, s.last)
	}
	s.last = v.Sequence
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
		return 0, 0, hex.EncodeToString(empty[:]), s.bad
	}
	return s.records, s.bytes, hex.EncodeToString(s.h.Sum(nil)), s.bad
}

func (c client) consume(ctx context.Context, cfg config, expected []hashState, actual []hashState, count int64, progress func(int64)) (phaseResult, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.ConsumeTimeout)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, cfg.Partitions)
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
			benchmarkLog("consume partition=%d endpoint=%s expected_records=%d", p, partitionClient.base, expected[p].recordsSnapshot())
			for {
				var resp consumeResponse
				started := time.Now()
				err := partitionClient.request(ctx, http.MethodGet, fmt.Sprintf("/v1/topics/%s/partitions/%d/messages?offset=%d&limit=1000", url.PathEscape(cfg.Topic), p, off), nil, &resp)
				if err != nil {
					benchmarkLog("consume partition=%d endpoint=%s offset=%d failed after=%s error=%v", p, partitionClient.base, off, time.Since(started), err)
					errs <- err
					return
				}
				if len(resp.Messages) == 0 {
					if actual[p].recordsSnapshot() >= expected[p].recordsSnapshot() {
						benchmarkLog("consume partition=%d complete records=%d offset=%d", p, actual[p].recordsSnapshot(), off)
						return
					}
					benchmarkLog("consume partition=%d endpoint=%s offset=%d empty records=%d expected=%d duration=%s", p, partitionClient.base, off, actual[p].recordsSnapshot(), expected[p].recordsSnapshot(), time.Since(started))
					time.Sleep(100 * time.Millisecond)
					continue
				}
				for _, m := range resp.Messages {
					var v typedValue
					if err := json.Unmarshal([]byte(m.Value), &v); err != nil {
						errs <- err
						return
					}
					if actual[p].recordsSnapshot() >= expected[p].recordsSnapshot() {
						errs <- fmt.Errorf("consume HTTP: partition %d received record at offset %d after expected end offset %d", p, m.Offset, expected[p].recordsSnapshot())
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
					actual[p].add(v)
					progress(1)
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
		db, _ := hex.DecodeString(d)
		h.Write(db)
	}
	d := time.Since(start)
	return phaseResult{Records: records, Bytes: bytesN, DurationSeconds: d.Seconds(), RecordsPerSecond: float64(records) / d.Seconds(), BytesPerSecond: float64(bytesN) / d.Seconds(), Digest: hex.EncodeToString(h.Sum(nil))}, nil
}
func (s *hashState) recordsSnapshot() int64 { s.mu.Lock(); defer s.mu.Unlock(); return s.records }

type sqlMetrics struct{ Count, MinSequence, MaxSequence, PayloadBytes int64 }

func (c client) sql(ctx context.Context, cfg config) (sqlMetrics, float64, error) {
	start := time.Now()
	var resp struct {
		Rows [][]any `json:"rows"`
	}
	quoted := `"` + strings.ReplaceAll(cfg.Topic, `"`, `""`) + `"`
	err := c.request(ctx, http.MethodPost, "/v1/sql", map[string]any{"sql": fmt.Sprintf("SELECT count(*)::BIGINT, min(sequence)::BIGINT, max(sequence)::BIGINT, sum(payload_bytes)::BIGINT FROM %s", quoted), "topics": []string{cfg.Topic}}, &resp)
	if err != nil {
		return sqlMetrics{}, time.Since(start).Seconds() * 1000, err
	}
	if len(resp.Rows) == 0 {
		return sqlMetrics{}, time.Since(start).Seconds() * 1000, nil
	}
	if len(resp.Rows[0]) < 4 {
		return sqlMetrics{}, time.Since(start).Seconds() * 1000, errors.New("SQL returned fewer than four integrity columns")
	}
	toInt := func(v any) int64 {
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int64:
			return n
		case nil:
			return 0
		}
		return 0
	}
	row := resp.Rows[0]
	return sqlMetrics{Count: toInt(row[0]), MinSequence: toInt(row[1]), MaxSequence: toInt(row[2]), PayloadBytes: toInt(row[3])}, time.Since(start).Seconds() * 1000, nil
}

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

func (c client) waitForSQL(ctx context.Context, cfg config, res *result, count int64) (sqlMetrics, error) {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		metrics, ms, err := c.sql(ctx, cfg)
		sample := sqlSample{At: time.Now(), LatencyMS: ms, ExecutionMS: ms, Visible: metrics.Count, MinSequence: metrics.MinSequence, MaxSequence: metrics.MaxSequence, PayloadBytes: metrics.PayloadBytes}
		if err != nil {
			sample.Error = err.Error()
			benchmarkLog("sql topic=%s failed latency_ms=%.2f error=%v", cfg.Topic, ms, err)
		} else {
			benchmarkLog("sql topic=%s visible=%d/%d min_sequence=%d max_sequence=%d payload_bytes=%d latency_ms=%.2f", cfg.Topic, metrics.Count, count, metrics.MinSequence, metrics.MaxSequence, metrics.PayloadBytes, ms)
		}
		res.SQL.Samples = append(res.SQL.Samples, sample)
		res.SQL.FinalVisible = metrics.Count
		res.SQL.FinalMinSequence = metrics.MinSequence
		res.SQL.FinalMaxSequence = metrics.MaxSequence
		res.SQL.FinalPayloadBytes = metrics.PayloadBytes
		res.SQL.FinalLatencyMS = ms
		res.SQL.FinalExecutionMS = ms
		if err == nil && metrics.Count >= count {
			return metrics, nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return metrics, err
			}
			return metrics, errors.New("timed out waiting for SQL visibility")
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return metrics, ctx.Err()
		}
	}
}

func runSingleOperation(ctx context.Context, c client, cfg config, res *result) {
	if err := c.waitClusterReady(ctx); err != nil {
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
			cfg.SequenceStart, err = c.committedRecordCount(ctx, cfg)
			if err != nil {
				res.Integrity.Error = "read existing topic: " + err.Error()
				benchmarkLog("read existing topic failed: %v", err)
				return
			}
			benchmarkLog("appending to topic %q at sequence=%d", cfg.Topic, cfg.SequenceStart)
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
		res.Integrity.OK = cr.Records == count && cr.Bytes == res.ExpectedBytes && verifyConsumeStates(expected, actual)
		if !res.Integrity.OK {
			res.Integrity.Error = "consume integrity mismatch"
		}
		benchmarkLog("consume complete: records=%d bytes=%d duration=%.3fs rate=%.2f records/s %.2f bytes/s integrity_ok=%t", cr.Records, cr.Bytes, cr.DurationSeconds, cr.RecordsPerSecond, cr.BytesPerSecond, res.Integrity.OK)
	case "sql":
		if !cfg.ExportEnabled {
			res.Integrity.Error = "sql operation requires EXPORT_ENABLED=true"
			return
		}
		benchmarkLog("waiting for SQL visibility of %d records", count)
		metrics, err := c.waitForSQL(ctx, cfg, res, count)
		if err != nil {
			res.Integrity.Error = "sql: " + err.Error()
			benchmarkLog("SQL visibility failed: %v", err)
			return
		}
		res.Integrity.OK = metrics.Count == count && metrics.MinSequence == 0 && metrics.MaxSequence == count-1 && metrics.PayloadBytes == res.ExpectedBytes
		if !res.Integrity.OK {
			res.Integrity.Error = "SQL integrity mismatch"
		}
		benchmarkLog("SQL complete: integrity_ok=%t", res.Integrity.OK)
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
	var queryDone chan struct{}
	var readinessDone chan struct{}
	var sqlMu sync.Mutex
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
		if queryDone != nil {
			<-queryDone
		}
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
	benchmarkLog("configuration: operation=%s api=%s endpoint=%s topic=%s target_bytes=%d message_bytes=%d partitions=%d replication_factor=%d export_enabled=%t batch_messages=%d producer_concurrency=%d", cfg.Operation, cfg.API, cfg.BaseURL, cfg.Topic, cfg.TargetBytes, cfg.MessageBytes, cfg.Partitions, cfg.ReplicationFactor, cfg.ExportEnabled, cfg.BatchMessages, cfg.ProducerConcurrency)
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
	if err := c.waitClusterReady(ctx); err != nil {
		res.Integrity.Error = "cluster readiness: " + err.Error()
		benchmarkLog("cluster readiness failed: %v", err)
		return
	}
	benchmarkLog("cluster is ready; starting producer and SQL visibility sampling")
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
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
	count, countErr := targetCount(cfg.TargetBytes, cfg.MessageBytes)
	if countErr != nil {
		panic(countErr)
	}
	res.Expected = count
	res.ExpectedBytes = count * cfg.MessageBytes
	expectedStates := make([]hashState, cfg.Partitions)
	var produced int64
	if cfg.ExportEnabled {
		queryDone = make(chan struct{})
		go func() {
			defer close(queryDone)
			var previous sqlMetrics
			ticker := time.NewTicker(cfg.QueryInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					metrics, ms, e := c.sql(runCtx, cfg)
					samp := sqlSample{At: time.Now(), LatencyMS: ms, ExecutionMS: ms, Visible: metrics.Count, MinSequence: metrics.MinSequence, MaxSequence: metrics.MaxSequence, PayloadBytes: metrics.PayloadBytes}
					if e != nil {
						samp.Error = e.Error()
						benchmarkLog("sql sample topic=%s failed latency_ms=%.2f error=%v", cfg.Topic, ms, e)
					} else {
						benchmarkLog("sql sample topic=%s visible=%d min_sequence=%d max_sequence=%d payload_bytes=%d latency_ms=%.2f", cfg.Topic, metrics.Count, metrics.MinSequence, metrics.MaxSequence, metrics.PayloadBytes, ms)
					}
					sqlMu.Lock()
					if e == nil {
						consistent := metrics.Count >= previous.Count && metrics.PayloadBytes == metrics.Count*cfg.MessageBytes
						if metrics.Count > 0 {
							consistent = consistent && metrics.MinSequence == 0 && metrics.MaxSequence == metrics.Count-1
						}
						if metrics.MinSequence < previous.MinSequence || metrics.MaxSequence < previous.MaxSequence || !consistent {
							res.SQL.Regression = true
						}
						if metrics.Count > previous.Count {
							previous = metrics
						}
					}
					res.SQL.Samples = append(res.SQL.Samples, samp)
					sqlMu.Unlock()
				case <-ctx.Done():
					return
				}
			}
		}()
	}
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
	if !cfg.ExportEnabled {
		if readinessLost.Load() {
			res.Integrity = integrityResult{Error: "cluster readiness became false during benchmark"}
			benchmarkLog("benchmark failed: cluster readiness was lost")
			return
		}
		ok := cr.Records == count && cr.Bytes == res.ExpectedBytes && verifyConsumeStates(expectedStates, actualStates)
		res.Integrity = integrityResult{OK: ok}
		benchmarkLog("benchmark complete: integrity_ok=%t export_enabled=false", ok)
		return
	}
	benchmarkLog("waiting for SQL visibility of %d records", count)
	var metrics sqlMetrics
	var ms float64
	deadline := time.Now().Add(2 * time.Minute)
	for {
		metrics, ms, err = c.sql(ctx, cfg)
		if err == nil && metrics.Count >= count || time.Now().After(deadline) {
			break
		}
		if ctx.Err() != nil {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	sqlMu.Lock()
	res.SQL.FinalVisible = metrics.Count
	res.SQL.FinalMinSequence = metrics.MinSequence
	res.SQL.FinalMaxSequence = metrics.MaxSequence
	res.SQL.FinalPayloadBytes = metrics.PayloadBytes
	res.SQL.FinalLatencyMS = ms
	res.SQL.FinalExecutionMS = ms
	sqlMu.Unlock()
	benchmarkLog("SQL visibility: records=%d payload_bytes=%d latency_ms=%.2f", metrics.Count, metrics.PayloadBytes, ms)
	readinessMu.Lock()
	res.Cluster.Lost = readinessLost.Load()
	readinessMu.Unlock()
	if readinessLost.Load() {
		res.Integrity = integrityResult{Error: "cluster readiness became false during benchmark"}
		benchmarkLog("benchmark failed: cluster readiness was lost")
		return
	}
	if err != nil {
		res.Integrity = integrityResult{Error: err.Error()}
		benchmarkLog("benchmark failed: SQL visibility wait ended with error: %v", err)
		return
	}
	ok := !res.SQL.Regression && metrics.Count == count && metrics.MinSequence == 0 && metrics.MaxSequence == count-1 && metrics.PayloadBytes == res.ExpectedBytes && cr.Records == count && cr.Bytes == res.ExpectedBytes
	for p := range expectedStates {
		er, eb, ed, ee := expectedStates[p].result()
		ar, ab, ad, ae := actualStates[p].result()
		if ee != nil || ae != nil || er != ar || eb != ab || ed != ad {
			ok = false
		}
	}
	res.Integrity = integrityResult{OK: ok && !readinessLost.Load()}
	benchmarkLog("benchmark complete: integrity_ok=%t sql_regression=%t", res.Integrity.OK, res.SQL.Regression)
}
