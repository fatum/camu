package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type serviceConfig struct {
	RunID         string
	Topics        []string
	StorageModes  []string
	RateLimit     int
	Partitions    int
	MessageBytes  int64
	StatsInterval time.Duration
	S3Bucket      string
	S3Prefix      string
	S3Endpoint    string
	S3Region      string
	KafkaBrokers  []string
	NodeID        string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadServiceConfig() (serviceConfig, error) {
	rateLimit, err := strconv.Atoi(env("BENCHMARK_RATE", "10000"))
	if err != nil || rateLimit < 1 {
		return serviceConfig{}, fmt.Errorf("BENCHMARK_RATE must be a positive integer")
	}
	partitions, err := strconv.Atoi(env("PARTITIONS", "12"))
	if err != nil || partitions < 1 {
		return serviceConfig{}, fmt.Errorf("PARTITIONS must be a positive integer")
	}
	messageBytes, err := strconv.ParseInt(env("MESSAGE_BYTES", "1024"), 10, 64)
	if err != nil || messageBytes < 1 {
		return serviceConfig{}, fmt.Errorf("MESSAGE_BYTES must be a positive integer")
	}
	statsInterval, err := time.ParseDuration(env("STATS_INTERVAL", "5m"))
	if err != nil || statsInterval <= 0 {
		return serviceConfig{}, fmt.Errorf("STATS_INTERVAL must be a positive duration")
	}

	topicRaw := env("TOPICS", "")
	if topicRaw == "" {
		return serviceConfig{}, fmt.Errorf("TOPICS is required (comma-separated)")
	}
	topics := strings.Split(topicRaw, ",")
	modesRaw := env("STORAGE_MODES", "")
	if modesRaw == "" {
		return serviceConfig{}, fmt.Errorf("STORAGE_MODES is required (comma-separated, diskless or classic)")
	}
	modes := strings.Split(modesRaw, ",")
	if len(topics) != len(modes) {
		return serviceConfig{}, fmt.Errorf("TOPICS and STORAGE_MODES must have the same length")
	}

	kafkaRaw := env("KAFKA_BROKERS", "")
	if kafkaRaw == "" {
		return serviceConfig{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	var brokers []string
	for _, b := range strings.Split(kafkaRaw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}

	runID := env("BENCHMARK_RUN_ID", "")
	if runID == "" {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return serviceConfig{}, fmt.Errorf("generate run ID: %w", err)
		}
		runID = hex.EncodeToString(token[:])
	}

	return serviceConfig{
		RunID:         runID,
		Topics:        topics,
		StorageModes:  modes,
		RateLimit:     rateLimit,
		Partitions:    partitions,
		MessageBytes:  messageBytes,
		StatsInterval: statsInterval,
		S3Bucket:      env("S3_BUCKET", ""),
		S3Prefix:      env("S3_PREFIX", "benchmark-stats"),
		S3Endpoint:    env("S3_ENDPOINT", ""),
		S3Region:      env("S3_REGION", "us-east-1"),
		KafkaBrokers:  brokers,
		NodeID:        env("NODE_ID", "benchmark-node"),
	}, nil
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if len(os.Args) > 1 && os.Args[1] == "analyze" {
		os.Exit(runAnalyze())
	}

	cfg, err := loadServiceConfig()
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(2)
	}
	initS3()

	slog.Info("benchmark_service_starting",
		"run_id", cfg.RunID,
		"topics", cfg.Topics,
		"rate", cfg.RateLimit,
		"partitions", cfg.Partitions,
		"message_bytes", cfg.MessageBytes,
		"stats_interval", cfg.StatsInterval.String(),
		"node_id", cfg.NodeID,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	stats := newStats(cfg)
	go statsWindowLoop(ctx, cfg, stats)

	for i, topic := range cfg.Topics {
		topic := strings.TrimSpace(topic)
		mode := strings.TrimSpace(cfg.StorageModes[i])
		go runTopic(ctx, cfg, topic, mode, stats)
	}

	// Verification loop checks diskless merge, Parquet export, and Iceberg
	// metadata every 10 minutes (or 2x stats interval, whichever is longer).
	go startVerificationLoop(ctx, cfg, stats)

	<-ctx.Done()
	slog.Info("benchmark_service_shutting_down")
	time.Sleep(2 * time.Second)

	b, _ := json.MarshalIndent(map[string]any{
		"run_id":         cfg.RunID,
		"total_produced": stats.totalProd.Load(),
		"total_consumed": stats.totalCons.Load(),
		"total_errors":   stats.totalErr.Load(),
	}, "", "  ")
	fmt.Fprintln(os.Stderr, string(b))
}

func runTopic(ctx context.Context, cfg serviceConfig, topic, mode string, stats *statsAccumulator) {
	slog.Info("topic_loop_starting", "topic", topic, "mode", mode)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		produceLoop(ctx, cfg, topic, stats)
	}()
	go func() {
		defer wg.Done()
		consumeLoop(ctx, cfg, topic, stats)
	}()
	wg.Wait()
}

func statsWindowLoop(ctx context.Context, cfg serviceConfig, stats *statsAccumulator) {
	ticker := time.NewTicker(cfg.StatsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			end := now.UTC()
			start := end.Add(-cfg.StatsInterval)
			snap := stats.snapshot(start, end)
			stats.uploadSnapshot(snap)
		}
	}
}

func newKafkaConsumer(cfg serviceConfig, topic string) (*kgo.Client, error) {
	assign := make(map[int32]kgo.Offset, cfg.Partitions)
	for p := 0; p < cfg.Partitions; p++ {
		assign[int32(p)] = kgo.NewOffset().AtStart()
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.FetchMaxPartitionBytes(16<<20),
		kgo.FetchMaxBytes(64<<20),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}),
	)
}
