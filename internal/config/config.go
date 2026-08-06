package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the camu server.
type Config struct {
	Server       ServerConfig       `yaml:"server"`
	Storage      StorageConfig      `yaml:"storage"`
	Segments     SegmentsConfig     `yaml:"segments"`
	Cache        CacheConfig        `yaml:"cache"`
	Coordination CoordinationConfig `yaml:"coordination"`
	Diskless     DisklessConfig     `yaml:"diskless"`
	Maintenance  MaintenanceConfig  `yaml:"maintenance"`
}

// MaintenanceConfig groups partition-leader maintenance knobs.
type MaintenanceConfig struct {
	ParquetExport ParquetExportConfig `yaml:"parquet_export"`
}

// ParquetExportConfig controls the per-partition export consumer, which
// writes committed records as self-managed Apache Iceberg tables.
type ParquetExportConfig struct {
	MaxRecords  int    `yaml:"max_records"`
	MaxDuration string `yaml:"max_duration"`
	// TempDirectory is where the export pipeline encodes Parquet data files
	// before uploading them.
	TempDirectory string `yaml:"temp_directory"`
	// Warehouse is the object-store prefix Iceberg tables live under.
	Warehouse string `yaml:"warehouse"`
	// TargetBytes bounds how much data one Iceberg snapshot commits; the
	// export pass keeps appending data files until the target or MaxInterval
	// is reached, so snapshots and manifest lists stay small at high load.
	TargetBytes int64 `yaml:"target_bytes"`
	// MaxInterval bounds how long an Iceberg export pass may run before it
	// commits whatever it buffered.
	MaxInterval string `yaml:"max_interval"`
}

const (
	defaultExportWarehouse   = "warehouse/"
	defaultExportTargetBytes = 64 << 20
	defaultExportMaxInterval = 30 * time.Second
	defaultExportTempDir     = "/var/lib/camu/export-tmp"
)

func (p ParquetExportConfig) MaxRecordsValue() int {
	if p.MaxRecords <= 0 {
		return 16384
	}
	return p.MaxRecords
}
func (p ParquetExportConfig) MaxDurationValue() time.Duration {
	d, err := time.ParseDuration(p.MaxDuration)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

// WarehouseValue returns the Iceberg warehouse prefix, defaulting to
// warehouse/.
func (p ParquetExportConfig) WarehouseValue() string {
	if p.Warehouse == "" {
		return defaultExportWarehouse
	}
	return p.Warehouse
}

// TempDirectoryValue returns the export temp directory, defaulting to
// /var/lib/camu/export-tmp.
func (p ParquetExportConfig) TempDirectoryValue() string {
	if p.TempDirectory == "" {
		return defaultExportTempDir
	}
	return p.TempDirectory
}

// TargetBytesValue returns the Iceberg snapshot byte target, defaulting to
// 64 MiB.
func (p ParquetExportConfig) TargetBytesValue() int64 {
	if p.TargetBytes <= 0 {
		return defaultExportTargetBytes
	}
	return p.TargetBytes
}

// MaxIntervalValue returns the Iceberg snapshot commit interval, defaulting to
// 30s.
func (p ParquetExportConfig) MaxIntervalValue() time.Duration {
	d, err := time.ParseDuration(p.MaxInterval)
	if err != nil || d <= 0 {
		return defaultExportMaxInterval
	}
	return d
}

// DisklessConfig holds settings for diskless topic mode.
type DisklessConfig struct {
	LingerMs      int              `yaml:"linger_ms"`       // Max buffer time before flush (default 250)
	MaxBatchBytes int64            `yaml:"max_batch_bytes"` // Max buffer size before flush (default 8MiB)
	MetaStore     string           `yaml:"metastore"`       // "memory" (default), "s3", or "dynamodb"
	DynamoDB      DynamoDBConfig   `yaml:"dynamodb"`
	Compaction    CompactionConfig `yaml:"compaction"`
}

// DynamoDBConfig holds DynamoDB settings for the diskless MetaStore.
type DynamoDBConfig struct {
	TablePrefix string `yaml:"table_prefix"` // default "camu"
	Region      string `yaml:"region"`
	Endpoint    string `yaml:"endpoint"` // for local DynamoDB
}

// CompactionConfig holds settings for diskless small-segment compaction.
type CompactionConfig struct {
	Enabled             bool   `yaml:"enabled"`
	MinSegments         int    `yaml:"min_segments"`           // merge only when at least this many eligible refs
	TargetBytes         int64  `yaml:"target_bytes"`           // merge refs until approximately this total size
	MaxSegmentsPerMerge int    `yaml:"max_segments_per_merge"` // cap a single merge (DynamoDB transactions allow at most 100)
	Grace               string `yaml:"grace"`                  // minimum age of a ref before it is eligible
	DeleteGrace         string `yaml:"delete_grace"`           // delay before deleting compacted source data
	Interval            string `yaml:"interval"`               // how often a node drives merge discovery and execution for the partitions it leads
}

const (
	defaultCompactionMinSegments         = 4
	defaultCompactionTargetBytes         = 64 << 20
	defaultCompactionMaxSegmentsPerMerge = 90
	defaultCompactionGrace               = 60 * time.Second
	defaultCompactionDeleteGrace         = 5 * time.Minute
	// DefaultCompactionInterval is how often the dedicated compaction loop
	// drives diskless merge work. The maintenance pass runs far less often
	// (every 10th heartbeat), which alone limits compaction to roughly one
	// target-sized merge per pass — an order of magnitude below sustained
	// production. The loop ticks at this interval so merges pipeline as fast
	// as the merge executor and object store allow.
	DefaultCompactionInterval = 2 * time.Second

	// minCompactionSegments is the lower bound for a merge run. A single source
	// cannot be merged (there is nothing to combine) and would be rejected by
	// the merge executor, so min_segments is clamped to at least 2.
	minCompactionSegments = 2
	// maxCompactionSegmentsPerMerge is the upper bound for a merge run. The
	// DynamoDB metastore replaces refs in one transaction of at most 100
	// operations (one delete per removed ref plus one put for the merged ref),
	// so a run is clamped to 99 sources to guarantee the transaction fits.
	maxCompactionSegmentsPerMerge = 99
)

func (c CompactionConfig) MinSegmentsValue() int {
	min := defaultCompactionMinSegments
	if c.MinSegments > 0 {
		min = c.MinSegments
	}
	if min < minCompactionSegments {
		min = minCompactionSegments
	}
	// Clamp the minimum to the effective maximum so the two bounds never become
	// unreachable (a run is capped at the maximum, so it must be able to reach
	// the minimum).
	if max := c.MaxSegmentsPerMergeValue(); min > max {
		min = max
	}
	return min
}

func (c CompactionConfig) TargetBytesValue() int64 {
	if c.TargetBytes <= 0 {
		return defaultCompactionTargetBytes
	}
	return c.TargetBytes
}

func (c CompactionConfig) MaxSegmentsPerMergeValue() int {
	max := defaultCompactionMaxSegmentsPerMerge
	if c.MaxSegmentsPerMerge > 0 {
		max = c.MaxSegmentsPerMerge
	}
	if max < minCompactionSegments {
		return minCompactionSegments
	}
	if max > maxCompactionSegmentsPerMerge {
		return maxCompactionSegmentsPerMerge
	}
	return max
}

func (c CompactionConfig) GraceDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.Grace, defaultCompactionGrace)
}

func (c CompactionConfig) DeleteGraceDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.DeleteGrace, defaultCompactionDeleteGrace)
}

// IntervalDuration returns how often the dedicated diskless compaction loop
// drives merge work, defaulting to DefaultCompactionInterval.
func (c CompactionConfig) IntervalDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.Interval, DefaultCompactionInterval)
}

// LingerDuration returns the linger duration, defaulting to 250ms.
func (d DisklessConfig) LingerDuration() time.Duration {
	ms := d.LingerMs
	if ms <= 0 {
		ms = 250
	}
	return time.Duration(ms) * time.Millisecond
}

// MaxBatchBytesValue returns the max batch bytes, defaulting to 8MiB.
func (d DisklessConfig) MaxBatchBytesValue() int64 {
	if d.MaxBatchBytes <= 0 {
		return 8 * 1024 * 1024
	}
	return d.MaxBatchBytes
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Address               string `yaml:"address"`
	InternalAddress       string `yaml:"internal_address"`
	ReplicationAddress    string `yaml:"replication_address"`
	InstanceID            string `yaml:"instance_id"`
	AuthToken             string `yaml:"auth_token"`              // Public API bearer token (optional)
	HeapProfileEnabled    bool   `yaml:"heap_profile_enabled"`    // Authenticated heap-profile endpoint (disabled by default)
	ClusterToken          string `yaml:"cluster_token"`           // Internal API shared secret (optional)
	KafkaPort             int    `yaml:"kafka_port"`              // Kafka protocol port (0 = disabled)
	KafkaAdvertiseAddress string `yaml:"kafka_advertise_address"` // Public Kafka host:port override (optional)
}

// StorageConfig holds S3-compatible object storage settings.
type StorageConfig struct {
	Bucket      string            `yaml:"bucket"`
	Region      string            `yaml:"region"`
	Endpoint    string            `yaml:"endpoint"`
	Credentials CredentialsConfig `yaml:"credentials"`
}

// CredentialsConfig holds S3 access credentials.
type CredentialsConfig struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

// SegmentsConfig holds segment management settings.
type SegmentsConfig struct {
	MaxSize               int64  `yaml:"max_size"`
	MaxAge                string `yaml:"max_age"`
	Compression           string `yaml:"compression"`
	RecordBatchTargetSize int64  `yaml:"record_batch_target_size"`
	IndexIntervalBytes    int    `yaml:"index_interval_bytes"`
}

const (
	defaultSegmentRecordBatchTargetSize = 16 * 1024
	defaultSegmentIndexIntervalBytes    = 4096
)

// MaxAgeDuration parses MaxAge as a time.Duration.
// Returns 5 * time.Second if MaxAge is empty.
func (s SegmentsConfig) MaxAgeDuration() (time.Duration, error) {
	if s.MaxAge == "" {
		return 5 * time.Second, nil
	}
	return time.ParseDuration(s.MaxAge)
}

// CacheConfig holds disk cache settings.
type CacheConfig struct {
	Directory string `yaml:"directory"`
	MaxSize   int64  `yaml:"max_size"`
}

// CoordinationConfig holds distributed coordination settings.
type CoordinationConfig struct {
	LeaseTTL                  string `yaml:"lease_ttl"`
	HeartbeatInterval         string `yaml:"heartbeat_interval"`
	RebalanceDelay            string `yaml:"rebalance_delay"`
	InstanceTTL               string `yaml:"instance_ttl"`
	ISRExpansionThreshold     int    `yaml:"isr_expansion_threshold"`
	ReplicationTimeout        string `yaml:"replication_timeout"`
	ReplicationReadTimeout    string `yaml:"replication_read_timeout"`
	MaintenanceMaxConcurrency int    `yaml:"maintenance_max_concurrency"`
	FenceInterval             string `yaml:"fence_interval"`
}

const (
	defaultLeaseTTL                  = 30 * time.Second
	defaultHeartbeatInterval         = 10 * time.Second
	defaultRebalanceDelay            = 5 * time.Second
	defaultISRExpansionThreshold     = 1000
	defaultReplicationTimeout        = 30 * time.Second
	defaultReplicationReadTimeout    = 10 * time.Second
	defaultMaintenanceMaxConcurrency = 4
	defaultFenceInterval             = 2 * time.Second
)

func parseDurationOrDefault(raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	return time.ParseDuration(raw)
}

func (c CoordinationConfig) LeaseTTLDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.LeaseTTL, defaultLeaseTTL)
}

func (c CoordinationConfig) HeartbeatIntervalDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.HeartbeatInterval, defaultHeartbeatInterval)
}

func (c CoordinationConfig) RebalanceDelayDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.RebalanceDelay, defaultRebalanceDelay)
}

func (c CoordinationConfig) InstanceTTLDuration() (time.Duration, error) {
	if c.InstanceTTL == "" {
		leaseTTL, err := c.LeaseTTLDuration()
		if err != nil {
			return 0, err
		}
		return leaseTTL * 3, nil
	}
	return time.ParseDuration(c.InstanceTTL)
}

func (c CoordinationConfig) ISRExpansionThresholdValue() int {
	if c.ISRExpansionThreshold <= 0 {
		return defaultISRExpansionThreshold
	}
	return c.ISRExpansionThreshold
}

func (c CoordinationConfig) ReplicationTimeoutDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.ReplicationTimeout, defaultReplicationTimeout)
}

// ReplicationReadTimeoutDuration returns how long a follower waits for the
// leader to respond to a fetch before counting it as an error toward
// leader-down detection. It is independent of the produce purgatory timeout:
// a healthy leader responds within its ~500ms long-poll window, so this can be
// far shorter than replication_timeout to detect a paused or unresponsive
// leader faster.
func (c CoordinationConfig) ReplicationReadTimeoutDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.ReplicationReadTimeout, defaultReplicationReadTimeout)
}

func (c CoordinationConfig) MaintenanceMaxConcurrencyValue() int {
	if c.MaintenanceMaxConcurrency <= 0 {
		return defaultMaintenanceMaxConcurrency
	}
	return c.MaintenanceMaxConcurrency
}

// FenceIntervalDuration returns how often the rf=1 produce path re-verifies
// partition ownership against the assignment store before acknowledging. A
// shorter interval narrows the window in which a fenced leader can ack writes
// it will later lose, at the cost of more assignment-store reads.
func (c CoordinationConfig) FenceIntervalDuration() (time.Duration, error) {
	return parseDurationOrDefault(c.FenceInterval, defaultFenceInterval)
}

// Load reads a YAML config file at path, applies defaults, and validates required fields.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %q: %w", path, err)
	}

	cfg := defaults()

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file %q: %w", path, err)
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// defaults returns a Config populated with all default values.
func defaults() *Config {
	return &Config{
		Server: ServerConfig{
			Address:            ":8080",
			InternalAddress:    ":8081",
			ReplicationAddress: ":8082",
		},
		Segments: SegmentsConfig{
			MaxSize:               8388608,
			MaxAge:                "5s",
			Compression:           "none",
			RecordBatchTargetSize: defaultSegmentRecordBatchTargetSize,
			IndexIntervalBytes:    defaultSegmentIndexIntervalBytes,
		},
		Cache: CacheConfig{
			Directory: "/var/lib/camu/cache",
			MaxSize:   10737418240,
		},
		Coordination: CoordinationConfig{
			LeaseTTL:                  defaultLeaseTTL.String(),
			HeartbeatInterval:         defaultHeartbeatInterval.String(),
			RebalanceDelay:            defaultRebalanceDelay.String(),
			MaintenanceMaxConcurrency: defaultMaintenanceMaxConcurrency,
			FenceInterval:             defaultFenceInterval.String(),
			ReplicationReadTimeout:    defaultReplicationReadTimeout.String(),
		},
	}
}

// validate checks required fields.
func validate(cfg *Config) error {
	if cfg.Storage.Bucket == "" {
		return fmt.Errorf("storage.bucket is required")
	}
	return nil
}
