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
	SQL          SQLConfig          `yaml:"sql"`
	Coordination CoordinationConfig `yaml:"coordination"`
	Diskless     DisklessConfig     `yaml:"diskless"`
	Maintenance  MaintenanceConfig  `yaml:"maintenance"`
}

// MaintenanceConfig groups partition-leader maintenance knobs.
type MaintenanceConfig struct {
	ParquetExport ParquetExportConfig `yaml:"parquet_export"`
}

// ParquetExportConfig controls the per-partition Parquet export consumer.
type ParquetExportConfig struct {
	MaxRecords  int    `yaml:"max_records"`
	MaxDuration string `yaml:"max_duration"`
}

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

type SQLConfig struct {
	Enabled        *bool  `yaml:"enabled"`
	CacheDirectory string `yaml:"cache_directory"`
	CacheMaxSize   int64  `yaml:"cache_max_size"`
	TempDirectory  string `yaml:"duckdb_temp_directory"`
	MemoryLimit    string `yaml:"duckdb_memory_limit"`
	MaxConcurrency int    `yaml:"max_concurrency"`
	QueryTimeout   string `yaml:"query_timeout"`
	MaxScanFiles   int    `yaml:"max_scan_files"`
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
	Mode                  string `yaml:"mode"`                    // "stream" (default) or "query"
}

const (
	ServerModeStream = "stream"
	ServerModeQuery  = "query"
)

func (s ServerConfig) ModeValue() string {
	if s.Mode == "" {
		return ServerModeStream
	}
	return s.Mode
}

func (s ServerConfig) IsQueryMode() bool {
	return s.ModeValue() == ServerModeQuery
}

func (s ServerConfig) IsStreamMode() bool {
	return s.ModeValue() == ServerModeStream
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
	defaultSQLCacheMaxSize              = 5 * 1024 * 1024 * 1024
	defaultSQLMaxConcurrency            = 4
	defaultSQLCacheDirectory            = "/var/lib/camu/sql-cache"
	defaultSQLTempDirectory             = "/var/lib/camu/sql-tmp"
	defaultSQLQueryTimeout              = 30 * time.Second
	defaultSQLMaxScanFiles              = 4096
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

func (s SQLConfig) CacheMaxSizeValue() int64 {
	if s.CacheMaxSize <= 0 {
		return defaultSQLCacheMaxSize
	}
	return s.CacheMaxSize
}

func (s SQLConfig) CacheDirectoryValue() string {
	if s.CacheDirectory == "" {
		return defaultSQLCacheDirectory
	}
	return s.CacheDirectory
}

func (s SQLConfig) TempDirectoryValue() string {
	if s.TempDirectory == "" {
		return defaultSQLTempDirectory
	}
	return s.TempDirectory
}

func (s SQLConfig) MaxConcurrencyValue() int {
	if s.MaxConcurrency <= 0 {
		return defaultSQLMaxConcurrency
	}
	return s.MaxConcurrency
}

// EnabledValue reports whether the /v1/sql endpoint is enabled. Defaults to
// true in query mode and false in stream mode unless explicitly overridden,
// so that analytical SQL does not land on hot streaming nodes by default.
func (s SQLConfig) EnabledValue(queryMode bool) bool {
	if s.Enabled != nil {
		return *s.Enabled
	}
	return queryMode
}

func (s SQLConfig) QueryTimeoutDuration() (time.Duration, error) {
	return parseDurationOrDefault(s.QueryTimeout, defaultSQLQueryTimeout)
}

func (s SQLConfig) MaxScanFilesValue() int {
	if s.MaxScanFiles <= 0 {
		return defaultSQLMaxScanFiles
	}
	return s.MaxScanFiles
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
			Mode:               ServerModeStream,
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
		SQL: SQLConfig{
			CacheDirectory: "/var/lib/camu/sql-cache",
			CacheMaxSize:   defaultSQLCacheMaxSize,
			TempDirectory:  "/var/lib/camu/sql-tmp",
			MaxConcurrency: defaultSQLMaxConcurrency,
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
	switch cfg.Server.ModeValue() {
	case ServerModeStream, ServerModeQuery:
	default:
		return fmt.Errorf("server.mode must be %q or %q", ServerModeStream, ServerModeQuery)
	}
	if cfg.Server.IsQueryMode() && !cfg.SQL.EnabledValue(true) {
		return fmt.Errorf("sql.enabled must be true in query mode")
	}
	return nil
}
