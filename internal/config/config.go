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
	MaxRecords    int    `yaml:"max_records"`
	MaxDuration   string `yaml:"max_duration"`
	TempDirectory string `yaml:"temp_directory"`
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
func (p ParquetExportConfig) TempDirectoryValue() string {
	if p.TempDirectory == "" {
		return "/var/lib/camu/parquet-tmp"
	}
	return p.TempDirectory
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
	LingerMs      int            `yaml:"linger_ms"`       // Max buffer time before flush (default 250)
	MaxBatchBytes int64          `yaml:"max_batch_bytes"` // Max buffer size before flush (default 8MiB)
	MetaStore     string         `yaml:"metastore"`       // "memory" (default) or "dynamodb"
	DynamoDB      DynamoDBConfig `yaml:"dynamodb"`
}

// DynamoDBConfig holds DynamoDB settings for the diskless MetaStore.
type DynamoDBConfig struct {
	TablePrefix string `yaml:"table_prefix"` // default "camu"
	Region      string `yaml:"region"`
	Endpoint    string `yaml:"endpoint"` // for local DynamoDB
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
	InstanceID            string `yaml:"instance_id"`
	AuthToken             string `yaml:"auth_token"`              // Public API bearer token (optional)
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
	MaintenanceMaxConcurrency int    `yaml:"maintenance_max_concurrency"`
}

const (
	defaultLeaseTTL                  = 30 * time.Second
	defaultHeartbeatInterval         = 10 * time.Second
	defaultRebalanceDelay            = 5 * time.Second
	defaultISRExpansionThreshold     = 1000
	defaultReplicationTimeout        = 30 * time.Second
	defaultMaintenanceMaxConcurrency = 4
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

func (c CoordinationConfig) MaintenanceMaxConcurrencyValue() int {
	if c.MaintenanceMaxConcurrency <= 0 {
		return defaultMaintenanceMaxConcurrency
	}
	return c.MaintenanceMaxConcurrency
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
			Address:         ":8080",
			InternalAddress: ":8081",
			Mode:            ServerModeStream,
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
