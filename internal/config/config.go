package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the camu server.
type Config struct {
	Server       ServerConfig      `yaml:"server"`
	Storage      StorageConfig     `yaml:"storage"`
	WAL          WALConfig         `yaml:"wal"`
	Segments     SegmentsConfig    `yaml:"segments"`
	Cache        CacheConfig       `yaml:"cache"`
	Coordination CoordinationConfig `yaml:"coordination"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Address    string `yaml:"address"`
	InstanceID string `yaml:"instance_id"`
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

// WALConfig holds write-ahead log settings.
type WALConfig struct {
	Directory string `yaml:"directory"`
	Fsync     bool   `yaml:"fsync"`
}

// SegmentsConfig holds segment management settings.
type SegmentsConfig struct {
	MaxSize     int64  `yaml:"max_size"`
	MaxAge      string `yaml:"max_age"`
	Compression string `yaml:"compression"`
}

// MaxAgeDuration parses MaxAge as a time.Duration.
func (s SegmentsConfig) MaxAgeDuration() (time.Duration, error) {
	return time.ParseDuration(s.MaxAge)
}

// CacheConfig holds disk cache settings.
type CacheConfig struct {
	Directory string `yaml:"directory"`
	MaxSize   int64  `yaml:"max_size"`
}

// CoordinationConfig holds distributed coordination settings.
type CoordinationConfig struct {
	LeaseTTL          string `yaml:"lease_ttl"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`
	RebalanceDelay    string `yaml:"rebalance_delay"`
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

	if err := applyDefaults(cfg); err != nil {
		return nil, err
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
			Address: ":8080",
		},
		WAL: WALConfig{
			Directory: "/var/lib/camu/wal",
			Fsync:     true,
		},
		Segments: SegmentsConfig{
			MaxSize:     8388608,
			MaxAge:      "5s",
			Compression: "none",
		},
		Cache: CacheConfig{
			Directory: "/var/lib/camu/cache",
			MaxSize:   10737418240,
		},
		Coordination: CoordinationConfig{
			LeaseTTL:          "10s",
			HeartbeatInterval: "3s",
			RebalanceDelay:    "5s",
		},
	}
}

// applyDefaults fills in any zero-value fields that should have defaults.
// Since yaml.Unmarshal overwrites the whole struct, we rely on defaults() being
// called before Unmarshal. This function handles cases where yaml.Unmarshal
// would leave fields as zero values when the key is absent.
func applyDefaults(cfg *Config) error {
	// yaml.v3 sets bool fields to false when absent, but our default is true for Fsync.
	// We cannot distinguish "absent" from "explicitly false" after Unmarshal.
	// To properly handle this, we use a two-pass approach: unmarshal into a raw map
	// to check which keys were actually set, but that complicates the code significantly.
	// Instead, we accept this limitation: if users set fsync: false explicitly, it will
	// be overridden back to true by applyDefaults. This is a known tradeoff with simple
	// YAML unmarshaling without custom logic.
	//
	// The current implementation uses defaults() before Unmarshal, which means yaml.Unmarshal
	// DOES overwrite fields — including bool fields set to false. So defaults work correctly
	// for fields absent from YAML (they stay at default), but explicitly false booleans also work
	// because yaml.Unmarshal sets them to false, which is the intended value.
	//
	// The only problematic scenario is: default=true, user wants false — yaml sets it to false,
	// which is correct. default=false, user wants true — yaml sets it to true, correct.
	// So the approach works for all bool cases.
	return nil
}

// validate checks required fields.
func validate(cfg *Config) error {
	if cfg.Storage.Bucket == "" {
		return fmt.Errorf("storage.bucket is required")
	}
	return nil
}
