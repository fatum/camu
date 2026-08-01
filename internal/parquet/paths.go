package parquet

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	DataPrefix         = "parquet/"
	ManifestPrefix     = "_meta/parquet_manifests/"
	QueryCatalogPrefix = "_meta/query_catalog/topics/"
	BucketIndexPrefix  = "_meta/parquet_buckets/"
)

// BucketDateHour splits a timestamp into the dt/hour bucket strings used
// for path layout. Callers MUST pass the segment-flush (ingest) time of
// the partition leader, NOT record event time — see ExportObjectKey.
func BucketDateHour(ts time.Time) (string, string) {
	utc := ts.UTC()
	return utc.Format("2006-01-02"), utc.Format("15")
}

// ManifestKeyForBucket returns the manifest object key for a specific
// (topic, partition, dt, hour) bucket.
func ManifestKeyForBucket(topic string, partition int, date, hour string) string {
	return fmt.Sprintf("%s%s/dt=%s/hour=%s/part-%d.json", ManifestPrefix, topic, date, hour, partition)
}

// ExportObjectKey returns the canonical content-addressed S3 key for an
// exported Parquet file.
//
// ingestTime MUST be the partition leader's segment-flush wall-clock
// time, NOT the event timestamp of any record. Using ingest time keeps
// late-arriving records out of "past" buckets, lets compaction scan
// "current bucket" without racing retroactive writes, and lets Parquet
// retention run in lockstep with native-log retention.
//
// Record event timestamps should be exposed as a column inside the
// Parquet file for query-time filtering, not as part of the path layout.
//
// The returned filename is an opaque deterministic id rather than a
// human-readable partition/offset path. Kafka mechanics stay in manifest
// metadata; the object layout stays analytics-oriented.
//
// sourceIdentity must identify the immutable native segment being exported
// (normally log.SegmentRef.Key plus its leader epoch). It is required: without
// it, divergent leaders that reuse offsets could address the same object.
func ExportObjectKey(topic string, partition int, ingestTime time.Time, baseOffset, endOffset int64, schemaVersion int, sourceIdentity string) string {
	dt, hour := BucketDateHour(ingestTime)
	id := exportFileID(topic, partition, dt, hour, baseOffset, endOffset, schemaVersion, sourceIdentity)
	return fmt.Sprintf("%sdt=%s/topic=%s/hour=%s/%s.parquet", DataPrefix, dt, topic, hour, id)
}

// ManifestKey returns the manifest key for the bucket containing
// ingestTime. Convenience wrapper around ManifestKeyForBucket.
func ManifestKey(topic string, partition int, ingestTime time.Time) string {
	dt, hour := BucketDateHour(ingestTime)
	return ManifestKeyForBucket(topic, partition, dt, hour)
}

// ManifestBucketPrefix returns the S3 prefix that matches all
// per-partition manifests for one (topic, date, hour) bucket.
func ManifestBucketPrefix(topic, date, hour string) string {
	return fmt.Sprintf("%s%s/dt=%s/hour=%s/", ManifestPrefix, topic, date, hour)
}

// DataBucketPrefix returns the S3 prefix that matches all Parquet data
// files for one (topic, date, hour) bucket, across all partitions.
// Note: data layout is topic-scoped but not partition-scoped — all
// partitions' exports share this prefix.
func DataBucketPrefix(topic, date, hour string) string {
	return fmt.Sprintf("%sdt=%s/topic=%s/hour=%s/", DataPrefix, date, topic, hour)
}

// QueryCatalogTopicKey returns the query-catalog entry key for a topic.
func QueryCatalogTopicKey(topic string) string {
	return fmt.Sprintf("%s%s.json", QueryCatalogPrefix, topic)
}

// ParseManifestKey extracts (date, hour) from a manifest key of the form
// `_meta/parquet_manifests/{topic}/dt=YYYY-MM-DD/hour=HH/part-N.json`.
// Returns ok=false for any key that does not match.
func ParseManifestKey(topic, key string) (string, string, bool) {
	prefix := ManifestPrefix + topic + "/"
	rest, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		return "", "", false
	}
	date, ok := strings.CutPrefix(parts[0], "dt=")
	if !ok {
		return "", "", false
	}
	hour, ok := strings.CutPrefix(parts[1], "hour=")
	if !ok {
		return "", "", false
	}
	return date, hour, true
}

func exportFileID(topic string, partition int, date, hour string, baseOffset, endOffset int64, schemaVersion int, sourceIdentity string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s|%s|%d|%d|%d|%s", topic, partition, date, hour, baseOffset, endOffset, schemaVersion, sourceIdentity)))
	return hex.EncodeToString(sum[:16])
}
