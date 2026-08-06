package iceberg

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

const DataPrefix = "parquet/"

// BucketDateHour splits a timestamp into the dt/hour bucket strings used for
// path layout. Callers MUST pass the segment-flush (ingest) time of the
// partition leader, NOT record event time — see ExportObjectKey.
func BucketDateHour(ts time.Time) (string, string) {
	utc := ts.UTC()
	return utc.Format("2006-01-02"), utc.Format("15")
}

// ExportObjectKey returns the canonical content-addressed S3 key for an
// exported Parquet data file.
//
// ingestTime MUST be the partition leader's segment-flush wall-clock time, NOT
// the event timestamp of any record. Using ingest time keeps late-arriving
// records out of "past" buckets.
//
// sourceIdentity must identify the immutable native segment being exported
// (normally log.SegmentRef.Key plus its leader epoch). It is required: without
// it, divergent leaders that reuse offsets could address the same object.
func ExportObjectKey(topic string, partition int, ingestTime time.Time, baseOffset, endOffset int64, schemaVersion int, sourceIdentity string) string {
	dt, hour := BucketDateHour(ingestTime)
	id := exportFileID(topic, partition, dt, hour, baseOffset, endOffset, schemaVersion, sourceIdentity)
	return fmt.Sprintf("%sdt=%s/topic=%s/hour=%s/%s.parquet", DataPrefix, dt, topic, hour, id)
}

func exportFileID(topic string, partition int, date, hour string, baseOffset, endOffset int64, schemaVersion int, sourceIdentity string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s|%s|%d|%d|%d|%s", topic, partition, date, hour, baseOffset, endOffset, schemaVersion, sourceIdentity)))
	return hex.EncodeToString(sum[:16])
}
