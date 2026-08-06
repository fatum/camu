//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
	"github.com/maksim/camu/pkg/camutest"
)

// testIcebergObjectStore adapts the integration S3 client to the
// iceberg.ObjectStore interface, translating storage errors into the iceberg
// package's own sentinels.
type testIcebergObjectStore struct{ c *storage.S3Client }

func (a testIcebergObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := a.c.Get(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, errors.Join(err, iceberg.ErrNotFound)
	}
	return data, err
}
func (a testIcebergObjectStore) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := a.c.GetWithETag(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, "", errors.Join(err, iceberg.ErrNotFound)
	}
	return data, etag, err
}
func (a testIcebergObjectStore) ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error) {
	newETag, err := a.c.ConditionalPut(ctx, key, data, etag)
	if errors.Is(err, storage.ErrConflict) {
		return "", errors.Join(err, iceberg.ErrConflict)
	}
	return newETag, err
}
func (a testIcebergObjectStore) Delete(ctx context.Context, key string) error {
	return a.c.Delete(ctx, key)
}
func (a testIcebergObjectStore) List(ctx context.Context, prefix string) ([]string, error) {
	return a.c.List(ctx, prefix)
}
func (a testIcebergObjectStore) ListEach(ctx context.Context, prefix string, fn func(string) error) error {
	return a.c.ListEach(ctx, prefix, fn)
}
func (a testIcebergObjectStore) DeleteMany(ctx context.Context, keys []string) error {
	return a.c.DeleteMany(ctx, keys)
}

// TestIntegrationIcebergExportRoundTrip produces records into a diskless
// topic, lets the real partition-leader export consumer commit them as a
// self-managed Iceberg table, and reads the table back through the iceberg
// package: the current snapshot's manifest must reference every exported data
// file, and the data files must exist in the object store.
func TestIntegrationIcebergExportRoundTrip(t *testing.T) {
	env := camutest.New(t,
		camutest.WithInstances(1),
		camutest.WithConfigMutator(func(cfg *config.Config) {
			cfg.Coordination.HeartbeatInterval = "100ms"
		}),
	)
	defer env.Cleanup()

	ctx := context.Background()
	client := env.Client()
	topic := "iceberg-e2e"
	createDisklessTopic(t, client, topic, 1)

	produced := []camutest.ProduceMessage{
		{Key: "k1", Value: "alpha"},
		{Key: "k2", Value: "beta"},
		{Key: "k3", Value: "gamma"},
	}
	if _, err := client.Produce(topic, produced); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	tc := meta.TopicConfig{
		Name: topic, Partitions: 1, Retention: 24 * time.Hour,
		CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1,
		StorageMode: meta.StorageModeDiskless, ExportEnabled: true,
	}
	table := iceberg.NewTableStore(testIcebergObjectStore{c: env.S3Client()}, iceberg.NoFencer{}, "warehouse/")

	var totalRecords int64
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		env.Server(0).RunPartitionMaintenanceForTest([]meta.TopicConfig{tc})
		files, err := table.CurrentDataFiles(ctx, topic)
		if err == nil {
			totalRecords = 0
			for _, f := range files {
				totalRecords += f.RecordCount
			}
			if totalRecords == int64(len(produced)) {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	if totalRecords != int64(len(produced)) {
		t.Fatalf("exported record count = %d, want %d", totalRecords, len(produced))
	}

	files, err := table.CurrentDataFiles(ctx, topic)
	if err != nil {
		t.Fatalf("CurrentDataFiles: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no data files referenced by the current snapshot")
	}
	seen := make(map[string]bool, len(files))
	for _, f := range files {
		if _, err := env.S3Client().Get(ctx, f.FilePath); err != nil {
			t.Fatalf("data file %s missing: %v", f.FilePath, err)
		}
		seen[f.FilePath] = true
	}
	// A re-run of the maintenance pass must not duplicate files (the snapshot
	// commit is idempotent for the same file set).
	env.Server(0).RunPartitionMaintenanceForTest([]meta.TopicConfig{tc})
	again, err := table.CurrentDataFiles(ctx, topic)
	if err != nil {
		t.Fatalf("CurrentDataFiles after re-run: %v", err)
	}
	for _, f := range again {
		if !seen[f.FilePath] {
			t.Fatalf("data file %s appeared after idempotent re-run", f.FilePath)
		}
	}

	// The iceberg-export checkpoint must be durable at the produced head.
	cpData, err := env.S3Client().Get(ctx, "_meta/pipelines/iceberg-export/"+topic+"/0.json")
	if err != nil {
		t.Fatalf("read iceberg checkpoint: %v", err)
	}
	var cp struct {
		NextOffset uint64 `json:"next_offset"`
	}
	if err := json.Unmarshal(cpData, &cp); err != nil {
		t.Fatalf("decode iceberg checkpoint: %v", err)
	}
	if cp.NextOffset != uint64(len(produced)) {
		t.Fatalf("checkpoint next offset = %d, want %d", cp.NextOffset, len(produced))
	}
}
