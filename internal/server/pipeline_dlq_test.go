package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/pipeline"
	"github.com/maksim/camu/internal/storage"
)

func newDLQAppenderTestServer(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t)
	tc := meta.TopicConfig{Name: "events-dlq", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1}
	if err := s.topicStore.Create(context.Background(), tc); err != nil {
		t.Fatal(err)
	}
	if err := s.partitionManager.InitTopic(context.Background(), tc, map[int]uint64{0: 1}); err != nil {
		t.Fatal(err)
	}
	s.assignmentsMu.Lock()
	s.myPartitions[tc.Name] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.myPartitions["events"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	s.assignmentsMu.Unlock()
	return s
}

func TestServerDLQAppenderAppendDuplicate(t *testing.T) {
	s := newDLQAppenderTestServer(t)
	a := serverDLQAppender{server: s, destination: "events-dlq"}
	msg := []log.Message{{Value: []byte{0, 1, 2}}}
	last, duplicate, err := a.Append(context.Background(), "events-dlq", 0, 11, 0, msg)
	if err != nil || duplicate || last != 0 {
		t.Fatalf("first append = offset %d duplicate %v err %v", last, duplicate, err)
	}
	last, duplicate, err = a.Append(context.Background(), "events-dlq", 0, 11, 0, msg)
	if err != nil || !duplicate || last != 0 {
		t.Fatalf("retry = offset %d duplicate %v err %v", last, duplicate, err)
	}
}

func TestServerDLQAppenderAppendFencesDeletion(t *testing.T) {
	s := newDLQAppenderTestServer(t)
	ctx := context.Background()
	if err := s.putTopicDeletion(ctx, topicDeletionRecord{Topic: meta.TopicConfig{Name: "events-dlq"}, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	a := serverDLQAppender{server: s, destination: "events-dlq"}
	_, _, err := a.Append(ctx, "events-dlq", 0, 11, 0, []log.Message{{Value: []byte("x")}})
	if !errors.Is(err, pipeline.ErrFenced) {
		t.Fatalf("Append error = %v, want deletion fence", err)
	}
}

func TestServerDLQAppenderWaitDurableFencesSource(t *testing.T) {
	s := newDLQAppenderTestServer(t)
	a := serverDLQAppender{server: s, destination: "events-dlq"}
	original := waitForReplicatedOffsetFn
	t.Cleanup(func() { waitForReplicatedOffsetFn = original })
	waitForReplicatedOffsetFn = func(context.Context, *partitionState, uint64, time.Duration) error { return nil }
	s.assignmentsMu.Lock()
	assignment := s.myPartitions["events"][0]
	assignment.Owned = false
	s.myPartitions["events"][0] = assignment
	s.assignmentsMu.Unlock()
	if err := a.WaitDurable(context.Background(), "events", 0, 1, 0); !errors.Is(err, pipeline.ErrFenced) {
		t.Fatalf("WaitDurable error = %v, want source fence", err)
	}
}

func TestServerDLQAppenderWaitDurablePropagatesFailure(t *testing.T) {
	s := newDLQAppenderTestServer(t)
	a := serverDLQAppender{server: s, destination: "events-dlq"}
	original := waitForReplicatedOffsetFn
	t.Cleanup(func() { waitForReplicatedOffsetFn = original })
	want := errors.New("replication timeout")
	waitForReplicatedOffsetFn = func(context.Context, *partitionState, uint64, time.Duration) error { return want }
	if err := a.WaitDurable(context.Background(), "events", 0, 1, 0); !errors.Is(err, want) {
		t.Fatalf("WaitDurable error = %v, want %v", err, want)
	}
}

func TestServerDLQAppenderWaitDurableFencesEpochZeroReassignment(t *testing.T) {
	s := newDLQAppenderTestServer(t)
	a := serverDLQAppender{server: s, destination: "events-dlq", destinationEpoch: 0, destinationEpochSet: true}
	original := waitForReplicatedOffsetFn
	t.Cleanup(func() { waitForReplicatedOffsetFn = original })
	waitForReplicatedOffsetFn = func(context.Context, *partitionState, uint64, time.Duration) error { return nil }
	s.assignmentsMu.Lock()
	assignment := s.myPartitions["events-dlq"][0]
	assignment.LeaderEpoch = 1
	s.myPartitions["events-dlq"][0] = assignment
	s.assignmentsMu.Unlock()
	if err := a.WaitDurable(context.Background(), "events", 0, 1, 0); !errors.Is(err, pipeline.ErrFenced) {
		t.Fatalf("WaitDurable error = %v, want destination epoch fence", err)
	}
}

func TestServerDLQAppenderRemoteLeaderUsesNormalProduceAndDeduplicatesRetry(t *testing.T) {
	ctx := context.Background()
	source := newTestServer(t)
	destination := cloneTestServerForInstance(t, source, "n2")
	dlq := meta.TopicConfig{Name: "events-dlq", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1}
	if err := source.topicStore.Create(ctx, dlq); err != nil {
		t.Fatal(err)
	}
	if err := source.assignmentStore.Write(ctx, "events", coordination.TopicAssignments{Partitions: map[int]coordination.PartitionAssignment{0: {Leader: source.instanceID, Replicas: []string{source.instanceID}, LeaderEpoch: 1}}, Version: 1}, ""); err != nil {
		t.Fatal(err)
	}
	if err := source.assignmentStore.Write(ctx, dlq.Name, coordination.TopicAssignments{Partitions: map[int]coordination.PartitionAssignment{0: {Leader: destination.instanceID, Replicas: []string{destination.instanceID}, LeaderEpoch: 1}}, Version: 1}, ""); err != nil {
		t.Fatal(err)
	}
	if err := destination.partitionManager.InitTopic(ctx, dlq, map[int]uint64{0: 1}); err != nil {
		t.Fatal(err)
	}
	source.assignmentsMu.Lock()
	source.myPartitions["events"] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	source.assignmentsMu.Unlock()
	destination.assignmentsMu.Lock()
	destination.myPartitions[dlq.Name] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	destination.assignmentsMu.Unlock()

	remote := httptest.NewServer(destination.publicRoutes())
	t.Cleanup(remote.Close)
	source.internalClient = remote.Client()
	info, err := json.Marshal(coordination.InstanceInfo{InstanceID: destination.instanceID, Address: strings.TrimPrefix(remote.URL, "http://"), InternalAddress: strings.TrimPrefix(remote.URL, "http://"), HeartbeatAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.s3Client.Put(ctx, "_coordination/instances/"+destination.instanceID+".json", info, storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	sink := pipeline.NewDLQSink(&serverDLQAppender{server: source, destination: dlq.Name}, "events", dlq.Name, serverPipelineFence{server: source})
	batch := pipeline.Batch{SourceTopic: "events", Partition: 0, SourceEpoch: 1, StartOffset: 7, EndOffset: 7, SinkStartSequence: 0, Messages: []log.Message{{Offset: 7, Key: []byte("key"), Value: []byte("bad"), Timestamp: time.Now().UnixMilli()}}, Error: "invalid schema"}
	first, err := sink.Write(ctx, batch)
	if err != nil {
		t.Fatalf("first remote DLQ write: %v", err)
	}
	retry, err := sink.Write(ctx, batch)
	if err != nil {
		t.Fatalf("retry remote DLQ write: %v", err)
	}
	if retry != first {
		t.Fatalf("retry result = %+v, want %+v", retry, first)
	}
	ps := destination.partitionManager.GetPartitionState(dlq.Name, 0)
	ps.mu.RLock()
	nextOffset := ps.nextOffset
	ps.mu.RUnlock()
	if nextOffset != 1 {
		t.Fatalf("remote DLQ next offset = %d, want 1 after idempotent retry", nextOffset)
	}
}

func TestHandleSchemaDecodeFailuresDeliversToRemoteDLQAndRetriesIdempotently(t *testing.T) {
	ctx := context.Background()
	source := newTestServer(t)
	destination := cloneTestServerForInstance(t, source, "n2")
	events := meta.TopicConfig{
		Name:              "events",
		Partitions:        1,
		Retention:         time.Hour,
		CreatedAt:         time.Now(),
		ReplicationFactor: 1,
		MinInsyncReplicas: 1,
		Schema: &meta.TopicSchema{
			Encoding:        "json",
			DeadLetterTopic: "events-dlq",
			Fields:          []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}},
		},
	}
	dlq := meta.TopicConfig{Name: "events-dlq", Partitions: 1, Retention: time.Hour, CreatedAt: time.Now(), ReplicationFactor: 1, MinInsyncReplicas: 1, StorageMode: meta.StorageModeClassic}
	for _, topic := range []meta.TopicConfig{events, dlq} {
		if err := source.topicStore.Create(ctx, topic); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.assignmentStore.Write(ctx, events.Name, coordination.TopicAssignments{Partitions: map[int]coordination.PartitionAssignment{0: {Leader: source.instanceID, Replicas: []string{source.instanceID}, LeaderEpoch: 1}}, Version: 1}, ""); err != nil {
		t.Fatal(err)
	}
	if err := source.assignmentStore.Write(ctx, dlq.Name, coordination.TopicAssignments{Partitions: map[int]coordination.PartitionAssignment{0: {Leader: destination.instanceID, Replicas: []string{destination.instanceID}, LeaderEpoch: 1}}, Version: 1}, ""); err != nil {
		t.Fatal(err)
	}
	if err := destination.partitionManager.InitTopic(ctx, dlq, map[int]uint64{0: 1}); err != nil {
		t.Fatal(err)
	}
	source.assignmentsMu.Lock()
	source.myPartitions[events.Name] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	source.assignmentsMu.Unlock()
	destination.assignmentsMu.Lock()
	destination.myPartitions[dlq.Name] = map[int]localPartitionAssignment{0: {Owned: true, LeaderEpoch: 1}}
	destination.assignmentsMu.Unlock()

	remote := httptest.NewServer(destination.publicRoutes())
	t.Cleanup(remote.Close)
	source.internalClient = remote.Client()
	info, err := json.Marshal(coordination.InstanceInfo{InstanceID: destination.instanceID, Address: strings.TrimPrefix(remote.URL, "http://"), InternalAddress: strings.TrimPrefix(remote.URL, "http://"), HeartbeatAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.s3Client.Put(ctx, "_coordination/instances/"+destination.instanceID+".json", info, storage.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	identity := PartitionIdentity{Topic: events.Name, Partition: 0, Role: PartitionRoleLeader, Leader: source.instanceID, LeaderEpoch: 1}
	failure := iceberg.SchemaFailure{Message: log.Message{Offset: 0, Key: []byte("key"), Value: []byte(`{"id":"not-an-int"}`)}, Err: validateTypedValue(events.Schema, `{"id":"not-an-int"}`)}
	if err := source.handleSchemaDecodeFailures(ctx, events, identity, []iceberg.SchemaFailure{failure}); err != nil {
		t.Fatalf("initial schema DLQ delivery: %v", err)
	}

	dlqStore := pipeline.NewCheckpointStore(source.s3Client, serverPipelineFence{server: source})
	dlqCP, err := dlqStore.Load(ctx, "schema-dead-letter", events.Name, 0)
	if err != nil {
		t.Fatalf("load schema DLQ checkpoint: %v", err)
	}
	if dlqCP.NextOffset != 1 || dlqCP.OutputStart != 0 || dlqCP.OutputEnd != 0 {
		t.Fatalf("schema DLQ checkpoint = %+v, want first output at offset zero", dlqCP)
	}

	// Simulate a retry after the output was accepted but before its DLQ
	// checkpoint became durable. The deterministic producer sequence must
	// replay the same remote output rather than append another record.
	if err := source.s3Client.Delete(ctx, pipeline.CheckpointKey("schema-dead-letter", events.Name, 0)); err != nil {
		t.Fatal(err)
	}
	if err := source.handleSchemaDecodeFailures(ctx, events, identity, []iceberg.SchemaFailure{failure}); err != nil {
		t.Fatalf("retry schema DLQ delivery: %v", err)
	}
	destinationPS := destination.partitionManager.GetPartitionState(dlq.Name, 0)
	destinationPS.mu.RLock()
	nextOffset := destinationPS.nextOffset
	destinationPS.mu.RUnlock()
	if nextOffset != 1 {
		t.Fatalf("remote DLQ next offset = %d, want 1 after exporter retry", nextOffset)
	}
}
