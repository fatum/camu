package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/maksim/camu/internal/idempotency"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/pipeline"
)

// serverPipelineFence binds a pipeline to the current source partition owner.
// It is intentionally evaluated at every sink/checkpoint boundary.
type serverPipelineFence struct{ server *Server }

func (f serverPipelineFence) Fenced(ctx context.Context, topic string, partition int, epoch uint64) bool {
	return f.server.topicDeletionPending(ctx, topic) || !f.server.partitionOwnerIsCurrent(topic, partition, epoch)
}

// partitionOwnerIsCurrent is the non-error form of the owner check used by
// pipeline.Fence and sink implementations.
func (s *Server) partitionOwnerIsCurrent(topic string, partition int, epoch uint64) bool {
	s.assignmentsMu.RLock()
	a, ok := s.myPartitions[topic][partition]
	s.assignmentsMu.RUnlock()
	return ok && a.Owned && a.LeaderEpoch == epoch
}

// serverDLQAppender adapts Camu's normal partition produce path to the generic
// pipeline DLQ sink. Local leaders append directly; remote leaders receive the
// ordinary idempotent produce request. The destination is bound when the
// adapter is constructed; WaitDurable receives source ownership so it can
// fence the source after output durability is confirmed.
type serverDLQAppender struct {
	server              *Server
	destination         string
	destinationEpoch    uint64
	destinationEpochSet bool
	remoteDurable       bool
}

func (a *serverDLQAppender) Append(ctx context.Context, topic string, partition int, producerID, sequence uint64, messages []log.Message) (uint64, bool, error) {
	s := a.server
	identity, err := s.ResolvePartitionIdentity(ctx, topic, partition)
	if err == nil && identity.Role == PartitionRoleLeader {
		return a.appendLocal(ctx, topic, partition, producerID, sequence, messages, identity)
	}
	return a.appendRemote(ctx, topic, partition, producerID, sequence, messages)
}

func (a *serverDLQAppender) appendLocal(ctx context.Context, topic string, partition int, producerID, sequence uint64, messages []log.Message, identity PartitionIdentity) (uint64, bool, error) {
	s := a.server
	a.remoteDurable = false
	ps := s.partitionManager.GetPartitionState(topic, partition)
	if ps == nil {
		return 0, false, fmt.Errorf("dead-letter partition %s/%d unavailable", topic, partition)
	}
	// Re-check deletion immediately before touching the destination log. A
	// deletion marker may have appeared after resolving the assignment.
	if s.topicDeletionPending(ctx, a.destination) || !s.CanRunOwnerJob(topic, partition, identity.Leader, identity.LeaderEpoch) {
		return 0, false, pipeline.ErrFenced
	}
	a.destinationEpoch = identity.LeaderEpoch
	a.destinationEpochSet = true
	offsets, err := s.partitionManager.appendBatchWithMetaToPS(ps, topic, partition, log.Batch{ProducerID: producerID, Sequence: sequence, Messages: messages}, &IdempotencyOpts{Sequence: sequence})
	if errors.Is(err, idempotency.ErrDuplicateSequence) {
		ps.mu.RLock()
		last, ok := ps.getLastOffset(producerID)
		ps.mu.RUnlock()
		if !ok {
			return 0, true, fmt.Errorf("dead-letter duplicate has no committed offset")
		}
		if s.topicDeletionPending(ctx, a.destination) || !s.CanRunOwnerJob(topic, partition, identity.Leader, identity.LeaderEpoch) {
			return 0, true, pipeline.ErrFenced
		}
		return last, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(offsets) == 0 {
		return 0, false, errors.New("dead-letter append returned no offsets")
	}
	if !s.CanRunOwnerJob(topic, partition, identity.Leader, identity.LeaderEpoch) {
		return 0, false, pipeline.ErrFenced
	}
	return offsets[len(offsets)-1], false, nil
}

// appendRemote uses Camu's ordinary partition-specific produce API. The
// endpoint performs the same leader validation, forwarding, idempotency, and
// replication wait as an external producer; the exporter does not access a
// remote partition manager or use a DLQ-specific RPC.
func (a *serverDLQAppender) appendRemote(ctx context.Context, topic string, partition int, producerID, sequence uint64, messages []log.Message) (uint64, bool, error) {
	s := a.server
	leaderAddr := s.leaderInternalAddr(topic, partition)
	if leaderAddr == "" {
		return 0, false, fmt.Errorf("dead-letter partition %s/%d has no reachable leader", topic, partition)
	}
	request := produceBatchRequest{ProducerID: producerID, Sequence: sequence, Messages: make([]produceMessageRequest, len(messages))}
	for i, message := range messages {
		request.Messages[i] = produceMessageRequest{Key: string(message.Key), Value: string(message.Value), Headers: message.Headers}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return 0, false, fmt.Errorf("encode dead-letter produce request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+leaderAddr+"/v1/topics/"+topic+"/partitions/"+strconv.Itoa(partition)+"/messages", bytes.NewReader(body))
	if err != nil {
		return 0, false, fmt.Errorf("create dead-letter produce request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.internalClient.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("produce dead-letter messages: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("produce dead-letter messages: %s", resp.Status)
	}
	var result struct {
		Duplicate bool         `json:"duplicate"`
		Offsets   []offsetInfo `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, false, fmt.Errorf("decode dead-letter produce response: %w", err)
	}
	if len(result.Offsets) != 1 {
		return 0, false, fmt.Errorf("dead-letter produce returned %d offsets, want 1", len(result.Offsets))
	}
	if result.Offsets[0].Partition != partition {
		return 0, false, fmt.Errorf("dead-letter produce returned partition %d, want %d", result.Offsets[0].Partition, partition)
	}
	// A successful normal produce response is already replication-durable,
	// including a duplicate response (which waits for the original batch).
	a.remoteDurable = true
	return result.Offsets[0].Offset, result.Duplicate, nil
}

func (a *serverDLQAppender) WaitDurable(ctx context.Context, sourceTopic string, partition int, sourceEpoch, offset uint64) error {
	if a.remoteDurable {
		if (serverPipelineFence{server: a.server}).Fenced(ctx, sourceTopic, partition, sourceEpoch) {
			return pipeline.ErrFenced
		}
		return nil
	}
	ps := a.server.partitionManager.GetPartitionState(a.destination, partition)
	if ps == nil {
		return fmt.Errorf("dead-letter partition %s/%d unavailable", a.destination, partition)
	}
	if err := waitForReplicatedOffsetFn(ctx, ps, offset, a.server.replicationTimeout); err != nil {
		return err
	}
	destinationIdentity, err := a.server.ResolvePartitionIdentity(ctx, a.destination, partition)
	if err != nil || destinationIdentity.Role != PartitionRoleLeader ||
		(a.destinationEpochSet && destinationIdentity.LeaderEpoch != a.destinationEpoch) ||
		!a.server.CanRunOwnerJob(a.destination, partition, destinationIdentity.Leader, destinationIdentity.LeaderEpoch) ||
		a.server.topicDeletionPending(ctx, a.destination) {
		return pipeline.ErrFenced
	}
	if (serverPipelineFence{server: a.server}).Fenced(ctx, sourceTopic, partition, sourceEpoch) {
		return pipeline.ErrFenced
	}
	return nil
}
