package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// OffsetStore persists consumer group and standalone consumer offsets in S3.
type OffsetStore struct {
	s3Client *S3Client
}

// NewOffsetStore creates a new OffsetStore.
func NewOffsetStore(s3 *S3Client) *OffsetStore {
	return &OffsetStore{s3Client: s3}
}

// offsetData is the JSON structure stored in S3.
type offsetData struct {
	Topic           string                       `json:"topic"`
	Offsets         map[string]uint64            `json:"offsets"` // legacy partition-only offsets
	Topics          map[string]map[string]uint64 `json:"topics,omitempty"`
	ControllerEpoch string                       `json:"controller_epoch,omitempty"`
}

// CommitGroup stores offsets for a consumer group.
// Path: _coordination/groups/{groupID}/offsets.json
func (o *OffsetStore) CommitGroup(ctx context.Context, groupID, topic string, offsets map[int]uint64) error {
	if topic != "" {
		return o.CommitGroupTopics(ctx, groupID, map[string]map[int]uint64{topic: offsets})
	}
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	return o.commitOffsets(ctx, key, topic, offsets)
}

// GetGroup reads offsets for a consumer group.
func (o *OffsetStore) GetGroup(ctx context.Context, groupID, topic string) (map[int]uint64, error) {
	if topic != "" {
		topics, err := o.GetGroupTopics(ctx, groupID)
		if err != nil {
			return nil, err
		}
		return cloneOffsets(topics[topic]), nil
	}
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	return o.getOffsets(ctx, key)
}

// CommitGroupTopics stores topic-partition offsets for a consumer group.
func (o *OffsetStore) CommitGroupTopics(ctx context.Context, groupID string, offsets map[string]map[int]uint64) error {
	return o.CommitGroupTopicsWithEpoch(ctx, groupID, offsets, "")
}

// CommitGroupTopicsWithEpoch stores topic-partition offsets for a consumer group with controller fencing metadata.
func (o *OffsetStore) CommitGroupTopicsWithEpoch(ctx context.Context, groupID string, offsets map[string]map[int]uint64, controllerEpoch string) error {
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	return o.commitTopicOffsets(ctx, key, offsets, controllerEpoch)
}

// GetGroupTopics reads topic-partition offsets for a consumer group.
func (o *OffsetStore) GetGroupTopics(ctx context.Context, groupID string) (map[string]map[int]uint64, error) {
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	return o.getTopicOffsets(ctx, key)
}

// DeleteGroupTopics deletes specific topic-partition offsets for a consumer group.
func (o *OffsetStore) DeleteGroupTopics(ctx context.Context, groupID string, deletes map[string][]int, controllerEpoch string) error {
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	existing, err := o.getTopicOffsets(ctx, key)
	if err != nil {
		return fmt.Errorf("read existing topic offsets: %w", err)
	}

	for topic, partitions := range deletes {
		topicOffsets := existing[topic]
		if topicOffsets == nil {
			continue
		}
		for _, partition := range partitions {
			delete(topicOffsets, partition)
		}
		if len(topicOffsets) == 0 {
			delete(existing, topic)
		}
	}

	strTopics := make(map[string]map[string]uint64, len(existing))
	for topic, partitions := range existing {
		strPartitions := make(map[string]uint64, len(partitions))
		for partition, offset := range partitions {
			strPartitions[fmt.Sprintf("%d", partition)] = offset
		}
		strTopics[topic] = strPartitions
	}

	raw, err := json.Marshal(offsetData{Topics: strTopics, ControllerEpoch: controllerEpoch})
	if err != nil {
		return fmt.Errorf("marshal topic offsets: %w", err)
	}
	return o.s3Client.Put(ctx, key, raw, PutOpts{ContentType: "application/json"})
}

// DeleteGroup removes all stored offsets for a consumer group.
func (o *OffsetStore) DeleteGroup(ctx context.Context, groupID string) error {
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	return o.s3Client.Delete(ctx, key)
}

// CommitConsumer stores offsets for a standalone consumer.
// Path: _coordination/consumers/{consumerID}/offsets.json
func (o *OffsetStore) CommitConsumer(ctx context.Context, consumerID, topic string, offsets map[int]uint64) error {
	key := fmt.Sprintf("_coordination/consumers/%s/offsets.json", consumerID)
	return o.commitOffsets(ctx, key, topic, offsets)
}

// GetConsumer reads offsets for a standalone consumer.
func (o *OffsetStore) GetConsumer(ctx context.Context, consumerID, topic string) (map[int]uint64, error) {
	key := fmt.Sprintf("_coordination/consumers/%s/offsets.json", consumerID)
	return o.getOffsets(ctx, key)
}

func (o *OffsetStore) commitOffsets(ctx context.Context, key, topic string, offsets map[int]uint64) error {
	existing, err := o.getOffsets(ctx, key)
	if err != nil {
		return fmt.Errorf("read existing offsets: %w", err)
	}
	for k, v := range offsets {
		existing[k] = v
	}

	strOffsets := make(map[string]uint64, len(existing))
	for k, v := range existing {
		strOffsets[fmt.Sprintf("%d", k)] = v
	}
	data := offsetData{
		Topic:   topic,
		Offsets: strOffsets,
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal offsets: %w", err)
	}
	return o.s3Client.Put(ctx, key, raw, PutOpts{ContentType: "application/json"})
}

func (o *OffsetStore) getOffsets(ctx context.Context, key string) (map[int]uint64, error) {
	raw, err := o.s3Client.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return make(map[int]uint64), nil
		}
		return nil, fmt.Errorf("get offsets: %w", err)
	}
	var data offsetData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("unmarshal offsets: %w", err)
	}
	result := make(map[int]uint64, len(data.Offsets))
	for k, v := range data.Offsets {
		var pid int
		if _, err := fmt.Sscanf(k, "%d", &pid); err != nil {
			continue
		}
		result[pid] = v
	}
	return result, nil
}

func (o *OffsetStore) commitTopicOffsets(ctx context.Context, key string, offsets map[string]map[int]uint64, controllerEpoch string) error {
	existing, err := o.getTopicOffsets(ctx, key)
	if err != nil {
		return fmt.Errorf("read existing topic offsets: %w", err)
	}
	for topic, partitions := range offsets {
		if existing[topic] == nil {
			existing[topic] = make(map[int]uint64)
		}
		for partition, offset := range partitions {
			existing[topic][partition] = offset
		}
	}

	strTopics := make(map[string]map[string]uint64, len(existing))
	for topic, partitions := range existing {
		strPartitions := make(map[string]uint64, len(partitions))
		for partition, offset := range partitions {
			strPartitions[fmt.Sprintf("%d", partition)] = offset
		}
		strTopics[topic] = strPartitions
	}

	raw, err := json.Marshal(offsetData{Topics: strTopics, ControllerEpoch: controllerEpoch})
	if err != nil {
		return fmt.Errorf("marshal topic offsets: %w", err)
	}
	return o.s3Client.Put(ctx, key, raw, PutOpts{ContentType: "application/json"})
}

func (o *OffsetStore) getTopicOffsets(ctx context.Context, key string) (map[string]map[int]uint64, error) {
	raw, err := o.s3Client.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return make(map[string]map[int]uint64), nil
		}
		return nil, fmt.Errorf("get offsets: %w", err)
	}
	var data offsetData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("unmarshal offsets: %w", err)
	}

	result := make(map[string]map[int]uint64, len(data.Topics))
	for topic, partitions := range data.Topics {
		topicOffsets := make(map[int]uint64, len(partitions))
		for k, v := range partitions {
			var pid int
			if _, err := fmt.Sscanf(k, "%d", &pid); err != nil {
				continue
			}
			topicOffsets[pid] = v
		}
		result[topic] = topicOffsets
	}

	if len(result) == 0 && len(data.Offsets) > 0 && data.Topic != "" {
		result[data.Topic] = make(map[int]uint64, len(data.Offsets))
		for k, v := range data.Offsets {
			var pid int
			if _, err := fmt.Sscanf(k, "%d", &pid); err != nil {
				continue
			}
			result[data.Topic][pid] = v
		}
	}
	return result, nil
}

func cloneOffsets(in map[int]uint64) map[int]uint64 {
	if len(in) == 0 {
		return make(map[int]uint64)
	}
	out := make(map[int]uint64, len(in))
	for partition, offset := range in {
		out[partition] = offset
	}
	return out
}
