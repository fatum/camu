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
	Topic   string            `json:"topic"`
	Offsets map[string]uint64 `json:"offsets"` // partition (as string) -> offset
}

// CommitGroup stores offsets for a consumer group.
// Path: _coordination/groups/{groupID}/offsets.json
func (o *OffsetStore) CommitGroup(ctx context.Context, groupID, topic string, offsets map[int]uint64) error {
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	return o.commitOffsets(ctx, key, topic, offsets)
}

// GetGroup reads offsets for a consumer group.
func (o *OffsetStore) GetGroup(ctx context.Context, groupID, topic string) (map[int]uint64, error) {
	key := fmt.Sprintf("_coordination/groups/%s/offsets.json", groupID)
	return o.getOffsets(ctx, key, topic)
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
	return o.getOffsets(ctx, key, topic)
}

func (o *OffsetStore) commitOffsets(ctx context.Context, key, topic string, offsets map[int]uint64) error {
	strOffsets := make(map[string]uint64, len(offsets))
	for k, v := range offsets {
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

func (o *OffsetStore) getOffsets(ctx context.Context, key, topic string) (map[int]uint64, error) {
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
