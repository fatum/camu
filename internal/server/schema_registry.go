package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

// The embedded schema registry stores versioned topic schemas so Avro values
// carrying the Confluent-style schema-id envelope can be decoded against their
// own writer schema (read-side evolution). One index object lists the version
// ids of a topic; each version is an immutable object.
const (
	schemaRegistryPrefix = "_meta/schemas/"
	schemaRegistryIndex  = "registry.json"
)

// schemaRegistry stores versioned topic schemas in the object store.
type schemaRegistry struct {
	s3 *storage.S3Client
}

// schemaIndex lists the registered schema ids for a topic, oldest first.
type schemaIndex struct {
	IDs []int `json:"ids"`
}

func (r *schemaRegistry) indexKey(topic string) string {
	return schemaRegistryPrefix + sanitizeSchemaTopic(topic) + "/" + schemaRegistryIndex
}

func (r *schemaRegistry) schemaKey(topic string, id int) string {
	return schemaRegistryPrefix + sanitizeSchemaTopic(topic) + "/" + strconv.Itoa(id) + ".json"
}

func sanitizeSchemaTopic(topic string) string {
	return strings.ReplaceAll(topic, "/", "_")
}

// RegisterTopicSchema registers the initial (version 0) schema for a topic and
// returns its id. No-op (returns 0) when the topic has no schema or the schema
// is already registered (idempotent).
func (r *schemaRegistry) RegisterTopicSchema(ctx context.Context, topic string, schema *meta.TopicSchema) (int, error) {
	if schema == nil {
		return 0, nil
	}
	schema = cloneTopicSchema(schema)
	schema.Version = 0
	encoded, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	if _, err := r.s3.ConditionalPut(ctx, r.schemaKey(topic, 0), encoded, ""); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return 0, nil // already registered
		}
		return 0, fmt.Errorf("register topic schema %q: %w", topic, err)
	}
	index := schemaIndex{IDs: []int{0}}
	idxEncoded, err := json.Marshal(index)
	if err != nil {
		return 0, err
	}
	if _, err := r.s3.ConditionalPut(ctx, r.indexKey(topic), idxEncoded, ""); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return 0, nil // a concurrent registration won
		}
		return 0, fmt.Errorf("register topic schema index %q: %w", topic, err)
	}
	return 0, nil
}

// RegisterSchemaVersion appends a new version of a topic's schema after
// checking backward compatibility (the new projection must read every prior
// version), and returns the new version id. It fails if the topic has no
// registered schema or the encoding differs.
func (r *schemaRegistry) RegisterSchemaVersion(ctx context.Context, topic string, schema *meta.TopicSchema) (int, error) {
	indexData, err := r.s3.Get(ctx, r.indexKey(topic))
	if errors.Is(err, storage.ErrNotFound) {
		return 0, fmt.Errorf("topic %q has no registered schema", topic)
	}
	if err != nil {
		return 0, fmt.Errorf("read schema registry %q: %w", topic, err)
	}
	var index schemaIndex
	if err := json.Unmarshal(indexData, &index); err != nil {
		return 0, fmt.Errorf("parse schema registry %q: %w", topic, err)
	}
	if len(index.IDs) == 0 {
		return 0, fmt.Errorf("topic %q has no registered schema", topic)
	}
	prior := make([]*meta.TopicSchema, 0, len(index.IDs))
	for _, id := range index.IDs {
		ps, err := r.SchemaForID(ctx, topic, id)
		if err != nil {
			return 0, err
		}
		prior = append(prior, ps)
	}
	if err := checkBackwardCompatible(prior, schema); err != nil {
		return 0, err
	}
	nextID := index.IDs[len(index.IDs)-1] + 1
	schema = cloneTopicSchema(schema)
	schema.Version = nextID
	encoded, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	if _, err := r.s3.ConditionalPut(ctx, r.schemaKey(topic, nextID), encoded, ""); err != nil {
		return 0, fmt.Errorf("write schema version %d for %q: %w", nextID, topic, err)
	}
	newIndex := schemaIndex{IDs: append(append([]int(nil), index.IDs...), nextID)}
	idxEncoded, err := json.Marshal(newIndex)
	if err != nil {
		return 0, err
	}
	_, hintETag, err := r.s3.GetWithETag(ctx, r.indexKey(topic))
	if err != nil {
		return 0, fmt.Errorf("read schema registry %q: %w", topic, err)
	}
	if _, err := r.s3.ConditionalPut(ctx, r.indexKey(topic), idxEncoded, hintETag); err != nil {
		return 0, fmt.Errorf("update schema registry %q: %w", topic, err)
	}
	return nextID, nil
}

// SchemaForID implements iceberg.SchemaResolver.
func (r *schemaRegistry) SchemaForID(ctx context.Context, topic string, id int) (*meta.TopicSchema, error) {
	data, err := r.s3.Get(ctx, r.schemaKey(topic, id))
	if err != nil {
		return nil, fmt.Errorf("read schema %q version %d: %w", topic, id, err)
	}
	var schema meta.TopicSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("parse schema %q version %d: %w", topic, id, err)
	}
	return &schema, nil
}

// checkBackwardCompatible verifies that new can read values written under every
// prior schema: fields are only added (never removed or retyped) and required
// fields may only relax to nullable.
func checkBackwardCompatible(prior []*meta.TopicSchema, new *meta.TopicSchema) error {
	if len(prior) == 0 {
		return nil
	}
	if new.Encoding != prior[0].Encoding {
		return fmt.Errorf("schema encoding cannot change from %q to %q", prior[0].Encoding, new.Encoding)
	}
	for _, p := range prior {
		for _, old := range p.Fields {
			found := false
			for _, cur := range new.Fields {
				if cur.Name != old.Name {
					continue
				}
				if cur.Type != old.Type {
					return fmt.Errorf("schema field %q type changed from %q to %q", old.Name, old.Type, cur.Type)
				}
				if old.Nullable && !cur.Nullable {
					return fmt.Errorf("schema field %q cannot become required after being nullable", old.Name)
				}
				found = true
				break
			}
			if !found {
				return fmt.Errorf("schema field %q was removed", old.Name)
			}
		}
	}
	return nil
}

func cloneTopicSchema(s *meta.TopicSchema) *meta.TopicSchema {
	c := *s
	c.Fields = append([]meta.SchemaField(nil), s.Fields...)
	return &c
}
