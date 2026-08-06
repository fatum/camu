package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
// is already registered (idempotent). It also heals the partial state a crash
// between the object write and the index write would leave: an existing object
// with a missing index is completed, never orphaned.
func (r *schemaRegistry) RegisterTopicSchema(ctx context.Context, topic string, schema *meta.TopicSchema) (int, error) {
	if schema == nil {
		return 0, nil
	}
	schema = meta.CloneSchema(schema)
	schema.Version = 0
	encoded, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	if _, err := r.s3.ConditionalPut(ctx, r.schemaKey(topic, 0), encoded, ""); err != nil {
		if !errors.Is(err, storage.ErrConflict) {
			return 0, fmt.Errorf("register topic schema %q: %w", topic, err)
		}
	}
	idxEncoded, err := json.Marshal(schemaIndex{IDs: []int{0}})
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

// errSchemaVersionTaken reports a concurrent registration that committed a
// different schema at the version this writer attempted; the caller reloads
// and retries at the next free version.
var errSchemaVersionTaken = errors.New("schema version taken by a concurrent registration")

// maxSchemaRegistrationAttempts bounds RegisterSchemaVersion's retry loop.
const maxSchemaRegistrationAttempts = 8

// RegisterSchemaVersion appends a new version of a topic's schema after
// checking backward compatibility (the new projection must read every prior
// version), and returns the new version id. It fails if the topic has no
// registered schema or the encoding differs.
//
// The write is idempotent and self-healing: a transient failure between the
// object write and the index commit leaves an object that the retry re-claims
// (the version id is deterministic from the index), and an unreferenced
// crash-orphan at the computed version is atomically replaced. Concurrent
// registrations of the same version with identical content both succeed.
func (r *schemaRegistry) RegisterSchemaVersion(ctx context.Context, topic string, schema *meta.TopicSchema) (int, error) {
	for attempt := 0; attempt < maxSchemaRegistrationAttempts; attempt++ {
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
		registered := meta.CloneSchema(schema)
		registered.Version = nextID
		encoded, err := json.Marshal(registered)
		if err != nil {
			return 0, err
		}
		if err := r.writeSchemaObject(ctx, topic, nextID, encoded); err != nil {
			if errors.Is(err, errSchemaVersionTaken) {
				continue
			}
			return 0, err
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
			if errors.Is(err, storage.ErrConflict) {
				if r.indexContains(ctx, topic, nextID) {
					return nextID, nil // a concurrent identical registration won
				}
				continue
			}
			return 0, fmt.Errorf("update schema registry %q: %w", topic, err)
		}
		return nextID, nil
	}
	return 0, fmt.Errorf("register schema version %q: conflict after %d attempts", topic, maxSchemaRegistrationAttempts)
}

// writeSchemaObject writes the immutable schema object for a version. A
// conflict means the object already exists: identical content is an idempotent
// success, an unreferenced crash-orphan is atomically replaced (the version is
// not in the index), and a version already committed with different content is
// reported via errSchemaVersionTaken.
func (r *schemaRegistry) writeSchemaObject(ctx context.Context, topic string, version int, encoded []byte) error {
	key := r.schemaKey(topic, version)
	if _, err := r.s3.ConditionalPut(ctx, key, encoded, ""); err == nil {
		return nil
	} else if !errors.Is(err, storage.ErrConflict) {
		return fmt.Errorf("write schema version %d for %q: %w", version, topic, err)
	}
	existing, err := r.s3.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read schema version %d for %q: %w", version, topic, err)
	}
	if bytes.Equal(existing, encoded) {
		return nil // already written, identical
	}
	if r.indexContains(ctx, topic, version) {
		return fmt.Errorf("%w: version %d for %q", errSchemaVersionTaken, version, topic)
	}
	for {
		_, eTag, err := r.s3.GetWithETag(ctx, key)
		if err != nil {
			return fmt.Errorf("read schema version %d for %q: %w", version, topic, err)
		}
		if _, err := r.s3.ConditionalPut(ctx, key, encoded, eTag); err != nil {
			if errors.Is(err, storage.ErrConflict) {
				continue // a concurrent writer moved the object; retry
			}
			return fmt.Errorf("write schema version %d for %q: %w", version, topic, err)
		}
		return nil
	}
}

// indexContains reports whether the topic's current index lists the version.
func (r *schemaRegistry) indexContains(ctx context.Context, topic string, version int) bool {
	data, err := r.s3.Get(ctx, r.indexKey(topic))
	if err != nil {
		return false
	}
	var index schemaIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return false
	}
	for _, id := range index.IDs {
		if id == version {
			return true
		}
	}
	return false
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

// DeleteTopicSchemas removes every registered schema object and the index for
// a topic. Called when the topic is deleted so registry state never outlives
// the topic.
func (r *schemaRegistry) DeleteTopicSchemas(ctx context.Context, topic string) error {
	keys, err := r.s3.List(ctx, schemaRegistryPrefix+sanitizeSchemaTopic(topic)+"/")
	if err != nil {
		return fmt.Errorf("list schemas for %q: %w", topic, err)
	}
	if len(keys) == 0 {
		return nil
	}
	if err := r.s3.DeleteMany(ctx, keys); err != nil {
		return fmt.Errorf("delete schemas for %q: %w", topic, err)
	}
	return nil
}

// GCUnreferencedSchemas deletes schema objects that are not referenced by any
// topic's index (crash-orphaned registrations). It is invoked periodically by
// the leader's coordination GC and is safe to run concurrently with
// registrations: a version that is mid-registration is either already indexed
// or about to be, so only objects that are permanently unreachable are removed.
func (r *schemaRegistry) GCUnreferencedSchemas(ctx context.Context) {
	keys, err := r.s3.List(ctx, schemaRegistryPrefix)
	if err != nil {
		slog.Warn("schema_registry_gc: list", "error", err)
		return
	}
	referenced := make(map[string]bool, len(keys))
	var objects []string
	for _, key := range keys {
		if !strings.HasSuffix(key, schemaRegistryIndex) {
			objects = append(objects, key)
			continue
		}
		data, err := r.s3.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			slog.Warn("schema_registry_gc: read index", "key", key, "error", err)
			continue
		}
		var index schemaIndex
		if err := json.Unmarshal(data, &index); err != nil {
			slog.Warn("schema_registry_gc: parse index", "key", key, "error", err)
			continue
		}
		dir := strings.TrimSuffix(key, schemaRegistryIndex)
		for _, id := range index.IDs {
			referenced[dir+strconv.Itoa(id)+".json"] = true
		}
	}
	var orphans []string
	for _, key := range objects {
		if !referenced[key] {
			orphans = append(orphans, key)
		}
	}
	if len(orphans) == 0 {
		return
	}
	if err := r.s3.DeleteMany(ctx, orphans); err != nil {
		slog.Warn("schema_registry_gc: delete", "error", err)
	}
}
