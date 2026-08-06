package server

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/meta"
)

// validateTypedValue validates a produce value against its topic schema.
// JSON values are the request string verbatim; avro values are base64-encoded
// and, when wrapped in the schema-id envelope, decoded against the registered
// writer schema.
func (s *Server) validateTypedValue(ctx context.Context, topicCfg meta.TopicConfig, value string) error {
	if topicCfg.Schema == nil {
		return nil
	}
	raw, err := typedValueBytes(topicCfg.Schema, value)
	if err != nil {
		return err
	}
	_, err = iceberg.DecodeTypedFields(ctx, topicCfg.Name, topicCfg.Schema, s.schemaRegistry, raw)
	return err
}

// typedValueBytes returns the raw bytes a topic value should be stored as.
// JSON schema values are the request string verbatim; avro schema values are
// base64-encoded in the HTTP body and decoded to raw bytes before storage (the
// Kafka path already carries raw bytes).
func typedValueBytes(schema *meta.TopicSchema, value string) ([]byte, error) {
	if schema != nil && schema.Encoding == "avro" {
		raw, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("avro value must be base64-encoded: %w", err)
		}
		return raw, nil
	}
	return []byte(value), nil
}
