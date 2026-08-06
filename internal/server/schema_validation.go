package server

import (
	"encoding/base64"
	"fmt"

	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/meta"
)

func validateTypedValue(schema *meta.TopicSchema, value string) error {
	if schema == nil {
		return nil
	}
	raw, err := typedValueBytes(schema, value)
	if err != nil {
		return err
	}
	_, err = iceberg.DecodeTypedFields(schema, raw)
	return err
}

// typedValueBytes returns the raw bytes a topic value should be stored as.
// JSON schema values are the request string verbatim; avro schema values are
// base64-encoded in the HTTP body and decoded to raw Avro bytes before storage
// (the Kafka path already carries raw bytes).
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
