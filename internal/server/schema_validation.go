package server

import (
	"github.com/maksim/camu/internal/iceberg"
	"github.com/maksim/camu/internal/meta"
)

func validateTypedValue(schema *meta.TopicSchema, value string) error {
	if schema == nil {
		return nil
	}
	_, err := iceberg.DecodeTypedFields(schema, []byte(value))
	return err
}
