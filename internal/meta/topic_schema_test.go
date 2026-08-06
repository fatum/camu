package meta

import "testing"

func TestTopicSchemaValidate(t *testing.T) {
	s := TopicSchema{Encoding: "json", Fields: []SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	avro := TopicSchema{Encoding: "avro", Fields: s.Fields}
	if err := avro.Validate(); err != nil {
		t.Fatalf("avro encoding should be valid: %v", err)
	}
	for _, bad := range []TopicSchema{
		{Encoding: "protobuf", Fields: s.Fields},
		{Encoding: "json", Fields: []SchemaField{{Name: "id", Type: "bytes", Path: "$.id"}}},
		{Encoding: "json", Fields: []SchemaField{{Name: "id", Type: "int64", Path: "id"}}},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("expected invalid schema: %+v", bad)
		}
	}
}
