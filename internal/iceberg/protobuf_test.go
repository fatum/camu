package iceberg

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maksim/camu/internal/meta"
)

func testProtobufSchema() *meta.TopicSchema {
	return &meta.TopicSchema{Encoding: "protobuf", Fields: []meta.SchemaField{
		{Name: "name", Type: "string", Path: "$.name"},
		{Name: "count", Type: "int64", Path: "$.count"},
		{Name: "ratio", Type: "float64", Path: "$.ratio"},
		{Name: "enabled", Type: "bool", Path: "$.enabled"},
		{Name: "note", Type: "string", Path: "$.note", Nullable: true},
	}}
}

func TestDecodeProtobufTypedFieldsRoundTrip(t *testing.T) {
	schema := testProtobufSchema()
	md, err := protobufDescriptor(schema)
	if err != nil {
		t.Fatalf("protobufDescriptor() error = %v", err)
	}
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("alpha"))
	msg.Set(md.Fields().ByName("count"), protoreflect.ValueOfInt64(7))
	msg.Set(md.Fields().ByName("ratio"), protoreflect.ValueOfFloat64(1.5))
	msg.Set(md.Fields().ByName("enabled"), protoreflect.ValueOfBool(true))
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}

	values, err := DecodeProtobufTypedFields(context.Background(), "t", schema, nil, raw)
	if err != nil {
		t.Fatalf("DecodeProtobufTypedFields() error = %v", err)
	}
	if !values[0].Present || values[0].Value.String() != "alpha" {
		t.Fatalf("name = %+v, want alpha", values[0])
	}
	if !values[1].Present || values[1].Value.Int64() != 7 {
		t.Fatalf("count = %+v, want 7", values[1])
	}
	if !values[2].Present || values[2].Value.Double() != 1.5 {
		t.Fatalf("ratio = %+v, want 1.5", values[2])
	}
	if !values[3].Present || !values[3].Value.Boolean() {
		t.Fatalf("enabled = %+v, want true", values[3])
	}
	if values[4].Present {
		t.Fatalf("note = %+v, want absent", values[4])
	}
}

func TestDecodeProtobufTypedFieldsTimestamp(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "protobuf", Fields: []meta.SchemaField{
		{Name: "occurred_at", Type: "timestamp", Path: "$.occurred_at"},
	}}
	md, err := protobufDescriptor(schema)
	if err != nil {
		t.Fatalf("protobufDescriptor() error = %v", err)
	}
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("occurred_at"), protoreflect.ValueOfMessage(timestamppb.New(time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)).ProtoReflect()))
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	values, err := DecodeProtobufTypedFields(context.Background(), "t", schema, nil, raw)
	if err != nil {
		t.Fatalf("DecodeProtobufTypedFields() error = %v", err)
	}
	want := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC).UnixNano()
	if !values[0].Present || values[0].Value.Int64() != want {
		t.Fatalf("occurred_at = %+v, want %d", values[0], want)
	}
}

func TestDecodeProtobufTypedFieldsRejectsGarbage(t *testing.T) {
	schema := testProtobufSchema()
	if _, err := DecodeProtobufTypedFields(context.Background(), "t", schema, nil, []byte("not protobuf")); err == nil {
		t.Fatal("DecodeProtobufTypedFields() error = nil, want decode error")
	}
}

func TestDecodeTypedFieldsDispatchesProtobuf(t *testing.T) {
	schema := &meta.TopicSchema{Encoding: "protobuf", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}}}
	md, err := protobufDescriptor(schema)
	if err != nil {
		t.Fatalf("protobufDescriptor() error = %v", err)
	}
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(9))
	raw, _ := proto.Marshal(msg)
	values, err := DecodeTypedFields(context.Background(), "t", schema, nil, raw)
	if err != nil || !values[0].Present || values[0].Value.Int64() != 9 {
		t.Fatalf("protobuf dispatch: values=%+v err=%v", values, err)
	}
}
