package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/maksim/camu/internal/meta"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file adds Protobuf as a topic value encoding. Like Avro, the topic
// schema's Fields define the projected columns and the value is decoded with a
// message descriptor derived from them (field numbers are positional and
// stable across evolution, since fields are only appended). Values may carry
// the schema-id envelope to resolve their writer schema.

// protobufDescriptor builds the message descriptor for a topic's projected
// fields. Field numbers are assigned positionally starting at 1, which stays
// stable across evolution because versions only add fields.
func protobufDescriptor(topicSchema *meta.TopicSchema) (protoreflect.MessageDescriptor, error) {
	fields := make([]*descriptorpb.FieldDescriptorProto, 0, len(topicSchema.Fields))
	deps := []string{}
	for i, f := range topicSchema.Fields {
		ft, err := protobufFieldType(f.Type)
		if err != nil {
			return nil, err
		}
		fdp := &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(avroFieldName(f.Path)),
			Number: proto.Int32(int32(i + 1)),
			Type:   ft.Enum(),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}
		if f.Type == "timestamp" {
			fdp.TypeName = proto.String(".google.protobuf.Timestamp")
			deps = []string{"google/protobuf/timestamp.proto"}
		}
		fields = append(fields, fdp)
	}
	fd := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("camu_value.proto"),
		Package:     proto.String("camu"),
		Syntax:      proto.String("proto3"),
		Dependency:  deps,
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Value"), Field: fields}},
	}
	file, err := protodesc.NewFile(fd, protoregistry.GlobalFiles)
	if err != nil {
		return nil, fmt.Errorf("build protobuf descriptor: %w", err)
	}
	md := file.Messages().ByName("Value")
	if md == nil {
		return nil, fmt.Errorf("build protobuf descriptor: message Value missing")
	}
	return md, nil
}

func protobufFieldType(t string) (descriptorpb.FieldDescriptorProto_Type, error) {
	switch t {
	case "string":
		return descriptorpb.FieldDescriptorProto_TYPE_STRING, nil
	case "int64":
		return descriptorpb.FieldDescriptorProto_TYPE_INT64, nil
	case "float64":
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, nil
	case "bool":
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL, nil
	case "timestamp":
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, nil
	default:
		return 0, fmt.Errorf("unsupported schema field type %q", t)
	}
}

// DecodeProtobufTypedFields decodes a protobuf-encoded record value and
// materializes the topic schema's projected fields as parquet values. A value
// wrapped in the schema-id envelope is decoded against its writer descriptor
// (resolved via resolver); an unwrapped value is decoded against the topic's
// own descriptor.
func DecodeProtobufTypedFields(ctx context.Context, topic string, topicSchema *meta.TopicSchema, resolver SchemaResolver, input []byte) ([]DecodedField, error) {
	plan, err := decodePlanFor(topicSchema)
	if err != nil {
		return nil, err
	}
	return plan.decode(ctx, topic, resolver, input)
}

func (p *decodePlan) decodeProtobufInto(ctx context.Context, topic string, resolver SchemaResolver, input []byte, values []DecodedField) error {
	// The wire scan matches fields by number, which the projection assigns
	// positionally (1..N) and evolution preserves by appending, so the writer
	// schema need not be resolved. Strip the envelope, if any, and decode.
	if _, payload, wrapped := AvroUnwrap(input); wrapped {
		input = payload
	}
	return p.decodeProtobufWire(input, values)
}

// EncodeProtobufValue encodes a record value against a topic's derived
// protobuf descriptor. It is the inverse of DecodeProtobufTypedFields and is
// used by tooling and tests. Timestamp fields take a time.Time.
func EncodeProtobufValue(topicSchema *meta.TopicSchema, record map[string]any) ([]byte, error) {
	plan, err := decodePlanFor(topicSchema)
	if err != nil {
		return nil, err
	}
	md := plan.proto
	msg := dynamicpb.NewMessage(md)
	for _, f := range topicSchema.Fields {
		name := protoreflect.Name(avroFieldName(f.Path))
		fd := md.Fields().ByName(name)
		if fd == nil {
			return nil, fmt.Errorf("no protobuf field %q", name)
		}
		raw, present := record[avroFieldName(f.Path)]
		if !present || raw == nil {
			continue
		}
		var val protoreflect.Value
		switch fd.Kind() {
		case protoreflect.StringKind:
			val = protoreflect.ValueOfString(raw.(string))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			val = protoreflect.ValueOfInt64(raw.(int64))
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			val = protoreflect.ValueOfInt32(raw.(int32))
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			val = protoreflect.ValueOfUint64(raw.(uint64))
		case protoreflect.DoubleKind:
			val = protoreflect.ValueOfFloat64(raw.(float64))
		case protoreflect.FloatKind:
			val = protoreflect.ValueOfFloat32(raw.(float32))
		case protoreflect.BoolKind:
			val = protoreflect.ValueOfBool(raw.(bool))
		case protoreflect.MessageKind:
			ts, ok := raw.(time.Time)
			if !ok {
				return nil, fmt.Errorf("field %q must be a timestamp", f.Name)
			}
			val = protoreflect.ValueOfMessage(timestamppb.New(ts).ProtoReflect())
		default:
			return nil, fmt.Errorf("unsupported protobuf kind for field %q", f.Name)
		}
		msg.Set(fd, val)
	}
	return proto.Marshal(msg)
}

func protobufFieldValue(f meta.SchemaField, kind protoreflect.Kind, v protoreflect.Value) (parquet.Value, error) {
	switch f.Type {
	case "string":
		if kind != protoreflect.StringKind {
			return parquet.Value{}, fmt.Errorf("field %q must be string", f.Name)
		}
		return parquet.ValueOf(v.String()), nil
	case "int64":
		switch kind {
		case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind,
			protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind,
			protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
			return parquet.Int64Value(v.Int()), nil
		}
		return parquet.Value{}, fmt.Errorf("field %q must be int64", f.Name)
	case "float64":
		switch kind {
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			return parquet.DoubleValue(v.Float()), nil
		}
		return parquet.Value{}, fmt.Errorf("field %q must be number", f.Name)
	case "bool":
		if kind != protoreflect.BoolKind {
			return parquet.Value{}, fmt.Errorf("field %q must be bool", f.Name)
		}
		return parquet.BooleanValue(v.Bool()), nil
	case "timestamp":
		if kind != protoreflect.MessageKind {
			return parquet.Value{}, fmt.Errorf("field %q must be a timestamp", f.Name)
		}
		m := v.Message()
		fields := m.Descriptor().Fields()
		seconds := m.Get(fields.ByName("seconds")).Int()
		nanos := m.Get(fields.ByName("nanos")).Int()
		return parquet.Int64Value(time.Unix(seconds, nanos).UTC().UnixNano()), nil
	default:
		return parquet.Value{}, fmt.Errorf("unsupported schema field type %q", f.Type)
	}
}
