package iceberg

import (
	"fmt"
	"math"

	"github.com/maksim/camu/internal/meta"
	"github.com/parquet-go/parquet-go"
)

// A hand-rolled protobuf wire decoder. The dynamicpb + proto.Unmarshal path
// builds a full message graph per value; this scans the wire bytes directly,
// reading projected fields by number and skipping the rest by wire type, so
// structural decoding is allocation-free and only string values are
// materialized.

type protoReader struct {
	data []byte
	pos  int
}

func (r *protoReader) readVarint() (uint64, error) {
	var v uint64
	var shift uint
	for i := 0; i < 10; i++ {
		if r.pos >= len(r.data) {
			return 0, fmt.Errorf("truncated varint")
		}
		b := r.data[r.pos]
		r.pos++
		v |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, nil
		}
		shift += 7
	}
	return 0, fmt.Errorf("varint too long")
}

func (r *protoReader) readLengthDelimited() ([]byte, error) {
	n, err := r.readVarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(r.data)-r.pos) {
		return nil, fmt.Errorf("truncated length-delimited field")
	}
	b := r.data[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return b, nil
}

func (r *protoReader) readFixed64() (uint64, error) {
	if len(r.data)-r.pos < 8 {
		return 0, fmt.Errorf("truncated 64-bit field")
	}
	v := binaryLittleEndianUint64(r.data[r.pos : r.pos+8])
	r.pos += 8
	return v, nil
}

func (r *protoReader) skip(wireType uint64) error {
	switch wireType {
	case 0:
		_, err := r.readVarint()
		return err
	case 1:
		if len(r.data)-r.pos < 8 {
			return fmt.Errorf("truncated 64-bit field")
		}
		r.pos += 8
		return nil
	case 2:
		_, err := r.readLengthDelimited()
		return err
	case 3: // start group: skip to the matching end group
		depth := 1
		for depth > 0 {
			tag, err := r.readVarint()
			if err != nil {
				return err
			}
			wt := tag & 7
			switch wt {
			case 4:
				depth--
			case 3:
				depth++
			default:
				if err := r.skip(wt); err != nil {
					return err
				}
			}
		}
		return nil
	case 4:
		return fmt.Errorf("unexpected end group")
	case 5:
		if len(r.data)-r.pos < 4 {
			return fmt.Errorf("truncated 32-bit field")
		}
		r.pos += 4
		return nil
	default:
		return fmt.Errorf("unknown wire type %d", wireType)
	}
}

// readProtoProjected reads a projected scalar field by wire type, converting it
// to a parquet value by the projection field type.
func readProtoProjected(r *protoReader, f meta.SchemaField, wireType uint64) (parquet.Value, bool, error) {
	switch f.Type {
	case "string":
		if wireType != 2 {
			return parquet.Value{}, false, fmt.Errorf("field %q must be string", f.Name)
		}
		b, err := r.readLengthDelimited()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.ValueOf(string(b)), true, nil
	case "int64":
		if wireType != 0 {
			return parquet.Value{}, false, fmt.Errorf("field %q must be int64", f.Name)
		}
		v, err := r.readVarint()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.Int64Value(int64(v)), true, nil
	case "float64":
		if wireType != 1 {
			return parquet.Value{}, false, fmt.Errorf("field %q must be number", f.Name)
		}
		v, err := r.readFixed64()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.DoubleValue(math.Float64frombits(v)), true, nil
	case "bool":
		if wireType != 0 {
			return parquet.Value{}, false, fmt.Errorf("field %q must be bool", f.Name)
		}
		v, err := r.readVarint()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.BooleanValue(v != 0), true, nil
	case "timestamp":
		if wireType != 2 {
			return parquet.Value{}, false, fmt.Errorf("field %q must be a timestamp", f.Name)
		}
		inner, err := r.readLengthDelimited()
		if err != nil {
			return parquet.Value{}, false, err
		}
		seconds, nanos, err := parseProtoTimestamp(inner)
		if err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be a timestamp", f.Name)
		}
		return parquet.Int64Value((seconds*1_000_000_000 + nanos)), true, nil
	default:
		return parquet.Value{}, false, fmt.Errorf("unsupported schema field type %q", f.Type)
	}
}

// parseProtoTimestamp reads a google.protobuf.Timestamp message (field 1 =
// seconds varint, field 2 = nanos varint).
func parseProtoTimestamp(inner []byte) (int64, int64, error) {
	r := &protoReader{data: inner}
	var seconds, nanos int64
	for r.pos < len(inner) {
		tag, err := r.readVarint()
		if err != nil {
			return 0, 0, err
		}
		fieldNum, wireType := tag>>3, tag&7
		switch {
		case fieldNum == 1 && wireType == 0:
			v, err := r.readVarint()
			if err != nil {
				return 0, 0, err
			}
			seconds = int64(v)
		case fieldNum == 2 && wireType == 0:
			v, err := r.readVarint()
			if err != nil {
				return 0, 0, err
			}
			nanos = int64(v)
		default:
			if err := r.skip(wireType); err != nil {
				return 0, 0, err
			}
		}
	}
	return seconds, nanos, nil
}

// decodeProtobufWire decodes a protobuf message by scanning wire tags and
// reading projected fields by number (the projection assigns numbers 1..N by
// position, which evolution preserves by appending).
func (p *decodePlan) decodeProtobufWire(input []byte, values []DecodedField) error {
	r := &protoReader{data: input}
	for r.pos < len(input) {
		tag, err := r.readVarint()
		if err != nil {
			return fmt.Errorf("decode protobuf value: %w", err)
		}
		fieldNum, wireType := tag>>3, tag&7
		if fieldNum == 0 {
			return fmt.Errorf("decode protobuf value: invalid field number 0")
		}
		idx := int(fieldNum) - 1
		if idx >= 0 && idx < len(p.fields) {
			v, present, err := readProtoProjected(r, p.fields[idx], wireType)
			if err != nil {
				return fmt.Errorf("decode protobuf value: %w", err)
			}
			values[idx] = DecodedField{Present: present, Value: v}
		} else {
			if err := r.skip(wireType); err != nil {
				return fmt.Errorf("decode protobuf value: %w", err)
			}
		}
	}
	for i, f := range p.fields {
		if !values[i].Present && !f.Nullable {
			return fmt.Errorf("required field %q is missing", f.Name)
		}
	}
	return nil
}
