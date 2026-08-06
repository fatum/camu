package iceberg

import (
	"fmt"
	"io"
	"math"

	"github.com/hamba/avro/v2"
	"github.com/maksim/camu/internal/meta"
	"github.com/parquet-go/parquet-go"
)

// A hand-rolled Avro binary record decoder. The libraries' generic decode
// (hamba Unmarshal into map[string]any) boxes every field and builds string
// keys per value; this walks the wire format directly, reading only the
// projected fields and skipping the rest by type, so structural decoding is
// allocation-free and only projected string values are materialized.

type avroReader struct {
	data []byte
	pos  int
}

func (r *avroReader) need(n int) error {
	if n < 0 || r.pos+n > len(r.data) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// readVarint reads an unsigned LEB128 varint.
func (r *avroReader) readVarint() (uint64, error) {
	var v uint64
	var shift uint
	for {
		if err := r.need(1); err != nil {
			return 0, err
		}
		b := r.data[r.pos]
		r.pos++
		v |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, nil
		}
		shift += 7
		if shift > 63 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
}

// readLong reads a zigzag-encoded Avro long.
func (r *avroReader) readLong() (int64, error) {
	u, err := r.readVarint()
	if err != nil {
		return 0, err
	}
	return int64(u>>1) ^ -int64(u&1), nil
}

func (r *avroReader) readBool() (bool, error) {
	if err := r.need(1); err != nil {
		return false, err
	}
	b := r.data[r.pos] != 0
	r.pos++
	return b, nil
}

func (r *avroReader) readFloat64() (float64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := math.Float64frombits(binaryLittleEndianUint64(r.data[r.pos : r.pos+8]))
	r.pos += 8
	return v, nil
}

func (r *avroReader) readBytes() ([]byte, error) {
	n, err := r.readLong()
	if err != nil {
		return nil, err
	}
	if n < 0 || n > int64(len(r.data)-r.pos) {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.data[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return b, nil
}

func (r *avroReader) readString() (string, error) {
	b, err := r.readBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// skipAvroValue skips one value encoded with the given Avro schema.
func skipAvroValue(r *avroReader, schema avro.Schema) error {
	switch schema.Type() {
	case avro.Boolean:
		_, err := r.readBool()
		return err
	case avro.Int, avro.Long, avro.Enum:
		_, err := r.readVarint()
		return err
	case avro.Float:
		if err := r.need(4); err != nil {
			return err
		}
		r.pos += 4
		return nil
	case avro.Double:
		if err := r.need(8); err != nil {
			return err
		}
		r.pos += 8
		return nil
	case avro.String, avro.Bytes:
		_, err := r.readBytes()
		return err
	case avro.Null:
		return nil
	case avro.Record:
		rs := schema.(*avro.RecordSchema)
		for _, f := range rs.Fields() {
			if err := skipAvroValue(r, f.Type()); err != nil {
				return err
			}
		}
		return nil
	case avro.Union:
		idx, err := r.readLong() // union index is a zigzag long
		if err != nil {
			return err
		}
		us := schema.(*avro.UnionSchema)
		if idx < 0 || int(idx) >= len(us.Types()) {
			return fmt.Errorf("invalid avro union index %d", idx)
		}
		return skipAvroValue(r, us.Types()[idx])
	case avro.Array:
		items := schema.(*avro.ArraySchema).Items()
		for {
			count, err := r.readLong()
			if err != nil {
				return err
			}
			if count == 0 {
				return nil
			}
			if count < 0 {
				if _, err := r.readVarint(); err != nil { // block size in bytes
					return err
				}
				count = -count
			}
			for i := int64(0); i < count; i++ {
				if err := skipAvroValue(r, items); err != nil {
					return err
				}
			}
		}
	case avro.Map:
		values := schema.(*avro.MapSchema).Values()
		for {
			count, err := r.readLong()
			if err != nil {
				return err
			}
			if count == 0 {
				return nil
			}
			if count < 0 {
				if _, err := r.readVarint(); err != nil { // block size in bytes
					return err
				}
				count = -count
			}
			for i := int64(0); i < count; i++ {
				if _, err := r.readString(); err != nil {
					return err
				}
				if err := skipAvroValue(r, values); err != nil {
					return err
				}
			}
		}
	case avro.Fixed:
		if err := r.need(schema.(*avro.FixedSchema).Size()); err != nil {
			return err
		}
		r.pos += schema.(*avro.FixedSchema).Size()
		return nil
	default:
		return fmt.Errorf("unsupported avro type %q", schema.Type())
	}
}

// readAvroProjected reads a projected scalar field from the wire, converting
// it to a parquet value by the projection field type. A nullable field is a
// union on the wire: a leading index selects null or the concrete value.
// Timestamps are epoch millis on the wire and converted to Unix nanoseconds.
func readAvroProjected(r *avroReader, f meta.SchemaField, wireType avro.Schema) (parquet.Value, bool, error) {
	if wireType.Type() == avro.Union {
		us := wireType.(*avro.UnionSchema)
		idx, err := r.readLong() // union index is a zigzag long
		if err != nil {
			return parquet.Value{}, false, err
		}
		if idx == 0 {
			return parquet.Value{}, false, nil // null
		}
		if idx < 0 || int(idx) >= len(us.Types()) {
			return parquet.Value{}, false, fmt.Errorf("invalid avro union index %d", idx)
		}
		wireType = us.Types()[idx]
	}
	switch f.Type {
	case "string":
		s, err := r.readString()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.ValueOf(s), true, nil
	case "int64":
		v, err := r.readLong()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.Int64Value(v), true, nil
	case "float64":
		v, err := r.readFloat64()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.DoubleValue(v), true, nil
	case "bool":
		v, err := r.readBool()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.BooleanValue(v), true, nil
	case "timestamp":
		v, err := r.readLong()
		if err != nil {
			return parquet.Value{}, false, err
		}
		return parquet.Int64Value(v * int64(1_000_000)), true, nil
	default:
		return parquet.Value{}, false, fmt.Errorf("unsupported schema field type %q", f.Type)
	}
}

// decodeAvroWire decodes an Avro record value by walking the writer schema's
// fields in order, reading projected fields and skipping the rest.
func (p *decodePlan) decodeAvroWire(input []byte, writer avro.Schema, values []DecodedField) error {
	record, ok := writer.(*avro.RecordSchema)
	if !ok {
		return fmt.Errorf("avro value schema is not a record")
	}
	r := &avroReader{data: input}
	for _, f := range record.Fields() {
		idx, projected := p.nameToIndex[f.Name()]
		if !projected {
			if err := skipAvroValue(r, f.Type()); err != nil {
				return fmt.Errorf("decode avro value: %w", err)
			}
			continue
		}
		v, present, err := readAvroProjected(r, p.fields[idx], f.Type())
		if err != nil {
			return fmt.Errorf("decode avro value: %w", err)
		}
		values[idx] = DecodedField{Present: present, Value: v}
	}
	if r.pos != len(r.data) {
		return fmt.Errorf("decode avro value: trailing bytes")
	}
	return nil
}

func binaryLittleEndianUint64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}
