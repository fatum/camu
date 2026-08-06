package iceberg

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/maksim/camu/internal/meta"
	"github.com/parquet-go/parquet-go"
)

// jsonFieldTree maps a topic schema's field paths to leaf nodes holding the
// projected field indexes. Children are matched by raw bytes so object keys
// are never materialized as strings.
type jsonFieldTree struct {
	children []jsonTreeEdge
	fields   []int
}

type jsonTreeEdge struct {
	name  string
	child *jsonFieldTree
}

func newJSONFieldTree(fields []meta.SchemaField) *jsonFieldTree {
	root := &jsonFieldTree{}
	for index, field := range fields {
		current := root
		for _, part := range strings.Split(strings.TrimPrefix(field.Path, "$."), ".") {
			var next *jsonFieldTree
			for i := range current.children {
				if current.children[i].name == part {
					next = current.children[i].child
					break
				}
			}
			if next == nil {
				next = &jsonFieldTree{}
				current.children = append(current.children, jsonTreeEdge{name: part, child: next})
			}
			current = next
		}
		current.fields = append(current.fields, index)
	}
	return root
}

func (t *jsonFieldTree) matchBytes(key []byte) *jsonFieldTree {
	for i := range t.children {
		if bytesEqualString(key, t.children[i].name) {
			return t.children[i].child
		}
	}
	return nil
}

func (t *jsonFieldTree) matchString(key string) *jsonFieldTree {
	for i := range t.children {
		if key == t.children[i].name {
			return t.children[i].child
		}
	}
	return nil
}

// jsonScanner is a minimal, allocation-light JSON tokenizer used to extract a
// topic schema's projected fields without decoding the whole value through
// encoding/json (whose Token API allocates a string per key/value and refills
// buffers). Keys are matched against the field tree by raw bytes; unknown
// values are skipped with a balanced scan; only projected string values are
// materialized.
type jsonScanner struct {
	data []byte
	pos  int
}

func (s *jsonScanner) eof() bool { return s.pos >= len(s.data) }

func (s *jsonScanner) skipSpace() {
	for !s.eof() {
		switch s.data[s.pos] {
		case ' ', '\t', '\n', '\r':
			s.pos++
		default:
			return
		}
	}
}

func (s *jsonScanner) peek() byte {
	if s.eof() {
		return 0
	}
	return s.data[s.pos]
}

// readStringBody consumes a JSON string starting at its opening quote and
// returns the raw body bytes and whether it contains escapes.
func (s *jsonScanner) readStringBody() ([]byte, bool, error) {
	if s.eof() || s.data[s.pos] != '"' {
		return nil, false, fmt.Errorf("expected string")
	}
	s.pos++
	start := s.pos
	escaped := false
	for {
		if s.eof() {
			return nil, false, fmt.Errorf("unterminated string")
		}
		switch c := s.data[s.pos]; {
		case c == '\\':
			escaped = true
			s.pos += 2
		case c == '"':
			raw := s.data[start:s.pos]
			s.pos++
			return raw, escaped, nil
		case c < 0x20:
			return nil, false, fmt.Errorf("invalid character in string")
		default:
			s.pos++
		}
	}
}

// unescapeString decodes a JSON string body (without quotes) into a Go string.
func unescapeString(raw []byte) (string, error) {
	var b []byte
	start := 0
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		if b == nil {
			b = make([]byte, 0, len(raw))
		}
		b = append(b, raw[start:i]...)
		i++
		if i >= len(raw) {
			return "", fmt.Errorf("invalid escape")
		}
		switch raw[i] {
		case '"':
			b = append(b, '"')
		case '\\':
			b = append(b, '\\')
		case '/':
			b = append(b, '/')
		case 'b':
			b = append(b, '\b')
		case 'f':
			b = append(b, '\f')
		case 'n':
			b = append(b, '\n')
		case 'r':
			b = append(b, '\r')
		case 't':
			b = append(b, '\t')
		case 'u':
			r, n, err := parseUnicodeEscape(raw[i:])
			if err != nil {
				return "", err
			}
			i += n
			b = utf8.AppendRune(b, r)
		default:
			return "", fmt.Errorf("invalid escape")
		}
		start = i + 1
	}
	if b == nil {
		return string(raw), nil
	}
	b = append(b, raw[start:]...)
	return string(b), nil
}

// parseUnicodeEscape decodes a \uXXXX escape at the start of raw (raw[0]=='u')
// and handles surrogate pairs. Returns the rune and the number of bytes
// consumed after the backslash.
func parseUnicodeEscape(raw []byte) (rune, int, error) {
	if len(raw) < 5 || raw[0] != 'u' {
		return 0, 0, fmt.Errorf("invalid unicode escape")
	}
	code, err := strconv.ParseUint(string(raw[1:5]), 16, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid unicode escape")
	}
	r := rune(code)
	if utf16.IsSurrogate(r) {
		// Look for a following \uXXXX low surrogate.
		if len(raw) >= 11 && raw[5] == '\\' && raw[6] == 'u' {
			lo, err := strconv.ParseUint(string(raw[7:11]), 16, 32)
			if err == nil {
				if combined := utf16.DecodeRune(r, rune(lo)); combined != utf8.RuneError {
					return combined, 10, nil
				}
			}
		}
		return utf8.RuneError, 4, nil
	}
	return r, 4, nil
}

// readNumber consumes a JSON number and returns its raw bytes.
func (s *jsonScanner) readNumber() []byte {
	start := s.pos
	for !s.eof() {
		switch c := s.data[s.pos]; {
		case c == '-', c == '+', c == '.', c == 'e', c == 'E':
			s.pos++
		case c >= '0' && c <= '9':
			s.pos++
		default:
			return s.data[start:s.pos]
		}
	}
	return s.data[start:s.pos]
}

// consumeKeyword consumes a literal keyword (true/false/null).
func (s *jsonScanner) consumeKeyword(word string) error {
	if len(s.data)-s.pos < len(word) || string(s.data[s.pos:s.pos+len(word)]) != word {
		return fmt.Errorf("value is not valid JSON")
	}
	s.pos += len(word)
	return nil
}

// skipValue skips one JSON value (scalar, object, or array) without decoding it.
func (s *jsonScanner) skipValue() error {
	c := s.peek()
	switch {
	case c == '"':
		if _, _, err := s.readStringBody(); err != nil {
			return err
		}
	case c == '{':
		s.pos++
		s.skipSpace()
		if s.peek() == '}' {
			s.pos++
			return nil
		}
		for {
			s.skipSpace()
			if _, _, err := s.readStringBody(); err != nil {
				return err
			}
			s.skipSpace()
			if s.peek() != ':' {
				return fmt.Errorf("value is not valid JSON")
			}
			s.pos++
			s.skipSpace()
			if err := s.skipValue(); err != nil {
				return err
			}
			s.skipSpace()
			if s.peek() == ',' {
				s.pos++
				continue
			}
			break
		}
		s.skipSpace()
		if s.peek() != '}' {
			return fmt.Errorf("value is not valid JSON")
		}
		s.pos++
	case c == '[':
		s.pos++
		s.skipSpace()
		if s.peek() == ']' {
			s.pos++
			return nil
		}
		for {
			s.skipSpace()
			if err := s.skipValue(); err != nil {
				return err
			}
			s.skipSpace()
			if s.peek() == ',' {
				s.pos++
				continue
			}
			break
		}
		s.skipSpace()
		if s.peek() != ']' {
			return fmt.Errorf("value is not valid JSON")
		}
		s.pos++
	case c == 't':
		return s.consumeKeyword("true")
	case c == 'f':
		return s.consumeKeyword("false")
	case c == 'n':
		return s.consumeKeyword("null")
	case c == '-' || (c >= '0' && c <= '9'):
		s.readNumber()
	default:
		return fmt.Errorf("value is not valid JSON")
	}
	return nil
}

func bytesEqualString(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

func parseJSONInt64(raw []byte) (int64, error) {
	i := 0
	neg := false
	if i < len(raw) && (raw[i] == '-' || raw[i] == '+') {
		neg = raw[i] == '-'
		i++
	}
	if i >= len(raw) {
		return 0, fmt.Errorf("must be int64")
	}
	var v int64
	for ; i < len(raw); i++ {
		c := raw[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("must be int64")
		}
		d := int64(c - '0')
		if v > (1<<63-1-d)/10 {
			return 0, fmt.Errorf("must be int64")
		}
		v = v*10 + d
	}
	if neg {
		v = -v
	}
	return v, nil
}

// stringScalarValue converts an unescaped JSON string into a parquet value for
// a string or timestamp field.
func stringScalarValue(f meta.SchemaField, val string) (parquet.Value, bool, error) {
	switch f.Type {
	case "string":
		return parquet.ValueOf(val), true, nil
	case "timestamp":
		parsed, err := ParseTimestamp(val)
		if err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be RFC3339 timestamp", f.Name)
		}
		return parquet.Int64Value(parsed.UnixNano()), true, nil
	case "int64":
		return parquet.Value{}, false, fmt.Errorf("field %q must be int64", f.Name)
	case "float64":
		return parquet.Value{}, false, fmt.Errorf("field %q must be number", f.Name)
	case "bool":
		return parquet.Value{}, false, fmt.Errorf("field %q must be bool", f.Name)
	default:
		return parquet.Value{}, false, fmt.Errorf("unsupported schema field type %q", f.Type)
	}
}

func numberScalarValue(f meta.SchemaField, raw []byte) (parquet.Value, bool, error) {
	switch f.Type {
	case "int64":
		v, err := parseJSONInt64(raw)
		if err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q %v", f.Name, err)
		}
		return parquet.Int64Value(v), true, nil
	case "float64":
		v, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return parquet.Value{}, false, fmt.Errorf("field %q must be number", f.Name)
		}
		return parquet.DoubleValue(v), true, nil
	case "string":
		return parquet.Value{}, false, fmt.Errorf("field %q must be string", f.Name)
	case "bool":
		return parquet.Value{}, false, fmt.Errorf("field %q must be bool", f.Name)
	default:
		return parquet.Value{}, false, fmt.Errorf("unsupported schema field type %q", f.Type)
	}
}

// parseJSONLeaf reads the scalar value at a leaf field node and converts it per
// the projected field types. A null value leaves the fields absent.
func parseJSONLeaf(s *jsonScanner, tree *jsonFieldTree, fields []meta.SchemaField, values []DecodedField) error {
	c := s.peek()
	switch {
	case c == '"':
		raw, escaped, err := s.readStringBody()
		if err != nil {
			return fmt.Errorf("value is not valid JSON: %w", err)
		}
		var val string
		if escaped {
			val, err = unescapeString(raw)
			if err != nil {
				return fmt.Errorf("value is not valid JSON: %w", err)
			}
		} else {
			val = string(raw)
		}
		for _, idx := range tree.fields {
			pv, present, err := stringScalarValue(fields[idx], val)
			if err != nil {
				return err
			}
			values[idx] = DecodedField{Present: present, Value: pv}
		}
	case c == '-' || (c >= '0' && c <= '9'):
		raw := s.readNumber()
		for _, idx := range tree.fields {
			pv, present, err := numberScalarValue(fields[idx], raw)
			if err != nil {
				return err
			}
			values[idx] = DecodedField{Present: present, Value: pv}
		}
	case c == 't' || c == 'f':
		boolean := c == 't'
		if err := s.consumeKeyword(map[bool]string{true: "true", false: "false"}[boolean]); err != nil {
			return err
		}
		for _, idx := range tree.fields {
			f := fields[idx]
			if f.Type != "bool" {
				return fmt.Errorf("field %q must be bool", f.Name)
			}
			values[idx] = DecodedField{Present: true, Value: parquet.BooleanValue(boolean)}
		}
	case c == 'n':
		if err := s.consumeKeyword("null"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("value is not valid JSON")
	}
	return nil
}

// walkJSONObject consumes an object after its opening delimiter, walking the
// field tree and skipping unknown values.
func walkJSONObject(s *jsonScanner, tree *jsonFieldTree, fields []meta.SchemaField, values []DecodedField) error {
	s.skipSpace()
	if s.peek() == '}' {
		s.pos++
		return nil
	}
	for {
		s.skipSpace()
		rawKey, escaped, err := s.readStringBody()
		if err != nil {
			return fmt.Errorf("value is not valid JSON: %w", err)
		}
		var child *jsonFieldTree
		if escaped {
			key, uerr := unescapeString(rawKey)
			if uerr != nil {
				return fmt.Errorf("value is not valid JSON: %w", uerr)
			}
			child = tree.matchString(key)
		} else {
			child = tree.matchBytes(rawKey)
		}
		s.skipSpace()
		if s.peek() != ':' {
			return fmt.Errorf("value is not valid JSON: expected ':'")
		}
		s.pos++
		s.skipSpace()
		switch {
		case child == nil:
			if err := s.skipValue(); err != nil {
				return fmt.Errorf("value is not valid JSON: %w", err)
			}
		case len(child.fields) > 0:
			if err := parseJSONLeaf(s, child, fields, values); err != nil {
				return err
			}
		case s.peek() == '{':
			s.pos++
			if err := walkJSONObject(s, child, fields, values); err != nil {
				return err
			}
		default:
			if err := s.skipValue(); err != nil {
				return fmt.Errorf("value is not valid JSON: %w", err)
			}
		}
		s.skipSpace()
		if s.peek() == ',' {
			s.pos++
			continue
		}
		break
	}
	s.skipSpace()
	if s.peek() != '}' {
		return fmt.Errorf("value is not valid JSON: expected object end")
	}
	s.pos++
	return nil
}
