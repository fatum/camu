package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/maksim/camu/internal/meta"
)

func validateTypedValue(schema *meta.TopicSchema, value string) error {
	if schema == nil {
		return nil
	}
	var root any
	if err := json.Unmarshal([]byte(value), &root); err != nil {
		return fmt.Errorf("value is not valid JSON: %w", err)
	}
	obj, ok := root.(map[string]any)
	if !ok {
		return fmt.Errorf("value must be a JSON object")
	}
	for _, f := range schema.Fields {
		v, found := jsonPathValue(obj, f.Path)
		if !found || v == nil {
			if f.Nullable {
				continue
			}
			return fmt.Errorf("required field %q is missing", f.Name)
		}
		switch f.Type {
		case "string":
			if _, ok := v.(string); !ok {
				return fmt.Errorf("field %q must be string", f.Name)
			}
		case "bool":
			if _, ok := v.(bool); !ok {
				return fmt.Errorf("field %q must be bool", f.Name)
			}
		case "int64":
			n, ok := v.(float64)
			if !ok || n != float64(int64(n)) {
				return fmt.Errorf("field %q must be int64", f.Name)
			}
		case "float64":
			if _, ok := v.(float64); !ok {
				return fmt.Errorf("field %q must be number", f.Name)
			}
		case "timestamp":
			if !validTimestamp(v) {
				return fmt.Errorf("field %q must be RFC3339 timestamp", f.Name)
			}
		}
	}
	return nil
}

func jsonPathValue(root map[string]any, path string) (any, bool) {
	parts := strings.Split(strings.TrimPrefix(path, "$."), ".")
	var cur any = root
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func validTimestamp(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, s)
	return err == nil
}

func typedValueAtPath(value string, path string) (any, bool, error) {
	var root map[string]any
	if err := json.Unmarshal([]byte(value), &root); err != nil {
		return nil, false, err
	}
	v, ok := jsonPathValue(root, path)
	return v, ok, nil
}

func asInt64(v any) (int64, error) {
	n, ok := v.(float64)
	if !ok || n != float64(int64(n)) {
		return 0, fmt.Errorf("not int64")
	}
	return int64(n), nil
}
func asFloat64(v any) (float64, error) {
	n, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("not number")
	}
	return n, nil
}
func asString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("not string")
	}
	return s, nil
}
func asBool(v any) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("not bool")
	}
	return b, nil
}
func asTimestamp(v any) (string, error) {
	s, err := asString(v)
	if err != nil {
		return "", err
	}
	if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
		return "", err
	}
	return s, nil
}
