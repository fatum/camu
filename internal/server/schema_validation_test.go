package server

import (
	"github.com/maksim/camu/internal/meta"
	"testing"
)

func TestValidateTypedValue(t *testing.T) {
	s := &meta.TopicSchema{Encoding: "json", Fields: []meta.SchemaField{{Name: "id", Type: "int64", Path: "$.id"}, {Name: "ok", Type: "bool", Path: "$.ok", Nullable: true}}}
	if err := validateTypedValue(s, `{"id": 4}`); err != nil {
		t.Fatal(err)
	}
	if err := validateTypedValue(s, `{"id":"4"}`); err == nil {
		t.Fatal("expected type error")
	}
	if err := validateTypedValue(s, `{"ok":true}`); err == nil {
		t.Fatal("expected missing required field")
	}
}
