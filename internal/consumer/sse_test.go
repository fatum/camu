package consumer

import (
	"bytes"
	"strings"
	"testing"

	"github.com/maksim/camu/internal/log"
)

func TestFormatSSEEvent(t *testing.T) {
	msg := log.Message{
		Offset:    42,
		Timestamp: 1000,
		Key:       []byte("k1"),
		Value:     []byte("hello"),
	}
	var buf bytes.Buffer
	err := WriteSSEEvent(&buf, msg)
	if err != nil {
		t.Fatalf("WriteSSEEvent() error: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "id: 42") {
		t.Error("SSE event missing id field")
	}
	if !strings.Contains(output, "data: ") {
		t.Error("SSE event missing data field")
	}
	if !strings.HasSuffix(output, "\n\n") {
		t.Error("SSE event should end with double newline")
	}
}
