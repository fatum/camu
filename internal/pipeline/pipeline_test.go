package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/maksim/camu/internal/consumer"
	"github.com/maksim/camu/internal/log"
	"github.com/maksim/camu/internal/storage"
)

type testAppender struct {
	rows      []log.Message
	last      uint64
	seqs      []uint64
	waits     int
	waitTopic string
	waitEpoch uint64
}

func (a *testAppender) Append(_ context.Context, _ string, _ int, _ uint64, sequence uint64, rows []log.Message) (uint64, bool, error) {
	for _, prior := range a.seqs {
		if prior == sequence {
			return a.last, true, nil
		}
	}
	a.seqs = append(a.seqs, sequence)
	a.rows = append(a.rows, rows...)
	a.last = sequence + uint64(len(rows)) - 1
	return a.last, false, nil
}
func (a *testAppender) WaitDurable(_ context.Context, topic string, _ int, epoch, _ uint64) error {
	a.waits++
	a.waitTopic = topic
	a.waitEpoch = epoch
	return nil
}

func TestDLQSinkPreservesOriginalBytesAndIsIdempotent(t *testing.T) {
	a := &testAppender{}
	s := NewDLQSink(a, "source", "source-dlq", nil)
	m := log.Message{Offset: 7, Key: []byte{0, 1, 2}, Value: []byte{0xff, 0x00}, Headers: map[string]string{"x": "y"}}
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "source", Partition: 2, SourceEpoch: 4, StartOffset: 100, SinkStartSequence: 0, Messages: []log.Message{m}}); err != nil {
		t.Fatal(err)
	}
	if a.waits != 1 || len(a.rows) != 1 {
		t.Fatalf("append/wait = %d/%d", a.waits, len(a.rows))
	}
	var got map[string]any
	if err := json.Unmarshal(a.rows[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got["original_value"] != "/wA=" {
		t.Fatalf("original_value = %v, want base64 /wA=", got["original_value"])
	}
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "source", Partition: 2, SourceEpoch: 4, StartOffset: 101, SinkStartSequence: 0, Messages: []log.Message{m}}); err != nil {
		t.Fatal(err)
	}
	if len(a.rows) != 1 {
		t.Fatalf("duplicate retry appended %d rows", len(a.rows))
	}
	if len(a.seqs) != 1 || a.seqs[0] != 0 {
		t.Fatalf("sink sequences = %v, want [0]", a.seqs)
	}
}

func TestDLQSinkWaitDurableUsesSourceFenceIdentity(t *testing.T) {
	a := &testAppender{}
	s := NewDLQSink(a, "source", "source-dlq", nil)
	_, err := s.Write(context.Background(), Batch{
		SourceTopic: "source", Partition: 1, SourceEpoch: 9,
		SinkStartSequence: 0, Messages: []log.Message{{Offset: 3, Value: []byte("bad")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.waitTopic != "source" || a.waitEpoch != 9 {
		t.Fatalf("WaitDurable identity = %q/%d, want source/9", a.waitTopic, a.waitEpoch)
	}
}

func TestDLQSinkFencesBeforeAppend(t *testing.T) {
	a := &testAppender{}
	s := NewDLQSink(a, "source", "dlq", fenceFunc(func(context.Context, string, int, uint64) bool { return true }))
	_, err := s.Write(context.Background(), Batch{SourceTopic: "source", Partition: 0, SourceEpoch: 1, StartOffset: 0, Messages: []log.Message{{Value: []byte("x")}}})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("error = %v, want ErrFenced", err)
	}
}

func TestDLQSinkFencesSourceIdentity(t *testing.T) {
	a := &testAppender{}
	f := &recordingFence{}
	s := NewDLQSink(a, "source", "dlq", f)
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "source", Partition: 3, SourceEpoch: 9, Messages: []log.Message{{Value: []byte("x")}}}); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 2 || f.calls[0] != "source/3/9" || f.calls[1] != "source/3/9" {
		t.Fatalf("fence calls = %v, want source identity before/after", f.calls)
	}
}

func TestDLQSinkRejectsNilAppenderAndWrongSource(t *testing.T) {
	s := NewDLQSink(nil, "source", "dlq", nil)
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "source", Messages: []log.Message{{Value: []byte("x")}}}); err == nil {
		t.Fatal("nil appender accepted")
	}
	s = NewDLQSink(&testAppender{}, "source", "dlq", nil)
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "other", Messages: []log.Message{{Value: []byte("x")}}}); err == nil {
		t.Fatal("wrong source topic accepted")
	}
}

func TestDLQSinkUsesContiguousSinkSequenceAcrossSourceGaps(t *testing.T) {
	a := &testAppender{}
	s := NewDLQSink(a, "source", "dlq", nil)
	first := []log.Message{{Offset: 100, Value: []byte("a")}, {Offset: 105, Value: []byte("b")}}
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "source", Partition: 0, SourceEpoch: 1, StartOffset: 100, EndOffset: 105, SinkStartSequence: 0, Messages: first}); err != nil {
		t.Fatal(err)
	}
	// A retry uses the same sink sequence and is idempotent, regardless of
	// source offsets. A later batch starts at the next contiguous sink seq.
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "source", Partition: 0, SourceEpoch: 1, StartOffset: 100, EndOffset: 105, SinkStartSequence: 0, Messages: first}); err != nil {
		t.Fatal(err)
	}
	if len(a.seqs) != 1 || a.seqs[0] != 0 {
		t.Fatalf("retry sequences = %v, want [0]", a.seqs)
	}
	if _, err := s.Write(context.Background(), Batch{SourceTopic: "source", Partition: 0, SourceEpoch: 1, StartOffset: 200, EndOffset: 205, SinkStartSequence: 2, Messages: []log.Message{{Offset: 200, Value: []byte("c")}}}); err != nil {
		t.Fatal(err)
	}
	if len(a.seqs) != 2 || a.seqs[1] != 2 {
		t.Fatalf("sink sequences = %v, want [0 2]", a.seqs)
	}
}

func TestCheckpointStoreOutputBeforeCheckpointAndRetry(t *testing.T) {
	obj, err := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	s := NewCheckpointStore(obj, nil)
	cp := Checkpoint{SourceTopic: "events", Partition: 0, NextOffset: 4, SourceEpoch: 2, Sink: "dlq", SinkVersion: "v1", OutputStart: 0, OutputEnd: 3, Generation: 1}
	if err := s.Publish(context.Background(), "dlq", cp); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(context.Background(), "dlq", cp); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	got, err := s.Load(context.Background(), "dlq", "events", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != cp {
		t.Fatalf("checkpoint = %+v, want %+v", got, cp)
	}
	if err := s.Publish(context.Background(), "dlq", Checkpoint{SourceTopic: "events", Partition: 0, NextOffset: 8, Sink: "dlq", SinkVersion: "v1", Generation: 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale publish = %v, want conflict", err)
	}
}

func TestCheckpointKey(t *testing.T) {
	if got, want := CheckpointKey("parquet", "events", 7), "_meta/pipelines/parquet/events/7.json"; got != want {
		t.Fatalf("CheckpointKey = %q, want %q", got, want)
	}
}

func TestCheckpointStoreRejectsStaleEpoch(t *testing.T) {
	obj, err := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	s := NewCheckpointStore(obj, nil)
	if err := s.Publish(context.Background(), "sink", Checkpoint{SourceTopic: "events", Partition: 0, SourceEpoch: 4, Sink: "sink", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	err = s.Publish(context.Background(), "sink", Checkpoint{SourceTopic: "events", Partition: 0, SourceEpoch: 3, Sink: "sink", Generation: 2})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale epoch error = %v, want conflict", err)
	}
}

func TestCheckpointStoreRejectsProgressRegression(t *testing.T) {
	obj, err := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	s := NewCheckpointStore(obj, nil)
	base := Checkpoint{SourceTopic: "events", Partition: 0, NextOffset: 10, SourceEpoch: 1, Sink: "sink", OutputStart: 2, OutputEnd: 9, Generation: 1}
	if err := s.Publish(context.Background(), "sink", base); err != nil {
		t.Fatal(err)
	}
	for _, cp := range []Checkpoint{
		{SourceTopic: "events", Partition: 0, NextOffset: 9, SourceEpoch: 1, Sink: "sink", OutputStart: 2, OutputEnd: 9, Generation: 2},
		{SourceTopic: "events", Partition: 0, NextOffset: 11, SourceEpoch: 1, Sink: "sink", OutputStart: 1, OutputEnd: 10, Generation: 2},
		{SourceTopic: "events", Partition: 0, NextOffset: 11, SourceEpoch: 1, Sink: "sink", OutputStart: 2, OutputEnd: 8, Generation: 2},
	} {
		if err := s.Publish(context.Background(), "sink", cp); !errors.Is(err, ErrConflict) {
			t.Fatalf("regression publish error = %v, want conflict", err)
		}
	}
}

func TestCheckpointStoreLoadValidatesIdentityAndKeys(t *testing.T) {
	obj, err := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	if err != nil {
		t.Fatal(err)
	}
	s := NewCheckpointStore(obj, nil)
	if err := s.Publish(context.Background(), "sink", Checkpoint{SourceTopic: "events", Partition: 0, Sink: "sink", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(context.Background(), "sink", "other", 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("other topic load error = %v, want not found", err)
	}
	for _, bad := range []struct{ pipeline, topic string }{{"../sink", "events"}, {"sink", "events/x"}, {"", "events"}} {
		if _, err := s.Load(context.Background(), bad.pipeline, bad.topic, 0); err == nil {
			t.Fatalf("unsafe key accepted: %#v", bad)
		}
	}
}

func TestCheckpointStoreFencesEpoch(t *testing.T) {
	f := fenceFunc(func(context.Context, string, int, uint64) bool { return true })
	obj, _ := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	s := NewCheckpointStore(obj, f)
	err := s.Publish(context.Background(), "p", Checkpoint{SourceTopic: "t", Partition: 0, SourceEpoch: 3, Sink: "s", Generation: 1})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("error = %v, want fenced", err)
	}
}

func TestReaderStopsAtCommittedHighWatermark(t *testing.T) {
	obj, _ := storage.NewS3Client(storage.S3Config{Endpoint: "memory://"})
	cache, err := log.NewDiskCache(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	key := "events/0/0-4.segment"
	var data []byte
	for i := 0; i < 5; i++ {
		data = append(data, log.EncodeRecordBatch(int64(i), []log.Message{{Offset: uint64(i), Value: []byte{byte(i)}}})...)
	}
	if err := cache.Put(key, data); err != nil {
		t.Fatal(err)
	}
	idx := log.NewIndex()
	idx.Add(log.SegmentRef{BaseOffset: 0, EndOffset: 4, Key: key})
	r := NewReader(consumer.NewFetcher(obj, cache), nil)
	msgs, next, err := r.Read(context.Background(), idx, "events", 0, 0, 3, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 || next != 3 {
		t.Fatalf("read = %d messages, next %d", len(msgs), next)
	}
}

func TestValidateCommittedRange(t *testing.T) {
	if err := ValidateCommittedRange(2, 4, 5); err != nil {
		t.Fatalf("valid range: %v", err)
	}
	for _, tc := range []struct {
		start, end, high uint64
	}{
		{4, 2, 5},
		{2, 5, 5},
		{5, 5, 5},
	} {
		if err := ValidateCommittedRange(tc.start, tc.end, tc.high); err == nil {
			t.Fatalf("ValidateCommittedRange(%d,%d,%d) accepted invalid range", tc.start, tc.end, tc.high)
		}
	}
}

type fenceFunc func(context.Context, string, int, uint64) bool

func (f fenceFunc) Fenced(ctx context.Context, t string, p int, e uint64) bool {
	return f(ctx, t, p, e)
}

type recordingFence struct{ calls []string }

func (f *recordingFence) Fenced(_ context.Context, topic string, partition int, epoch uint64) bool {
	f.calls = append(f.calls, fmt.Sprintf("%s/%d/%d", topic, partition, epoch))
	return false
}
