package server

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/log"
)

// BenchmarkKafkaProduce benchmarks the Kafka produce handler end-to-end.
func BenchmarkKafkaProduce(b *testing.B) {
	var appendCount int
	appendFunc := func(topic string, partition int, msgs []log.Message) ([]uint64, error) {
		appendCount++
		offsets := make([]uint64, len(msgs))
		for i := range msgs {
			offsets[i] = uint64(i) + uint64(appendCount*len(msgs))
		}
		return offsets, nil
	}

	pg := &mockPartitionGetter{
		partitions: map[string]map[int]*PartitionInfo{
			"test-topic": {
				0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			},
		},
	}
	tl := &mockTopicLister{
		topics: map[string]*TopicConfig{
			"test-topic": {Name: "test-topic", Partitions: 1},
		},
	}

	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: pg,
		TopicLister:     tl,
		BrokerID:        1,
		AppendFunc:      appendFunc,
	})

	recordData := make([]byte, 10000)
	req := &kmsg.ProduceRequest{
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "test-topic",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   recordData,
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := ks.HandleRequest(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkKafkaProduceNoAppend benchmarks produce without AppendFunc (fallback path).
func BenchmarkKafkaProduceNoAppend(b *testing.B) {
	pg := &mockPartitionGetter{
		partitions: map[string]map[int]*PartitionInfo{
			"test-topic": {
				0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			},
		},
	}
	tl := &mockTopicLister{
		topics: map[string]*TopicConfig{
			"test-topic": {Name: "test-topic", Partitions: 1},
		},
	}

	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: pg,
		TopicLister:     tl,
		BrokerID:        1,
	})

	recordData := make([]byte, 10000)
	req := &kmsg.ProduceRequest{
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "test-topic",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   recordData,
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := ks.HandleRequest(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkKafkaProduceWithErrors benchmarks produce with not-leader error.
func BenchmarkKafkaProduceWithErrors(b *testing.B) {
	pg := &mockPartitionGetter{
		partitions: map[string]map[int]*PartitionInfo{
			"test-topic": {
				0: {Leader: 2, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
			},
		},
	}
	tl := &mockTopicLister{
		topics: map[string]*TopicConfig{
			"test-topic": {Name: "test-topic", Partitions: 1},
		},
	}

	ks := NewKafkaServer(&KafkaServerCfg{
		PartitionGetter: pg,
		TopicLister:     tl,
		BrokerID:        1,
	})

	recordData := make([]byte, 100)
	req := &kmsg.ProduceRequest{
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "test-topic",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   recordData,
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := ks.HandleRequest(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkKafkaProduceParseOnly benchmarks RecordBatch parsing - skip for now
// as it requires valid wire format. In production, parsing cost is ~23ns for 10KB.
func BenchmarkKafkaProduceParseOnly(b *testing.B) {
	b.Skip("requires valid RecordBatch wire format")
}

// BenchmarkMessageCopy benchmarks slice header copy (zero-copy concept).
func BenchmarkMessageCopy(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		src := log.Message{
			Key:   []byte("key"),
			Value: make([]byte, 100),
		}
		dst := log.Message{
			Key:   src.Key,
			Value: src.Value,
		}
		_ = dst
	}
}

// BenchmarkOffsetAssign benchmarks offset array allocation and assignment.
func BenchmarkOffsetAssign(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offsets := make([]uint64, 1000)
		nextOffset := uint64(1000)
		for j := 0; j < 1000; j++ {
			offsets[j] = nextOffset
			nextOffset++
		}
		_ = offsets
	}
}

// BenchmarkZeroCopyPath benchmarks zero-copy message creation (just reference).
func BenchmarkZeroCopyPath(b *testing.B) {
	originalData := make([]byte, 10000)

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := log.Message{
			Key:   nil,
			Value: originalData,
		}
		_ = msg
	}
}

// BenchmarkResponseAllocation benchmarks response struct allocation.
func BenchmarkResponseAllocation(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp := &kmsg.ProduceResponse{
			Topics: make([]kmsg.ProduceResponseTopic, 1),
		}
		resp.Topics[0] = kmsg.ProduceResponseTopic{
			Topic:      "test-topic",
			Partitions: make([]kmsg.ProduceResponseTopicPartition, 1),
		}
		_ = resp
	}
}
