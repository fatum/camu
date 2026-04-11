//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/maksim/camu/pkg/camutest"
)

func TestHTTPConsumerOffsetsParityClassicAndDiskless(t *testing.T) {
	modes := []string{"classic", "diskless"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			env := camutest.New(t, camutest.WithInstances(1))
			defer env.Cleanup()

			client := env.Client()
			topic := "consumer-offsets-" + mode
			createTopicForMode(t, client, topic, mode, 2)

			offsets := map[int]uint64{0: 42, 1: 99}
			if err := client.CommitConsumerOffsets(topic, "consumer-a", offsets); err != nil {
				t.Fatalf("CommitConsumerOffsets: %v", err)
			}

			got, err := client.GetConsumerOffsets(topic, "consumer-a")
			if err != nil {
				t.Fatalf("GetConsumerOffsets: %v", err)
			}
			if len(got) != len(offsets) {
				t.Fatalf("len(offsets) = %d, want %d", len(got), len(offsets))
			}
			for partition, want := range offsets {
				if got[partition] != want {
					t.Fatalf("partition %d offset = %d, want %d", partition, got[partition], want)
				}
			}

			other, err := client.GetConsumerOffsets(topic, "consumer-b")
			if err != nil {
				t.Fatalf("GetConsumerOffsets other: %v", err)
			}
			if len(other) != 0 {
				t.Fatalf("expected empty offsets for other consumer, got %v", other)
			}
		})
	}
}

func TestHTTPGroupOffsetsParityClassicAndDiskless(t *testing.T) {
	modes := []string{"classic", "diskless"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			env := camutest.New(t, camutest.WithInstances(1))
			defer env.Cleanup()

			client := env.Client()
			topicA := fmt.Sprintf("group-offsets-a-%s", mode)
			topicB := fmt.Sprintf("group-offsets-b-%s", mode)
			createTopicForMode(t, client, topicA, mode, 2)
			createTopicForMode(t, client, topicB, mode, 1)

			want := map[string]map[int]uint64{
				topicA: {0: 7, 1: 11},
				topicB: {0: 13},
			}
			if err := client.CommitOffsets("group-"+mode, want); err != nil {
				t.Fatalf("CommitOffsets: %v", err)
			}

			got, err := client.GetOffsets("group-" + mode)
			if err != nil {
				t.Fatalf("GetOffsets: %v", err)
			}
			if len(got) != len(want) {
				t.Fatalf("len(topics) = %d, want %d", len(got), len(want))
			}
			for topic, partitions := range want {
				if len(got[topic]) != len(partitions) {
					t.Fatalf("%s len(partitions) = %d, want %d", topic, len(got[topic]), len(partitions))
				}
				for partition, wantOffset := range partitions {
					if got[topic][partition] != wantOffset {
						t.Fatalf("%s partition %d offset = %d, want %d", topic, partition, got[topic][partition], wantOffset)
					}
				}
			}

			empty, err := client.GetOffsets("missing-group-" + mode)
			if err != nil {
				t.Fatalf("GetOffsets missing group: %v", err)
			}
			if len(empty) != 0 {
				t.Fatalf("expected empty offsets for missing group, got %v", empty)
			}
		})
	}
}
