//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func createTopicForMode(t *testing.T, client *camutest.Client, topic, mode string, partitions int) {
	t.Helper()

	switch mode {
	case "classic":
		if err := client.CreateTopic(topic, partitions, 24*time.Hour); err != nil {
			t.Fatalf("CreateTopic(%s): %v", topic, err)
		}
	case "diskless":
		createDisklessTopic(t, client, topic, partitions)
	default:
		t.Fatalf("unknown mode %q", mode)
	}
}

func settleTopicForMode(t *testing.T, client *camutest.Client, topic, mode string, partition int) uint64 {
	t.Helper()

	switch mode {
	case "classic":
		time.Sleep(6 * time.Second)
		return 0
	case "diskless":
		waitForPartitionProduceReady(t, client, topic, partition)
		return 1
	default:
		t.Fatalf("unknown mode %q", mode)
		return 0
	}
}

func settleProduceForMode(t *testing.T, mode string) {
	t.Helper()

	switch mode {
	case "classic":
		time.Sleep(6 * time.Second)
	case "diskless":
		time.Sleep(700 * time.Millisecond)
	default:
		t.Fatalf("unknown mode %q", mode)
	}
}

func TestHTTPConsumeLimitParityClassicAndDiskless(t *testing.T) {
	modes := []string{"classic", "diskless"}
	limits := []int{1, 3}

	for _, mode := range modes {
		for _, limit := range limits {
			t.Run(fmt.Sprintf("%s-limit-%d", mode, limit), func(t *testing.T) {
				env := camutest.New(t, camutest.WithInstances(1))
				defer env.Cleanup()

				client := env.Client()
				topic := fmt.Sprintf("consume-parity-%s-%d", mode, limit)
				createTopicForMode(t, client, topic, mode, 1)

				startOffset := settleTopicForMode(t, client, topic, mode, 0)

				largeValue := makeLargeValue(1500)
				_, err := client.ProduceToPartition(topic, 0, []camutest.ProduceMessage{
					{Key: "k1", Value: largeValue + "-1"},
					{Key: "k2", Value: largeValue + "-2"},
					{Key: "k3", Value: largeValue + "-3"},
				})
				if err != nil {
					t.Fatalf("ProduceToPartition: %v", err)
				}
				settleProduceForMode(t, mode)

				resp, err := client.Consume(topic, 0, startOffset, limit)
				if err != nil {
					t.Fatalf("Consume: %v", err)
				}
				if len(resp.Messages) != limit {
					t.Fatalf("len(messages) = %d, want %d", len(resp.Messages), limit)
				}
				for i, msg := range resp.Messages {
					wantOffset := startOffset + uint64(i)
					if msg.Offset != wantOffset {
						t.Fatalf("message[%d].offset = %d, want %d", i, msg.Offset, wantOffset)
					}
				}
				if resp.NextOffset != startOffset+uint64(limit) {
					t.Fatalf("next_offset = %d, want %d", resp.NextOffset, startOffset+uint64(limit))
				}
			})
		}
	}
}

func TestHTTPSSEIdleParityClassicAndDiskless(t *testing.T) {
	modes := []string{"classic", "diskless"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			env := camutest.New(t, camutest.WithInstances(1))
			defer env.Cleanup()

			client := env.Client()
			topic := "idle-sse-" + mode
			createTopicForMode(t, client, topic, mode, 1)

			switch mode {
			case "classic":
				events, err := client.StreamSSE(topic, 0, 0, 1, 1*time.Second)
				if err != nil {
					t.Fatalf("StreamSSE: %v", err)
				}
				if len(events) != 0 {
					t.Fatalf("len(events) = %d, want 0", len(events))
				}
			case "diskless":
				events := streamDisklessSSE(t, client, topic, 0, 0, "", 1, 1*time.Second)
				if len(events) != 0 {
					t.Fatalf("len(events) = %d, want 0", len(events))
				}
			}
		})
	}
}

func makeLargeValue(size int) string {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = 'x'
	}
	return string(buf)
}
