//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestTopicCRUD(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	// Create
	err := client.CreateTopic("test-topic", 4, 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}

	// Get
	topic, err := client.GetTopic("test-topic")
	if err != nil {
		t.Fatalf("GetTopic() error: %v", err)
	}
	if topic.Partitions != 4 {
		t.Errorf("Partitions = %d, want 4", topic.Partitions)
	}

	// List
	topics, err := client.ListTopics()
	if err != nil {
		t.Fatalf("ListTopics() error: %v", err)
	}
	found := false
	for _, tp := range topics {
		if tp.Name == "test-topic" {
			found = true
		}
	}
	if !found {
		t.Error("test-topic not found in list")
	}

	// Delete
	err = client.DeleteTopic("test-topic")
	if err != nil {
		t.Fatalf("DeleteTopic() error: %v", err)
	}
	_, err = client.GetTopic("test-topic")
	if err == nil {
		t.Error("GetTopic() after delete should error")
	}
}

func TestTopicCreateDuplicate(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	err := client.CreateTopic("dup-topic", 1, time.Hour)
	if err != nil {
		t.Fatalf("first CreateTopic() error: %v", err)
	}
	err = client.CreateTopic("dup-topic", 1, time.Hour)
	if err == nil {
		t.Error("creating duplicate topic should error")
	}
}

func TestClusterStatus(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(1))
	defer env.Cleanup()
	client := env.Client()

	status, err := client.ClusterStatus()
	if err != nil {
		t.Fatalf("ClusterStatus() error: %v", err)
	}
	if len(status.Instances) != 1 {
		t.Errorf("expected 1 instance, got %d", len(status.Instances))
	}
}
