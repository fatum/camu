package coordination

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/maksim/camu/internal/storage"
)

// mockStorageClient is a simple in-memory StorageClient for tests.
type mockStorageClient struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMockStorageClient() *mockStorageClient {
	return &mockStorageClient{data: make(map[string][]byte)}
}

func (m *mockStorageClient) Put(_ context.Context, key string, data []byte, _ storage.PutOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}

func (m *mockStorageClient) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

// sampleMeta returns a PartitionMeta for testing.
func sampleMeta(leader string, epoch uint64, hw uint64) *PartitionMeta {
	return &PartitionMeta{
		Leader:   leader,
		Epoch:    epoch,
		Replicas: []string{"A", "B", "C"},
		ISR:      []string{leader, "B"},
		HW:       hw,
		EpochHistory: []EpochEntry{
			{Epoch: 1, StartOffset: 0},
			{Epoch: epoch, StartOffset: hw - 10},
		},
	}
}

func TestMarshalUnmarshalCheckpoint(t *testing.T) {
	cs := NewControllerState()
	cs.SetPartition("orders", 0, sampleMeta("A", 3, 500))
	cs.SetPartition("orders", 1, sampleMeta("B", 2, 300))
	cs.SetPartition("payments", 0, sampleMeta("C", 5, 1000))
	cs.SetPartition("payments", 2, sampleMeta("A", 1, 50))

	data, err := MarshalCheckpoint(cs)
	if err != nil {
		t.Fatalf("MarshalCheckpoint: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty JSON output")
	}

	cs2 := NewControllerState()
	if err := UnmarshalCheckpoint(data, cs2); err != nil {
		t.Fatalf("UnmarshalCheckpoint: %v", err)
	}

	type testCase struct {
		topic string
		pid   int
		want  *PartitionMeta
	}
	cases := []testCase{
		{"orders", 0, sampleMeta("A", 3, 500)},
		{"orders", 1, sampleMeta("B", 2, 300)},
		{"payments", 0, sampleMeta("C", 5, 1000)},
		{"payments", 2, sampleMeta("A", 1, 50)},
	}
	for _, tc := range cases {
		got := cs2.GetPartition(tc.topic, tc.pid)
		if got == nil {
			t.Errorf("GetPartition(%q, %d) = nil, want non-nil", tc.topic, tc.pid)
			continue
		}
		if got.Leader != tc.want.Leader {
			t.Errorf("[%s/%d] Leader: got %q, want %q", tc.topic, tc.pid, got.Leader, tc.want.Leader)
		}
		if got.Epoch != tc.want.Epoch {
			t.Errorf("[%s/%d] Epoch: got %d, want %d", tc.topic, tc.pid, got.Epoch, tc.want.Epoch)
		}
		if got.HW != tc.want.HW {
			t.Errorf("[%s/%d] HW: got %d, want %d", tc.topic, tc.pid, got.HW, tc.want.HW)
		}
		if len(got.Replicas) != len(tc.want.Replicas) {
			t.Errorf("[%s/%d] Replicas len: got %d, want %d", tc.topic, tc.pid, len(got.Replicas), len(tc.want.Replicas))
		}
		if len(got.ISR) != len(tc.want.ISR) {
			t.Errorf("[%s/%d] ISR len: got %d, want %d", tc.topic, tc.pid, len(got.ISR), len(tc.want.ISR))
		}
		if len(got.EpochHistory) != len(tc.want.EpochHistory) {
			t.Errorf("[%s/%d] EpochHistory len: got %d, want %d", tc.topic, tc.pid, len(got.EpochHistory), len(tc.want.EpochHistory))
		}
	}
}

func TestCheckpoint_S3RoundTrip(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorageClient()

	cs := NewControllerState()
	cs.SetPartition("logs", 0, sampleMeta("A", 7, 9999))
	cs.SetPartition("logs", 1, sampleMeta("B", 3, 4000))

	if err := WriteCheckpoint(ctx, mock, cs); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	// Verify the key was written.
	if _, err := mock.Get(ctx, checkpointKey); err != nil {
		t.Fatalf("expected checkpoint key %q to exist: %v", checkpointKey, err)
	}

	cs2 := NewControllerState()
	if err := ReadCheckpoint(ctx, mock, cs2); err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}

	for _, pid := range []int{0, 1} {
		orig := cs.GetPartition("logs", pid)
		got := cs2.GetPartition("logs", pid)
		if got == nil {
			t.Errorf("GetPartition(logs, %d) = nil after round-trip", pid)
			continue
		}
		if got.Leader != orig.Leader || got.Epoch != orig.Epoch || got.HW != orig.HW {
			t.Errorf("logs/%d mismatch: got {leader:%q epoch:%d hw:%d}, want {leader:%q epoch:%d hw:%d}",
				pid, got.Leader, got.Epoch, got.HW, orig.Leader, orig.Epoch, orig.HW)
		}
	}
}

func TestUnmarshalCheckpoint_Empty(t *testing.T) {
	cs := NewControllerState()
	if err := UnmarshalCheckpoint([]byte(`{}`), cs); err != nil {
		t.Fatalf("unexpected error on empty object: %v", err)
	}
	all := cs.AllPartitions()
	if len(all) != 0 {
		t.Errorf("expected empty state, got %d topics", len(all))
	}
}

func TestMarshalCheckpoint_LargeState(t *testing.T) {
	cs := NewControllerState()
	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("topic-%03d", i)
		for pid := 0; pid < 4; pid++ {
			cs.SetPartition(topic, pid, sampleMeta("A", uint64(i+1), uint64(pid*1000)))
		}
	}

	data, err := MarshalCheckpoint(cs)
	if err != nil {
		t.Fatalf("MarshalCheckpoint on large state: %v", err)
	}
	if len(data) < 100 {
		t.Errorf("expected substantial JSON output, got %d bytes", len(data))
	}

	// Verify round-trip integrity by unmarshalling and spot-checking.
	cs2 := NewControllerState()
	if err := UnmarshalCheckpoint(data, cs2); err != nil {
		t.Fatalf("UnmarshalCheckpoint on large state: %v", err)
	}
	all := cs2.AllPartitions()
	if len(all) != 100 {
		t.Errorf("expected 100 topics, got %d", len(all))
	}
	for _, pids := range all {
		if len(pids) != 4 {
			t.Errorf("expected 4 partitions per topic, got %d", len(pids))
		}
	}
}
