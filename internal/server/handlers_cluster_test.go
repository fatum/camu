package server

import "testing"

func TestEvaluateClusterReadiness(t *testing.T) {
	tests := []struct {
		name                                           string
		active, ready, assigned, initialized, expected int
		unavailable                                    bool
		reasons                                        []string
		status                                         string
		wantReady                                      bool
	}{
		{"all-ready", 3, 3, 12, 12, 12, false, nil, "ready", true},
		{"missing-assignment", 3, 3, 11, 11, 12, false, []string{"assignment not initialized"}, "rebalancing", false},
		{"uninitialized-replica", 3, 3, 12, 11, 12, false, []string{"replica initializing"}, "rebalancing", false},
		{"epoch-mismatch", 3, 3, 12, 11, 12, false, []string{"epoch mismatch"}, "rebalancing", false},
		{"unreachable-node", 3, 2, 12, 10, 12, true, []string{"instance unreachable"}, "unavailable", false},
		{"leader-failover", 3, 3, 12, 10, 12, false, []string{"partition initializing after reassignment"}, "rebalancing", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evaluateClusterReadiness(tt.active, tt.ready, tt.assigned, tt.initialized, tt.expected, tt.unavailable, tt.reasons)
			if got.Status != tt.status || got.Ready != tt.wantReady {
				t.Fatalf("status=%q ready=%v, want %q/%v (response=%+v)", got.Status, got.Ready, tt.status, tt.wantReady, got)
			}
		})
	}
}
