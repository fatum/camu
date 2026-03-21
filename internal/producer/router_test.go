package producer

import "testing"

func TestRouter_KeyRouting(t *testing.T) {
	r := NewRouter(4)

	key := []byte("order-123")
	first := r.Route(key)

	if first < 0 || first >= 4 {
		t.Fatalf("partition %d out of range [0, 4)", first)
	}

	for i := 0; i < 10; i++ {
		got := r.Route(key)
		if got != first {
			t.Fatalf("expected consistent partition %d for key, got %d on iteration %d", first, got, i)
		}
	}
}

func TestRouter_NilKeyRoundRobin(t *testing.T) {
	r := NewRouter(4)

	seen := make(map[int]bool)
	for i := 0; i < 8; i++ {
		p := r.Route(nil)
		if p < 0 || p >= 4 {
			t.Fatalf("partition %d out of range [0, 4)", p)
		}
		seen[p] = true
	}

	if len(seen) < 2 {
		t.Fatalf("expected at least 2 different partitions in 8 tries, got %d", len(seen))
	}
}

func TestRouter_SinglePartition(t *testing.T) {
	r := NewRouter(1)

	for i := 0; i < 5; i++ {
		if p := r.Route([]byte("any-key")); p != 0 {
			t.Fatalf("expected partition 0, got %d", p)
		}
		if p := r.Route(nil); p != 0 {
			t.Fatalf("expected partition 0 for nil key, got %d", p)
		}
	}
}
