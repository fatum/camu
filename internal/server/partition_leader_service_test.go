package server

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRunBoundedPartitionTasksHonorsConcurrencyCap(t *testing.T) {
	tasks := []int{1, 2, 3, 4, 5}
	var current int32
	var peak int32
	started := make(chan struct{}, 5)
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		runBoundedPartitionTasks(2, tasks, func(_ int) {
			value := atomic.AddInt32(&current, 1)
			for {
				peakNow := atomic.LoadInt32(&peak)
				if value <= peakNow || atomic.CompareAndSwapInt32(&peak, peakNow, value) {
					break
				}
			}
			started <- struct{}{}
			<-release
			atomic.AddInt32(&current, -1)
		})
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for initial maintenance tasks")
		}
	}

	select {
	case <-started:
		t.Fatal("expected concurrency cap to block third task")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bounded runner to finish")
	}

	if got := atomic.LoadInt32(&peak); got != 2 {
		t.Fatalf("peak concurrency = %d, want 2", got)
	}
}
