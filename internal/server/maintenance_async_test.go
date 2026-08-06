package server

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/storage"
)

// TestRenewLeases_MaintenancePassRunsAsync verifies the periodic maintenance
// pass (partition jobs, leader GC, orphan sweeps) runs on a background
// goroutine and never blocks the coordination loop: renewLeases returns while
// a pass is still in flight, and the busy guard prevents overlapping passes.
//
// This guards a regression where the diskless orphan sweep's sequential Stat
// loop blocked renewLeases for minutes, letting the node's heartbeat lapse
// past the registry TTL so the leader GC'd it out of the active set.
func TestRenewLeases_MaintenancePassRunsAsync(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Seed a stale instance so the leader's GC tries to delete it during the
	// maintenance pass.
	stale := coordination.InstanceInfo{InstanceID: "n2", Address: "127.0.0.1:9090", HeartbeatAt: time.Now().Add(-24 * time.Hour)}
	data, err := json.Marshal(stale)
	if err != nil {
		t.Fatalf("marshal stale instance: %v", err)
	}
	if err := s.s3Client.Put(ctx, "_coordination/instances/n2.json", data, storage.PutOpts{}); err != nil {
		t.Fatalf("seed stale instance: %v", err)
	}

	// Block the maintenance pass at the GC delete so it stays in flight. The
	// coordination loop itself never deletes, so renewLeases must return while
	// the pass is blocked.
	deleteStarted := make(chan struct{})
	releaseDelete := make(chan struct{})
	var once sync.Once
	s.s3Client.SetFaultInjector(func(op string) error {
		if op == "delete" {
			once.Do(func() { close(deleteStarted) })
			<-releaseDelete
		}
		return nil
	})
	defer s.s3Client.SetFaultInjector(nil)

	// Position the tick counter so the next renewal triggers a pass.
	s.coordinationGCTick = 9

	done := make(chan struct{})
	go func() {
		s.renewLeases()
		close(done)
	}()

	// renewLeases must launch the pass and return without waiting for it.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("renewLeases blocked on the maintenance pass")
	}

	// The pass is still in flight (blocked on the GC delete).
	select {
	case <-deleteStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance pass did not reach the GC delete step")
	}
	if !s.maintenanceBusy.Load() {
		t.Fatal("expected maintenanceBusy to be set while the pass runs")
	}

	// Releasing the delete lets the pass finish and clear the busy flag.
	close(releaseDelete)
	drained := make(chan struct{})
	go func() {
		s.maintenanceWg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance pass did not drain after release")
	}
	if s.maintenanceBusy.Load() {
		t.Fatal("expected maintenanceBusy to be cleared after the pass")
	}
}

// TestRenewLeases_SkipsMaintenanceWhileBusy verifies a slow pass does not pile
// up: the next trigger tick is skipped while the previous pass is running.
func TestRenewLeases_SkipsMaintenanceWhileBusy(t *testing.T) {
	s := newTestServer(t)

	// Simulate an in-flight pass.
	s.maintenanceBusy.Store(true)

	// The next renewal must not start another pass (busy guard) and must not
	// block waiting for it.
	s.coordinationGCTick = 9
	done := make(chan struct{})
	go func() {
		s.renewLeases()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("renewLeases blocked despite the busy guard")
	}

	// Still busy and no pass was registered, so nothing drained it.
	if !s.maintenanceBusy.Load() {
		t.Fatal("expected maintenanceBusy to remain set after a skipped pass")
	}
}
