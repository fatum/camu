package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maksim/camu/internal/coordination"
	"github.com/maksim/camu/internal/storage"
)

// newTestServerForReportFailure builds a minimal Server backed by an in-memory
// S3 store. Both leaderElection and registry share the same S3 instance so
// helper writes are immediately visible.
func newTestServerForReportFailure(t *testing.T) (*Server, *storage.S3Client) {
	t.Helper()
	s3Client, err := storage.NewS3Client(storage.S3Config{
		Bucket:   "test",
		Endpoint: "memory://",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	instanceID := "test-instance"
	s := &Server{
		instanceID:     instanceID,
		leaderElection: coordination.NewLeaderElection(s3Client, instanceID, 30*time.Second),
		registry:       coordination.NewRegistry(s3Client, instanceID, "localhost:8080", "localhost:8081", "", 30*time.Second),
		internalClient: &http.Client{Timeout: 5 * time.Second},
		partitionManager: &PartitionManager{
			partitions: make(map[string]map[int]*partitionState),
		},
	}
	return s, s3Client
}

// acquireLeaseAs writes a valid controller lease for leaderID using the shared
// S3 store.
func acquireLeaseAs(t *testing.T, s3Client *storage.S3Client, leaderID string) {
	t.Helper()
	le := coordination.NewLeaderElection(s3Client, leaderID, 30*time.Second)
	if _, _, err := le.TryAcquire(context.Background()); err != nil {
		t.Fatalf("acquireLeaseAs %s: %v", leaderID, err)
	}
}

// registerInstance writes an InstanceInfo for instanceID with the given
// internalAddress into the shared S3 store.
func registerInstance(t *testing.T, s3Client *storage.S3Client, instanceID, internalAddr string) {
	t.Helper()
	reg := coordination.NewRegistry(s3Client, instanceID, "localhost:8080", internalAddr, "", 30*time.Second)
	if err := reg.Register(context.Background()); err != nil {
		t.Fatalf("registerInstance %s: %v", instanceID, err)
	}
}

// ---------------------------------------------------------------------------
// getControllerAddr
// ---------------------------------------------------------------------------

func TestGetControllerAddr_NoLease(t *testing.T) {
	s, _ := newTestServerForReportFailure(t)
	// No lease written → GetLeader returns error → addr must be "".
	addr := s.getControllerAddr()
	if addr != "" {
		t.Errorf("expected empty addr when no lease, got %q", addr)
	}
}

func TestGetControllerAddr_SelfIsController(t *testing.T) {
	s, s3 := newTestServerForReportFailure(t)
	// Acquire lease as this instance → we are the controller.
	acquireLeaseAs(t, s3, s.instanceID)

	addr := s.getControllerAddr()
	if addr != "" {
		t.Errorf("expected empty addr when self is controller, got %q", addr)
	}
}

func TestGetControllerAddr_RemoteController(t *testing.T) {
	s, s3 := newTestServerForReportFailure(t)
	s.instanceID = "node-follower"
	// Re-wire leaderElection so its instanceID matches.
	s.leaderElection = coordination.NewLeaderElection(s3, "node-follower", 30*time.Second)

	controllerID := "node-controller"
	acquireLeaseAs(t, s3, controllerID)
	registerInstance(t, s3, controllerID, "controller-host:8081")

	addr := s.getControllerAddr()
	if addr != "controller-host:8081" {
		t.Errorf("expected controller-host:8081, got %q", addr)
	}
}

// ---------------------------------------------------------------------------
// reportFailureToController
// ---------------------------------------------------------------------------

func TestReportFailureToController_NoController(t *testing.T) {
	s, _ := newTestServerForReportFailure(t)
	// No lease → getControllerAddr returns "" → immediate error.
	if err := s.reportFailureToController("my-topic", 0); err == nil {
		t.Fatal("expected error when no controller available, got nil")
	}
}

func TestReportFailureToController_Success(t *testing.T) {
	received := make(chan map[string]any, 1)
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/report-failure" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		received <- body
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ctrl.Close()

	s, s3 := newTestServerForReportFailure(t)
	s.instanceID = "node-follower"
	s.leaderElection = coordination.NewLeaderElection(s3, "node-follower", 30*time.Second)

	controllerID := "node-controller"
	acquireLeaseAs(t, s3, controllerID)
	// InternalAddress must be host:port (no "http://" prefix).
	registerInstance(t, s3, controllerID, ctrl.Listener.Addr().String())

	if err := s.reportFailureToController("my-topic", 3); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case body := <-received:
		if body["topic"] != "my-topic" {
			t.Errorf("topic = %v, want my-topic", body["topic"])
		}
		if int(body["partition"].(float64)) != 3 {
			t.Errorf("partition = %v, want 3", body["partition"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("controller never received request")
	}
}

func TestReportFailureToController_Non200(t *testing.T) {
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer ctrl.Close()

	s, s3 := newTestServerForReportFailure(t)
	s.instanceID = "node-follower"
	s.leaderElection = coordination.NewLeaderElection(s3, "node-follower", 30*time.Second)

	controllerID := "node-controller"
	acquireLeaseAs(t, s3, controllerID)
	registerInstance(t, s3, controllerID, ctrl.Listener.Addr().String())

	if err := s.reportFailureToController("my-topic", 0); err == nil {
		t.Fatal("expected error for non-200 response, got nil")
	}
}
