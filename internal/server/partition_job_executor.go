package server

import (
	"context"

	"github.com/maksim/camu/internal/diskless"
	"github.com/maksim/camu/internal/meta"
)

func (s *Server) runPartitionMaintenance(ctx context.Context, topics []meta.TopicConfig, fileIdx *diskless.FileIndex) {
	s.partitionLeader().runMaintenance(ctx, topics, fileIdx)
}

func (s *Server) runClaimedPartitionJob(ctx context.Context, job PartitionJob) error {
	return s.partitionLeader().runJob(ctx, job)
}

// RunPartitionMaintenanceForTest is a test-only accessor that drives
// one maintenance pass synchronously over the given topics. It is used
// by integration tests to make asynchronous partition maintenance behave
// deterministically instead of waiting on the real periodic tick.
//
// This method is not part of the production API and must not be called
// outside tests.
func (s *Server) RunPartitionMaintenanceForTest(topics []meta.TopicConfig) {
	s.runPartitionMaintenance(context.Background(), topics, nil)
}
