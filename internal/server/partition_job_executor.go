package server

import (
	"context"

	"github.com/maksim/camu/internal/meta"
)

func (s *Server) runPartitionMaintenance(ctx context.Context, topics []meta.TopicConfig) {
	s.partitionLeader().runMaintenance(ctx, topics)
}

func (s *Server) runPartitionJobsForTopic(ctx context.Context, tc meta.TopicConfig) {
	s.partitionLeader().runJobsForTopic(ctx, tc)
}

func (s *Server) runClaimedPartitionJob(ctx context.Context, job PartitionJob) error {
	return s.partitionLeader().runJob(ctx, job)
}
