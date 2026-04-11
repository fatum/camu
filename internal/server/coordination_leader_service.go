package server

import (
	"context"

	"github.com/maksim/camu/internal/meta"
)

type coordinationLeaderService struct {
	server *Server
}

func (s *Server) coordinationLeader() coordinationLeaderService {
	return coordinationLeaderService{server: s}
}

func (c coordinationLeaderService) runGC(ctx context.Context, topics []meta.TopicConfig) {
	c.server.gcStaleInstances(ctx)
	c.server.gcStaleISR(ctx, topics)
	c.server.gcPendingTopicDeletions(ctx)
}
