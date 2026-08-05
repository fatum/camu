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
	// Enqueue pending topic cleanups to the async workers; the GC tick must
	// never block on a long deletion.
	c.server.enqueueTopicDeletions(ctx)
}
