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
	// Collect schema objects orphaned by a crash mid-registration.
	c.server.schemaRegistry.GCUnreferencedSchemas(ctx)
	// Enqueue pending topic cleanups to the async workers; the GC tick must
	// never block on a long deletion.
	c.server.enqueueTopicDeletions(ctx)
}
