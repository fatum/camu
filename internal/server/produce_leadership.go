package server

import (
	"errors"
	"net/http"

	"github.com/maksim/camu/internal/replication"
)

// proxyOrRejectNotLeader proxies the request to the partition leader if known,
// otherwise responds with a 421 routing map.
func (s *Server) proxyOrRejectNotLeader(w http.ResponseWriter, r *http.Request, topicName string, partitionID int) {
	if leaderAddr := s.leaderInternalAddr(topicName, partitionID); leaderAddr != "" && r.Header.Get(headerForwardedBy) == "" {
		s.proxyToLeader(w, r, leaderAddr)
		return
	}
	writeJSON(w, 421, s.getRoutingMap(topicName))
}

// writeReplicationError writes the appropriate HTTP error for a replication
// failure. Returns true if an error was written (err was non-nil).
func writeReplicationError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, replication.ErrReplicationTimeout) {
		writeError(w, 408, "replication timeout")
		return true
	}
	writeError(w, http.StatusInternalServerError, "replication error: "+err.Error())
	return true
}
