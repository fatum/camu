package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
)

// ReplicaFetchHandler processes a replication fetch request and returns the
// response. The handler is called from the server's connection goroutine and
// may block for long-polling. The context is cancelled when the connection
// closes.
type ReplicaFetchHandler func(ctx context.Context, req *ReplicaFetchRequest) (*ReplicaFetchResponse, error)

// ReplicationServer is the leader-side TCP server for the raw replication
// protocol. It accepts persistent connections from followers and processes
// ReplicaFetchRequest frames.
type ReplicationServer struct {
	handler ReplicaFetchHandler
	log     *slog.Logger

	listenerMu sync.Mutex
	listener   net.Listener

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

// NewReplicationServer creates a TCP replication server that dispatches
// requests to the given handler.
func NewReplicationServer(handler ReplicaFetchHandler, log *slog.Logger) *ReplicationServer {
	if log == nil {
		log = slog.Default()
	}
	return &ReplicationServer{
		handler: handler,
		log:     log,
		conns:   make(map[net.Conn]struct{}),
	}
}

// Serve accepts connections on ln and processes replication requests until
// the listener is closed.
func (rs *ReplicationServer) Serve(ln net.Listener) error {
	rs.listenerMu.Lock()
	rs.listener = ln
	rs.listenerMu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			rs.listenerMu.Lock()
			if rs.listener == ln {
				rs.listener = nil
			}
			rs.listenerMu.Unlock()
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go rs.handleConn(conn)
	}
}

// Close stops the listener and closes all active connections.
func (rs *ReplicationServer) Close() error {
	rs.listenerMu.Lock()
	ln := rs.listener
	rs.listener = nil
	rs.listenerMu.Unlock()

	rs.connsMu.Lock()
	for conn := range rs.conns {
		conn.Close()
	}
	rs.conns = nil
	rs.connsMu.Unlock()

	if ln == nil {
		return nil
	}
	return ln.Close()
}

func (rs *ReplicationServer) handleConn(conn net.Conn) {
	rs.trackConn(conn)
	defer func() {
		rs.untrackConn(conn)
		conn.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		req, err := DecodeRequest(conn)
		if err != nil {
			if err != io.EOF {
				rs.log.Debug("replication: read request", "remote", conn.RemoteAddr(), "error", err)
			}
			return
		}

		resp, err := rs.handler(ctx, req)
		if err != nil {
			rs.log.Debug("replication: handler error",
				"remote", conn.RemoteAddr(),
				"topic", req.Topic, "pid", req.PartitionID,
				"error", err)
			resp = &ReplicaFetchResponse{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ReplicaErrInternal,
			}
		}

		if err := rs.writeResponse(conn, resp); err != nil {
			rs.log.Debug("replication: write response",
				"remote", conn.RemoteAddr(), "error", err)
			return
		}
	}
}

// writeResponse writes a replication response frame using net.Buffers for
// zero-copy writev when batch data is present.
func (rs *ReplicationServer) writeResponse(conn net.Conn, resp *ReplicaFetchResponse) error {
	header := EncodeResponseHeader(resp)
	totalPayload := len(header) + len(resp.BatchData)

	var frameLen [4]byte
	binary.BigEndian.PutUint32(frameLen[:], uint32(totalPayload))

	if len(resp.BatchData) > 0 {
		buffers := net.Buffers{frameLen[:], header, resp.BatchData}
		n, err := buffers.WriteTo(conn)
		if err != nil {
			return err
		}
		if n != int64(len(frameLen)+len(header)+len(resp.BatchData)) {
			return io.ErrShortWrite
		}
		return nil
	}

	buffers := net.Buffers{frameLen[:], header}
	n, err := buffers.WriteTo(conn)
	if err != nil {
		return err
	}
	if n != int64(len(frameLen)+len(header)) {
		return io.ErrShortWrite
	}
	return nil
}

func (rs *ReplicationServer) trackConn(conn net.Conn) {
	rs.connsMu.Lock()
	if rs.conns == nil {
		rs.conns = make(map[net.Conn]struct{})
	}
	rs.conns[conn] = struct{}{}
	rs.connsMu.Unlock()
}

func (rs *ReplicationServer) untrackConn(conn net.Conn) {
	rs.connsMu.Lock()
	delete(rs.conns, conn)
	rs.connsMu.Unlock()
}
