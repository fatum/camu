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

// ReplicaFetchResult is the outcome of a replication fetch handler call.
// When BatchReader is non-nil, the server will write the response header and
// then stream BatchReader to the connection (via io.Copy which triggers
// sendfile when the reader is backed by a file). When BatchReader is nil,
// BatchData is used (for small responses or error responses).
type ReplicaFetchResult struct {
	Resp        *ReplicaFetchResponse
	BatchReader io.Reader // optional: streamed after header (sendfile path)
	BatchLen    int32     // length of BatchReader data; must match when set
}

// ReplicaFetchHandler processes a replication fetch request and returns the
// result. The handler is called from the server's connection goroutine and
// may block for long-polling. The context is cancelled when the connection
// closes.
type ReplicaFetchHandler func(ctx context.Context, req *ReplicaFetchRequest) (*ReplicaFetchResult, error)

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
		_ = conn.Close()
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
		_ = conn.Close()
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

		result, err := rs.handler(ctx, req)
		if err != nil {
			rs.log.Debug("replication: handler error",
				"remote", conn.RemoteAddr(),
				"topic", req.Topic, "pid", req.PartitionID,
				"error", err)
			result = &ReplicaFetchResult{
				Resp: &ReplicaFetchResponse{
					CorrelationID: req.CorrelationID,
					ErrorCode:     ReplicaErrInternal,
				},
			}
		}

		if err := rs.writeResult(conn, result); err != nil {
			rs.log.Debug("replication: write response",
				"remote", conn.RemoteAddr(), "error", err)
			return
		}
	}
}

// writeResult writes a replication response frame. When result.BatchReader is
// non-nil, the header is written first (with BatchLen in the data-length
// field), then the batch data is streamed via io.Copy. When the reader is an
// io.ReaderAt (e.g. io.NewSectionReader over *os.File), Go's net.TCPConn
// ReadFrom triggers sendfile for zero-copy file→socket transfer.
func (rs *ReplicationServer) writeResult(conn net.Conn, result *ReplicaFetchResult) error {
	resp := result.Resp
	if result.BatchReader != nil {
		resp.BatchDataLen = result.BatchLen
		resp.BatchData = nil
	} else if resp.BatchData != nil {
		resp.BatchDataLen = int32(len(resp.BatchData))
	}

	header := EncodeResponseHeader(resp)
	headerLen := len(header)

	if result.BatchReader != nil {
		totalPayload := int32(headerLen) + result.BatchLen
		var frameLen [4]byte
		binary.BigEndian.PutUint32(frameLen[:], uint32(totalPayload))

		// Write frame length + header.
		if _, err := conn.Write(frameLen[:]); err != nil {
			return err
		}
		if _, err := conn.Write(header); err != nil {
			return err
		}
		// Stream batch data. io.Copy with a *net.TCPConn uses ReadFrom,
		// which invokes sendfile when the reader is file-backed.
		if _, err := io.Copy(conn, result.BatchReader); err != nil {
			return err
		}
		return nil
	}

	// Non-streaming path: BatchData is nil or empty.
	totalPayload := int32(headerLen)
	var frameLen [4]byte
	binary.BigEndian.PutUint32(frameLen[:], uint32(totalPayload))

	if len(resp.BatchData) > 0 {
		buffers := net.Buffers{frameLen[:], header, resp.BatchData}
		n, err := buffers.WriteTo(conn)
		if err != nil {
			return err
		}
		if n != int64(len(frameLen)+headerLen+len(resp.BatchData)) {
			return io.ErrShortWrite
		}
		return nil
	}

	buffers := net.Buffers{frameLen[:], header}
	n, err := buffers.WriteTo(conn)
	if err != nil {
		return err
	}
	if n != int64(len(frameLen)+headerLen) {
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
