package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (ks *KafkaServer) HandleRequest(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	if ks.cfg.RequestHandler != nil {
		return ks.cfg.RequestHandler(req)
	}

	switch req := req.(type) {
	case *kmsg.ApiVersionsRequest:
		return ks.handleAPIVersions(req), nil
	case *kmsg.InitProducerIDRequest:
		return ks.handleInitProducerID(req)
	case *kmsg.FindCoordinatorRequest:
		return ks.handleFindCoordinator(req)
	case *kmsg.DescribeGroupsRequest:
		return ks.handleDescribeGroups(req)
	case *kmsg.ListGroupsRequest:
		return ks.handleListGroups(req)
	case *kmsg.DeleteGroupsRequest:
		return ks.handleDeleteGroups(req)
	case *kmsg.OffsetDeleteRequest:
		return ks.handleOffsetDelete(req)
	case *kmsg.JoinGroupRequest:
		return ks.handleJoinGroup(req)
	case *kmsg.HeartbeatRequest:
		return ks.handleHeartbeat(req)
	case *kmsg.LeaveGroupRequest:
		return ks.handleLeaveGroup(req)
	case *kmsg.SyncGroupRequest:
		return ks.handleSyncGroup(req)
	case *kmsg.OffsetCommitRequest:
		return ks.handleOffsetCommit(req)
	case *kmsg.OffsetFetchRequest:
		return ks.handleOffsetFetch(req)
	case *kmsg.ListOffsetsRequest:
		return ks.handleListOffsets(req)
	case *kmsg.MetadataRequest:
		return ks.handleMetadata(req)
	case *kmsg.CreateTopicsRequest:
		return ks.handleCreateTopics(req)
	case *kmsg.DeleteTopicsRequest:
		return ks.handleDeleteTopics(req)
	case *kmsg.CreatePartitionsRequest:
		return ks.handleCreatePartitions(req)
	case *kmsg.DescribeConfigsRequest:
		return ks.handleDescribeConfigs(req)
	case *kmsg.AlterConfigsRequest:
		return ks.handleAlterConfigs(req)
	case *kmsg.IncrementalAlterConfigsRequest:
		return ks.handleIncrementalAlterConfigs(req)
	case *kmsg.DescribeClusterRequest:
		return ks.handleDescribeCluster(req)
	case *kmsg.DescribeACLsRequest:
		return ks.handleDescribeACLs(req)
	case *kmsg.CreateACLsRequest:
		return ks.handleCreateACLs(req)
	case *kmsg.DeleteACLsRequest:
		return ks.handleDeleteACLs(req)
	case *kmsg.ProduceRequest:
		return ks.handleProduce(req)
	case *kmsg.FetchRequest:
		return ks.handleFetch(req)
	default:
		return nil, fmt.Errorf("unsupported API key: %d", req.Key())
	}
}

func (ks *KafkaServer) HandleConn(conn net.Conn, _ net.Listener) {
	defer conn.Close()

	var reqBuf []byte

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			if err != io.EOF {
				ks.log.Debug("kafka read length", "error", err)
			}
			return
		}

		reader := kbin.Reader{Src: lenBuf[:]}
		length := int(reader.Int32())
		if length <= 0 || length > maxKafkaRequestSize {
			ks.log.Debug("kafka request size invalid", "length", length)
			return
		}

		if cap(reqBuf) < length {
			reqBuf = make([]byte, length)
		}
		reqBuf = reqBuf[:length]
		if _, err := io.ReadFull(conn, reqBuf); err != nil {
			ks.log.Debug("kafka read body", "error", err)
			return
		}

		correlationID, req, err := decodeKafkaRequest(reqBuf)
		if err != nil {
			ks.log.Debug("kafka decode", "error", err)
			return
		}

		resp, err := ks.HandleRequest(context.Background(), req)
		if err != nil {
			ks.log.Debug("kafka handle", "key", req.Key(), "error", err)
			return
		}

		if err := writeKafkaResponse(conn, correlationID, req, resp); err != nil {
			ks.log.Debug("kafka write", "error", err)
			return
		}
	}
}

func (ks *KafkaServer) StartListener(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	ks.listenerMu.Lock()
	ks.listener = ln
	ks.listenerMu.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			ks.listenerMu.Lock()
			if ks.listener == ln {
				ks.listener = nil
			}
			ks.listenerMu.Unlock()
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go ks.HandleConn(conn, ln)
	}
}

func (ks *KafkaServer) Close() error {
	ks.listenerMu.Lock()
	ln := ks.listener
	ks.listener = nil
	ks.listenerMu.Unlock()
	if ln == nil {
		return nil
	}
	return ln.Close()
}

func decodeKafkaRequest(buf []byte) (int32, kmsg.Request, error) {
	if len(buf) < 8 {
		return 0, nil, fmt.Errorf("request too short")
	}

	reader := kbin.Reader{Src: buf}
	apiKey := reader.Int16()
	apiVersion := reader.Int16()
	correlationID := reader.Int32()
	if err := reader.Complete(); err != nil && len(reader.Src) == 0 {
		return 0, nil, err
	}

	body := reader.Src
	if decoded, err := decodeKafkaRequestBody(apiKey, apiVersion, body, true); err == nil {
		return correlationID, decoded, nil
	}

	decoded, err := decodeKafkaRequestBody(apiKey, apiVersion, body, false)
	if err != nil {
		return 0, nil, err
	}
	return correlationID, decoded, nil
}

func decodeKafkaRequestBody(apiKey, apiVersion int16, body []byte, withHeader bool) (kmsg.Request, error) {
	req, err := newKafkaRequest(apiKey, apiVersion)
	if err != nil {
		return nil, err
	}

	if withHeader && !(apiKey == 7 && apiVersion == 0) {
		reader := kbin.Reader{Src: body}
		_ = reader.NullableString()
		if req.IsFlexible() {
			kmsg.SkipTags(&reader)
		}
		if err := reader.Complete(); err != nil && len(reader.Src) == 0 {
			return nil, err
		}
		body = reader.Src
	}

	if err := req.ReadFrom(body); err != nil {
		return nil, err
	}
	return req, nil
}

func newKafkaRequest(apiKey, apiVersion int16) (kmsg.Request, error) {
	req := kmsg.RequestForKey(apiKey)
	if req == nil {
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
	req.SetVersion(apiVersion)
	return req, nil
}

func writeKafkaResponse(w io.Writer, correlationID int32, req kmsg.Request, resp kmsg.Response) error {
	setKafkaResponseVersion(resp, req.GetVersion())

	payload := make([]byte, 0, 8+1+len(resp.AppendTo(nil)))
	payload = kbin.AppendInt32(payload, correlationID)
	if req.IsFlexible() && req.Key() != 18 {
		payload = append(payload, 0)
	}
	payload = resp.AppendTo(payload)

	var frame []byte
	frame = kbin.AppendInt32(frame, int32(len(payload)))
	frame = append(frame, payload...)
	_, err := w.Write(frame)
	return err
}
