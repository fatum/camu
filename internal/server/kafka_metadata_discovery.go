package server

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/log"
)

func (s *Server) handleKafkaMetadata(ctx context.Context, req *kmsg.MetadataRequest) (*kmsg.MetadataResponse, error) {
	resp := kmsg.NewPtrMetadataResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	instanceInfos, err := s.registry.ActiveInstanceInfos(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(instanceInfos, func(i, j int) bool {
		return instanceInfos[i].InstanceID < instanceInfos[j].InstanceID
	})
	brokerIDs := make(map[string]int32, len(instanceInfos))
	for _, info := range instanceInfos {
		if info.KafkaAddress == "" {
			continue
		}
		brokerID := kafkaBrokerID(info.InstanceID)
		brokerIDs[info.InstanceID] = brokerID
		host, port := splitKafkaBrokerAddr(info.KafkaAddress)
		resp.Brokers = append(resp.Brokers, kmsg.MetadataResponseBroker{
			NodeID: brokerID,
			Host:   host,
			Port:   port,
		})
	}

	// Use the cached leader lease if available (leader node always has it).
	// Non-leader nodes fall back to an S3 lookup so the AdminClient can
	// route controller-bound requests (CreateTopics, etc.) correctly.
	if lease := s.leaderLease.Load(); lease != nil && lease.InstanceID != "" {
		resp.ControllerID = kafkaBrokerID(lease.InstanceID)
	} else if lease, err := s.leaderElection.GetLeader(ctx); err == nil && lease.InstanceID != "" {
		resp.ControllerID = kafkaBrokerID(lease.InstanceID)
	} else {
		resp.ControllerID = kafkaBrokerID(s.instanceID)
	}

	requested := requestedMetadataTopics(req)
	seenTopics := make(map[string]struct{}, len(requested))
	// Use cached topic list to avoid S3 round-trips on every Metadata request.
	// The cache is refreshed every ~10s by renewLeases and on startup.
	// Fall back to S3 if a requested topic is missing from cache (may have been
	// created on another node).
	topics := s.topicStore.ListCached()
	if len(requested) > 0 {
		cached := make(map[string]struct{}, len(topics))
		for _, t := range topics {
			cached[t.Name] = struct{}{}
		}
		missing := false
		for name := range requested {
			if _, ok := cached[name]; !ok {
				missing = true
				break
			}
		}
		if missing {
			if fresh, err := s.topicStore.List(ctx); err == nil {
				topics = fresh
			}
		}
	}
	for _, topic := range topics {
		if len(requested) > 0 && !requested[topic.Name] {
			continue
		}

		assignments, err := s.assignmentStore.Read(ctx, topic.Name)
		if err != nil {
			continue
		}

		topicResp := kmsg.NewMetadataResponseTopic()
		topicResp.Topic = kmsg.StringPtr(topic.Name)
		for partitionID := 0; partitionID < topic.Partitions; partitionID++ {
			assignment, ok := assignments.Partitions[partitionID]
			if !ok {
				continue
			}

			partResp := kmsg.NewMetadataResponseTopicPartition()
			partResp.Partition = int32(partitionID)
			partResp.Leader = brokerIDs[assignment.Leader]
			for _, replica := range assignment.Replicas {
				if brokerID, ok := brokerIDs[replica]; ok {
					partResp.Replicas = append(partResp.Replicas, brokerID)
				}
			}

			isrState, err := s.isrStore.Read(ctx, topic.Name, partitionID)
			if err == nil {
				for _, isrReplica := range isrState.ISR {
					if brokerID, ok := brokerIDs[isrReplica]; ok {
						partResp.ISR = append(partResp.ISR, brokerID)
					}
				}
			}
			if len(partResp.ISR) == 0 {
				partResp.ISR = append(partResp.ISR, partResp.Replicas...)
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
		seenTopics[topic.Name] = struct{}{}
	}

	for topicName := range requested {
		if _, ok := seenTopics[topicName]; ok {
			continue
		}
		topicResp := kmsg.NewMetadataResponseTopic()
		topicResp.Topic = kmsg.StringPtr(topicName)
		topicResp.ErrorCode = kafkaErrorUnknownTopicPartition
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

func (s *Server) handleKafkaFindCoordinator(_ context.Context, req *kmsg.FindCoordinatorRequest) (*kmsg.FindCoordinatorResponse, error) {
	resp := kmsg.NewPtrFindCoordinatorResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	if req.GetVersion() >= 4 {
		keys := req.CoordinatorKeys
		if len(keys) == 0 && req.CoordinatorKey != "" {
			keys = []string{req.CoordinatorKey}
		}
		for _, key := range keys {
			brokerID, host, port := s.kafkaControllerBroker(context.Background())
			resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
				Key:    key,
				NodeID: brokerID,
				Host:   host,
				Port:   port,
			})
		}
		return resp, nil
	}

	brokerID, host, port := s.kafkaControllerBroker(context.Background())
	resp.NodeID = brokerID
	resp.Host = host
	resp.Port = port
	return resp, nil
}

func (s *Server) kafkaControllerBroker(ctx context.Context) (int32, string, int32) {
	lease, err := s.leaderElection.GetLeader(ctx)
	if err == nil && lease.InstanceID != "" && time.Now().Before(lease.ExpiresAt) {
		info, infoErr := s.registry.GetInstanceInfo(ctx, lease.InstanceID)
		if infoErr == nil && info.KafkaAddress != "" {
			host, port := splitKafkaBrokerAddr(info.KafkaAddress)
			return kafkaBrokerID(info.InstanceID), host, port
		}
	}

	host, port := splitKafkaBrokerAddr(kafkaAdvertiseAddr(s.instanceID, s.Address(), s.cfg.Server.KafkaPort, s.cfg.Server.KafkaAdvertiseAddress))
	return kafkaBrokerID(s.instanceID), host, port
}

func (s *Server) handleKafkaListOffsets(ctx context.Context, topic string, partition int, timestamp int64) (KafkaOffsetResponse, error) {
	topicCfg, err := s.topicStore.Get(ctx, topic)
	if err != nil {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: topic %q", errKafkaUnknownTopicPartition, topic)
	}
	if partition < 0 || partition >= topicCfg.Partitions {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}

	if topicCfg.StorageMode == "diskless" {
		disklessStart, startErr := s.disklessMeta.GetPartitionStart(ctx, topic, partition)
		if startErr != nil {
			return KafkaOffsetResponse{}, startErr
		}
		_, disklessHead, headErr := s.disklessEngine.Fetch(ctx, topic, partition, 0, 0)
		if headErr != nil {
			return KafkaOffsetResponse{}, headErr
		}
		switch timestamp {
		case -2:
			return KafkaOffsetResponse{Offset: disklessStart, Timestamp: -1, LeaderEpoch: 0}, nil
		case -4, -1:
			return KafkaOffsetResponse{Offset: disklessHead, Timestamp: -1, LeaderEpoch: 0}, nil
		default:
			return KafkaOffsetResponse{}, fmt.Errorf("%w: timestamp lookup unsupported for diskless topics", errKafkaInvalidRequest)
		}
	}

	ps := s.partitionManager.GetPartitionState(topic, partition)
	if ps == nil {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: partition %d for topic %q", errKafkaUnknownTopicPartition, partition, topic)
	}
	if topicCfg.ReplicationFactor > 1 && ps.replicaState == nil {
		return KafkaOffsetResponse{}, fmt.Errorf("%w: partition %d", errKafkaLeaderNotAvailable, partition)
	}

	logStartOffset := uint64(0)
	if firstOffset, ok := ps.index.FirstOffset(); ok {
		logStartOffset = firstOffset
	}
	ps.mu.RLock()
	if seg := ps.activeSegment; seg != nil {
		offsetIdx := seg.OffsetIndex()
		if len(offsetIdx) > 0 && (logStartOffset == 0 || uint64(offsetIdx[0].BaseOffset) < logStartOffset) {
			logStartOffset = uint64(offsetIdx[0].BaseOffset)
		}
	}
	nextOffset := ps.nextOffset
	ps.mu.RUnlock()

	switch timestamp {
	case -2:
		return KafkaOffsetResponse{Offset: int64(logStartOffset), Timestamp: -1, LeaderEpoch: int32(ps.epoch)}, nil
	case -4, -1:
		return KafkaOffsetResponse{Offset: int64(nextOffset), Timestamp: -1, LeaderEpoch: int32(ps.epoch)}, nil
	default:
		startOffset, ok := ps.index.FirstOffsetForTimestamp(timestamp)
		if !ok {
			startOffset = logStartOffset
		}
		if offset, found, err := s.findKafkaOffsetByTimestamp(ctx, ps, startOffset, timestamp); err != nil {
			return KafkaOffsetResponse{}, err
		} else if found {
			return KafkaOffsetResponse{Offset: int64(offset), Timestamp: timestamp, LeaderEpoch: int32(ps.epoch)}, nil
		}
		return KafkaOffsetResponse{Offset: -1, Timestamp: -1, LeaderEpoch: int32(ps.epoch)}, nil
	}
}

func (s *Server) findKafkaOffsetByTimestamp(ctx context.Context, ps *partitionState, startOffset uint64, targetTimestamp int64) (uint64, bool, error) {
	index := ps.index
	if index != nil {
		segments := index.SegmentsFrom(startOffset, 0)
		for _, seg := range segments {
			if seg.MaxTimestamp > 0 && normalizeTimestampForKafkaMillis(seg.MaxTimestamp) < targetTimestamp {
				continue
			}

			if offset, found, err := s.findTimestampInSealedSegment(ctx, seg, startOffset, targetTimestamp); err != nil {
				return 0, false, err
			} else if found {
				return offset, true, nil
			}
		}
	}

	ps.mu.RLock()
	activeSeg := ps.activeSegment
	hw, hwOK := readableHighWatermark(ps)
	ps.mu.RUnlock()
	if activeSeg != nil {
		if offset, found, err := s.findTimestampInActiveSegment(activeSeg, startOffset, targetTimestamp, hw, hwOK); err != nil {
			return 0, false, err
		} else if found {
			return offset, true, nil
		}
	}
	return 0, false, nil
}

func (s *Server) findTimestampInSealedSegment(ctx context.Context, seg log.SegmentRef, startOffset uint64, targetTimestamp int64) (uint64, bool, error) {
	segData, err := s.partitionManager.readSealedSegmentData(ctx, seg)
	if err != nil {
		return 0, false, err
	}

	scanPos := 0
	if sidecarData, err := s.partitionManager.readSealedSegmentSidecar(ctx, seg); err == nil {
		if entries, tsEntries, err := log.ReadSidecar(sidecarData); err == nil {
			if tsOffset, ok := log.LookupTimestampOffset(tsEntries, targetTimestamp); ok {
				seekOffset := tsOffset
				if uint64(seekOffset) < startOffset {
					seekOffset = int64(startOffset)
				}
				if pos, ok := log.LookupSidecarPosition(entries, seekOffset); ok {
					scanPos = int(pos)
				}
			}
		}
	}

	rawBatches, err := log.ReadSegmentBatchesFromPosition(segData, scanPos, startOffset, 0)
	if err != nil {
		return 0, false, err
	}
	for _, raw := range rawBatches {
		if hdr, err := log.ReadRecordBatchHeader(raw); err == nil {
			if normalizeTimestampForKafkaMillis(hdr.MaxTimestamp) < targetTimestamp {
				continue
			}
		}
		msgs, err := log.DecodeRecordBatch(raw)
		if err != nil {
			return 0, false, err
		}
		for _, msg := range msgs {
			if msg.Offset < startOffset {
				continue
			}
			if normalizeTimestampForKafkaMillis(msg.Timestamp) >= targetTimestamp {
				return msg.Offset, true, nil
			}
		}
	}
	return 0, false, nil
}

func (s *Server) findTimestampInActiveSegment(activeSeg *log.ActiveSegment, startOffset uint64, targetTimestamp int64, hw uint64, hwOK bool) (uint64, bool, error) {
	offsetIdx := activeSeg.OffsetIndex()

	tsIdx := activeSeg.TimestampIndex()
	startIdx := 0
	if tsOffset, ok := log.LookupTimestampOffset(tsIdx, targetTimestamp); ok {
		seekOffset := tsOffset
		if uint64(seekOffset) < startOffset {
			seekOffset = int64(startOffset)
		}
		startIdx = sort.Search(len(offsetIdx), func(i int) bool {
			return offsetIdx[i].LastOffset >= seekOffset
		})
	}

	for i := startIdx; i < len(offsetIdx); i++ {
		entry := offsetIdx[i]
		if uint64(entry.LastOffset) < startOffset {
			continue
		}
		if hwOK && uint64(entry.BaseOffset) >= hw {
			break
		}
		if entry.BatchSize <= 0 || entry.Position < 0 {
			continue
		}
		buf := make([]byte, entry.BatchSize)
		n, err := activeSeg.ReadAt(buf, entry.Position)
		if err != nil && n < int(entry.BatchSize) {
			return 0, false, err
		}
		msgs, err := log.DecodeRecordBatch(buf[:n])
		if err != nil {
			return 0, false, err
		}
		for _, msg := range msgs {
			if msg.Offset < startOffset {
				continue
			}
			if hwOK && msg.Offset >= hw {
				return 0, false, nil
			}
			if normalizeTimestampForKafkaMillis(msg.Timestamp) >= targetTimestamp {
				return msg.Offset, true, nil
			}
		}
	}
	return 0, false, nil
}

func normalizeTimestampForKafkaMillis(ts int64) int64 {
	if ts == 0 {
		return 0
	}
	if ts > 1_000_000_000_000_000 {
		return ts / int64(time.Millisecond)
	}
	return ts
}
