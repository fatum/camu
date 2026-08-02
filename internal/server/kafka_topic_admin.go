package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maksim/camu/internal/meta"
	"github.com/maksim/camu/internal/storage"
)

func (s *Server) handleKafkaCreateTopics(ctx context.Context, req *kmsg.CreateTopicsRequest) (*kmsg.CreateTopicsResponse, error) {
	resp := kmsg.NewPtrCreateTopicsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	seen := make(map[string]bool, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewCreateTopicsResponseTopic()
		topicResp.Topic = topic.Topic
		topicResp.NumPartitions = topic.NumPartitions
		topicResp.ReplicationFactor = topic.ReplicationFactor

		if !s.amLeader() {
			topicResp.ErrorCode = kafkaErrorNotController
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}

		reqBody, errCode, errMsg := s.kafkaCreateTopicRequest(topic)
		if errCode == 0 {
			if err := s.validateParquetExportTopicConfig(reqBody.ExportEnabled, reqBody.UncleanLeaderElection); err != nil {
				errCode = kafkaErrorInvalidConfig
				errMsg = err.Error()
			}
		}
		if errCode == 0 && seen[topic.Topic] {
			errCode = kafkaErrorInvalidRequest
			errMsg = "duplicate topic in request"
		}
		seen[topic.Topic] = true

		if errCode == 0 && !req.ValidateOnly {
			tc, err := s.createTopic(ctx, reqBody)
			switch {
			case err == nil:
				topicResp.NumPartitions = int32(tc.Partitions)
				topicResp.ReplicationFactor = int16(tc.ReplicationFactor)
			case strings.Contains(err.Error(), "already exists"):
				errCode = kafkaErrorTopicAlreadyExists
				errMsg = err.Error()
			case strings.Contains(err.Error(), "deletion in progress"):
				errCode = kafkaErrorTopicAlreadyExists
				errMsg = err.Error()
			case strings.Contains(err.Error(), "replication_factor"):
				errCode = kafkaErrorInvalidReplication
				errMsg = err.Error()
			case strings.Contains(err.Error(), "min_insync_replicas"):
				errCode = kafkaErrorInvalidConfig
				errMsg = err.Error()
			default:
				return nil, err
			}
		}

		if errCode != 0 {
			topicResp.ErrorCode = errCode
			topicResp.ErrorMessage = kmsg.StringPtr(errMsg)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaDeleteTopics(ctx context.Context, req *kmsg.DeleteTopicsRequest) (*kmsg.DeleteTopicsResponse, error) {
	resp := kmsg.NewPtrDeleteTopicsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	var topicNames []string
	if req.GetVersion() <= 5 {
		topicNames = append(topicNames, req.TopicNames...)
	} else {
		for _, topic := range req.Topics {
			if topic.Topic != nil {
				topicNames = append(topicNames, *topic.Topic)
				continue
			}
			topicResp := kmsg.NewDeleteTopicsResponseTopic()
			topicResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "topic IDs are not supported"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
		}
	}

	for _, topicName := range topicNames {
		topicResp := kmsg.NewDeleteTopicsResponseTopic()
		topicResp.Topic = kmsg.StringPtr(topicName)
		if !s.amLeader() {
			topicResp.ErrorCode = kafkaErrorNotController
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		if err := s.deleteTopic(ctx, topicName); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				topicResp.ErrorCode = kafkaErrorUnknownTopicPartition
			} else {
				return nil, err
			}
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaCreatePartitions(ctx context.Context, req *kmsg.CreatePartitionsRequest) (*kmsg.CreatePartitionsResponse, error) {
	resp := kmsg.NewPtrCreatePartitionsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	seen := make(map[string]bool, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewCreatePartitionsResponseTopic()
		topicResp.Topic = topic.Topic

		if !s.amLeader() {
			topicResp.ErrorCode = kafkaErrorNotController
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		if seen[topic.Topic] {
			topicResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "duplicate topic in request"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		seen[topic.Topic] = true

		if len(topic.Assignment) > 0 {
			topicResp.ErrorCode = kafkaErrorInvalidReplicaAssign
			msg := "manual partition assignment is not supported"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}

		tc, err := s.topicStore.Get(ctx, topic.Topic)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				topicResp.ErrorCode = kafkaErrorUnknownTopicPartition
				resp.Topics = append(resp.Topics, topicResp)
				continue
			}
			return nil, err
		}
		if int(topic.Count) <= tc.Partitions {
			topicResp.ErrorCode = kafkaErrorInvalidPartitions
			msg := "partition count must increase"
			topicResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}
		if req.ValidateOnly {
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}

		tc.Partitions = int(topic.Count)
		if err := s.topicStore.Update(ctx, tc); err != nil {
			return nil, err
		}
		s.publishAssignmentsForTopics(ctx, []meta.TopicConfig{tc})
		s.applyAssignmentsForTopic(ctx, tc.Name, tc.Partitions)
		if err := s.partitionManager.AddTopicPartitions(ctx, tc, s.getOwnedEpochs(tc.Name)); err != nil {
			return nil, err
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaDescribeConfigs(ctx context.Context, req *kmsg.DescribeConfigsRequest) (*kmsg.DescribeConfigsResponse, error) {
	resp := kmsg.NewPtrDescribeConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, resource := range req.Resources {
		resourceResp := kmsg.NewDescribeConfigsResponseResource()
		resourceResp.ResourceType = resource.ResourceType
		resourceResp.ResourceName = resource.ResourceName

		switch resource.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			tc, err := s.topicStore.Get(ctx, resource.ResourceName)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					resourceResp.ErrorCode = kafkaErrorUnknownTopicPartition
					resp.Resources = append(resp.Resources, resourceResp)
					continue
				}
				return nil, err
			}
			configs := kafkaTopicDescribeConfigs(tc)
			resourceResp.Configs = filterDescribeConfigs(configs, resource.ConfigNames)
		default:
			resourceResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "only topic configs are supported"
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
		}

		resp.Resources = append(resp.Resources, resourceResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaAlterConfigs(ctx context.Context, req *kmsg.AlterConfigsRequest) (*kmsg.AlterConfigsResponse, error) {
	resp := kmsg.NewPtrAlterConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, resource := range req.Resources {
		resourceResp := kmsg.NewAlterConfigsResponseResource()
		resourceResp.ResourceType = resource.ResourceType
		resourceResp.ResourceName = resource.ResourceName

		if !s.amLeader() {
			resourceResp.ErrorCode = kafkaErrorNotController
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if resource.ResourceType != kmsg.ConfigResourceTypeTopic {
			resourceResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "only topic config mutation is supported"
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}

		tc, err := s.topicStore.Get(ctx, resource.ResourceName)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				resourceResp.ErrorCode = kafkaErrorUnknownTopicPartition
				resp.Resources = append(resp.Resources, resourceResp)
				continue
			}
			return nil, err
		}
		next, err := applyKafkaTopicConfigs(tc, kafkaAlterConfigsToMap(resource.Configs), true)
		if err != nil {
			resourceResp.ErrorCode = kafkaErrorInvalidConfig
			msg := err.Error()
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if err := s.validateParquetExportTopicConfig(next.ExportEnabled, next.UncleanLeaderElection); err != nil {
			resourceResp.ErrorCode = kafkaErrorInvalidConfig
			msg := err.Error()
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if !req.ValidateOnly {
			if err := s.topicStore.Update(ctx, next); err != nil {
				return nil, err
			}
		}
		resp.Resources = append(resp.Resources, resourceResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaIncrementalAlterConfigs(ctx context.Context, req *kmsg.IncrementalAlterConfigsRequest) (*kmsg.IncrementalAlterConfigsResponse, error) {
	resp := kmsg.NewPtrIncrementalAlterConfigsResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, resource := range req.Resources {
		resourceResp := kmsg.NewIncrementalAlterConfigsResponseResource()
		resourceResp.ResourceType = resource.ResourceType
		resourceResp.ResourceName = resource.ResourceName

		if !s.amLeader() {
			resourceResp.ErrorCode = kafkaErrorNotController
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if resource.ResourceType != kmsg.ConfigResourceTypeTopic {
			resourceResp.ErrorCode = kafkaErrorInvalidRequest
			msg := "only topic config mutation is supported"
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}

		tc, err := s.topicStore.Get(ctx, resource.ResourceName)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				resourceResp.ErrorCode = kafkaErrorUnknownTopicPartition
				resp.Resources = append(resp.Resources, resourceResp)
				continue
			}
			return nil, err
		}
		next, err := applyKafkaTopicIncrementalConfigs(tc, resource.Configs)
		if err != nil {
			resourceResp.ErrorCode = kafkaErrorInvalidConfig
			msg := err.Error()
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if err := s.validateParquetExportTopicConfig(next.ExportEnabled, next.UncleanLeaderElection); err != nil {
			resourceResp.ErrorCode = kafkaErrorInvalidConfig
			msg := err.Error()
			resourceResp.ErrorMessage = kmsg.StringPtr(msg)
			resp.Resources = append(resp.Resources, resourceResp)
			continue
		}
		if !req.ValidateOnly {
			if err := s.topicStore.Update(ctx, next); err != nil {
				return nil, err
			}
		}
		resp.Resources = append(resp.Resources, resourceResp)
	}
	return resp, nil
}

func (s *Server) handleKafkaDescribeCluster(ctx context.Context, req *kmsg.DescribeClusterRequest) (*kmsg.DescribeClusterResponse, error) {
	resp := kmsg.NewPtrDescribeClusterResponse()
	setKafkaResponseVersion(resp, req.GetVersion())
	resp.ClusterID = s.kafkaClusterID()
	resp.EndpointType = req.EndpointType

	instanceInfos, err := s.registry.ActiveInstanceInfos(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(instanceInfos, func(i, j int) bool {
		return instanceInfos[i].InstanceID < instanceInfos[j].InstanceID
	})
	for _, info := range instanceInfos {
		if info.KafkaAddress == "" {
			continue
		}
		host, port := splitKafkaBrokerAddr(info.KafkaAddress)
		resp.Brokers = append(resp.Brokers, kmsg.DescribeClusterResponseBroker{
			NodeID: kafkaBrokerID(info.InstanceID),
			Host:   host,
			Port:   port,
		})
	}

	lease, err := s.leaderElection.GetLeader(ctx)
	if err == nil && lease.InstanceID != "" {
		resp.ControllerID = kafkaBrokerID(lease.InstanceID)
	} else {
		resp.ControllerID = kafkaBrokerID(s.instanceID)
	}
	return resp, nil
}

func (s *Server) kafkaCreateTopicRequest(topic kmsg.CreateTopicsRequestTopic) (createTopicRequest, int16, string) {
	reqBody := createTopicRequest{
		Name:              topic.Topic,
		Partitions:        int(topic.NumPartitions),
		ReplicationFactor: int(topic.ReplicationFactor),
	}
	if reqBody.Name == "" {
		return reqBody, kafkaErrorInvalidRequest, "topic name is required"
	}
	if len(topic.ReplicaAssignment) > 0 {
		return reqBody, kafkaErrorInvalidReplicaAssign, "manual replica assignment is not supported"
	}
	if reqBody.Partitions == -1 {
		reqBody.Partitions = 1
	}
	if reqBody.ReplicationFactor == -1 {
		reqBody.ReplicationFactor = 1
	}
	if reqBody.Partitions < 1 {
		return reqBody, kafkaErrorInvalidPartitions, "partitions must be at least 1"
	}
	if reqBody.ReplicationFactor < 1 {
		return reqBody, kafkaErrorInvalidReplication, "replication factor must be at least 1"
	}

	for _, cfg := range topic.Configs {
		if cfg.Value == nil {
			continue
		}
		switch cfg.Name {
		case "camu.storage.mode":
			if *cfg.Value != "classic" && *cfg.Value != "diskless" {
				return reqBody, kafkaErrorInvalidConfig, "invalid camu.storage.mode"
			}
			reqBody.StorageMode = *cfg.Value
		case "camu.export.enabled":
			v, err := strconv.ParseBool(*cfg.Value)
			if err != nil {
				return reqBody, kafkaErrorInvalidConfig, "invalid camu.export.enabled"
			}
			reqBody.ExportEnabled = v
		case "camu.schema":
			var schema meta.TopicSchema
			if err := json.Unmarshal([]byte(*cfg.Value), &schema); err != nil {
				return reqBody, kafkaErrorInvalidConfig, "invalid camu.schema JSON"
			}
			if err := schema.Validate(); err != nil {
				return reqBody, kafkaErrorInvalidConfig, err.Error()
			}
			reqBody.Schema = &schema
		case "retention.bytes":
			return reqBody, kafkaErrorInvalidConfig, "retention.bytes is unsupported; use retention.ms (time-based retention only)"
		case "retention.ms":
			ms, err := strconv.ParseInt(*cfg.Value, 10, 64)
			if err != nil || ms < 0 {
				return reqBody, kafkaErrorInvalidConfig, "invalid retention.ms"
			}
			reqBody.Retention = fmt.Sprintf("%dms", ms)
		case "min.insync.replicas":
			minISR, err := strconv.Atoi(*cfg.Value)
			if err != nil || minISR < 1 {
				return reqBody, kafkaErrorInvalidConfig, "invalid min.insync.replicas"
			}
			reqBody.MinInsyncReplicas = minISR
		case "unclean.leader.election.enable":
			v, err := strconv.ParseBool(*cfg.Value)
			if err != nil {
				return reqBody, kafkaErrorInvalidConfig, "invalid unclean.leader.election.enable"
			}
			reqBody.UncleanLeaderElection = v
		case "cleanup.policy":
			if *cfg.Value != "delete" {
				return reqBody, kafkaErrorInvalidConfig, "only cleanup.policy=delete is supported"
			}
		default:
			return reqBody, kafkaErrorInvalidConfig, fmt.Sprintf("unsupported topic config %q", cfg.Name)
		}
	}

	return reqBody, 0, ""
}

func kafkaTopicDescribeConfigs(tc meta.TopicConfig) []kmsg.DescribeConfigsResponseResourceConfig {
	retentionMs := fmt.Sprintf("%d", tc.Retention/time.Millisecond)
	minISR := fmt.Sprintf("%d", tc.MinInsyncReplicas)
	cleanupPolicy := "delete"
	unclean := strconv.FormatBool(tc.UncleanLeaderElection)
	storageMode := tc.StorageMode
	if storageMode == "" {
		storageMode = "classic"
	}

	return []kmsg.DescribeConfigsResponseResourceConfig{
		{Name: "camu.schema", Value: topicSchemaConfigValue(tc.Schema), IsDefault: tc.Schema == nil},
		{Name: "camu.storage.mode", Value: kmsg.StringPtr(storageMode), IsDefault: storageMode == "classic"},
		{Name: "camu.export.enabled", Value: kmsg.StringPtr(strconv.FormatBool(tc.ExportEnabled)), IsDefault: !tc.ExportEnabled},
		{Name: "cleanup.policy", Value: kmsg.StringPtr(cleanupPolicy), IsDefault: true},
		{Name: "min.insync.replicas", Value: kmsg.StringPtr(minISR), IsDefault: false},
		{Name: "retention.ms", Value: kmsg.StringPtr(retentionMs), IsDefault: false},
		{Name: "unclean.leader.election.enable", Value: kmsg.StringPtr(unclean), IsDefault: !tc.UncleanLeaderElection},
	}
}

func topicSchemaConfigValue(schema *meta.TopicSchema) *string {
	if schema == nil {
		return nil
	}
	b, _ := json.Marshal(schema)
	v := string(b)
	return &v
}

func kafkaAlterConfigsToMap(configs []kmsg.AlterConfigsRequestResourceConfig) map[string]*string {
	out := make(map[string]*string, len(configs))
	for _, cfg := range configs {
		out[cfg.Name] = cfg.Value
	}
	return out
}

func applyKafkaTopicIncrementalConfigs(tc meta.TopicConfig, configs []kmsg.IncrementalAlterConfigsRequestResourceConfig) (meta.TopicConfig, error) {
	values := map[string]*string{}
	for _, cfg := range configs {
		switch cfg.Op {
		case kmsg.IncrementalAlterConfigOpSet:
			values[cfg.Name] = cfg.Value
		case kmsg.IncrementalAlterConfigOpDelete:
			values[cfg.Name] = nil
		default:
			return tc, fmt.Errorf("unsupported incremental alter op for %q", cfg.Name)
		}
	}
	return applyKafkaTopicConfigs(tc, values, false)
}

func applyKafkaTopicConfigs(tc meta.TopicConfig, values map[string]*string, resetMissing bool) (meta.TopicConfig, error) {
	next := tc

	if resetMissing {
		next.Retention = 7 * 24 * time.Hour
		next.MinInsyncReplicas = 1
		next.UncleanLeaderElection = false
		next.ExportEnabled = false
	}

	for name, value := range values {
		switch name {
		case "camu.schema":
			return tc, fmt.Errorf("camu.schema is immutable")
		case "camu.storage.mode":
			if value == nil {
				return tc, fmt.Errorf("camu.storage.mode is immutable")
			}
			if *value != "classic" && *value != "diskless" {
				return tc, fmt.Errorf("invalid camu.storage.mode")
			}
			if *value != next.StorageMode && !(*value == "classic" && next.StorageMode == "") {
				return tc, fmt.Errorf("camu.storage.mode is immutable")
			}
		case "camu.export.enabled":
			if value == nil {
				next.ExportEnabled = false
				continue
			}
			v, err := strconv.ParseBool(*value)
			if err != nil {
				return tc, fmt.Errorf("invalid camu.export.enabled")
			}
			if v && next.StorageMode == meta.StorageModeDiskless {
				return tc, fmt.Errorf("export_enabled is unsupported for diskless topics")
			}
			next.ExportEnabled = v
		case "cleanup.policy":
			if value == nil {
				continue
			}
			if *value != "delete" {
				return tc, fmt.Errorf("only cleanup.policy=delete is supported")
			}
		case "retention.bytes":
			return tc, fmt.Errorf("retention.bytes is unsupported; use retention.ms (time-based retention only)")
		case "retention.ms":
			if value == nil {
				next.Retention = 7 * 24 * time.Hour
				continue
			}
			ms, err := strconv.ParseInt(*value, 10, 64)
			if err != nil || ms < 0 {
				return tc, fmt.Errorf("invalid retention.ms")
			}
			next.Retention = time.Duration(ms) * time.Millisecond
		case "min.insync.replicas":
			if value == nil {
				next.MinInsyncReplicas = 1
				continue
			}
			v, err := strconv.Atoi(*value)
			if err != nil || v < 1 || v > next.ReplicationFactor {
				return tc, fmt.Errorf("invalid min.insync.replicas")
			}
			next.MinInsyncReplicas = v
		case "unclean.leader.election.enable":
			if value == nil {
				next.UncleanLeaderElection = false
				continue
			}
			v, err := strconv.ParseBool(*value)
			if err != nil {
				return tc, fmt.Errorf("invalid unclean.leader.election.enable")
			}
			next.UncleanLeaderElection = v
		default:
			return tc, fmt.Errorf("unsupported topic config %q", name)
		}
	}

	return next, nil
}

func filterDescribeConfigs(configs []kmsg.DescribeConfigsResponseResourceConfig, names []string) []kmsg.DescribeConfigsResponseResourceConfig {
	if len(names) == 0 {
		return configs
	}
	allowed := make(map[string]bool, len(names))
	for _, name := range names {
		allowed[name] = true
	}
	filtered := make([]kmsg.DescribeConfigsResponseResourceConfig, 0, len(configs))
	for _, cfg := range configs {
		if allowed[cfg.Name] {
			filtered = append(filtered, cfg)
		}
	}
	return filtered
}

func (s *Server) kafkaClusterID() string {
	if s.cfg.Storage.Bucket != "" {
		return "camu-" + s.cfg.Storage.Bucket
	}
	return "camu"
}
