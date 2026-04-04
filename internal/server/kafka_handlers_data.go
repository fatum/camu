package server

import (
	"context"

	"github.com/maksim/camu/internal/log"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (ks *KafkaServer) handleProduce(req *kmsg.ProduceRequest) (kmsg.Response, error) {
	resp := kmsg.NewPtrProduceResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, topic := range req.Topics {
		topicResp := kmsg.NewProduceResponseTopic()
		topicResp.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			partResp := kmsg.NewProduceResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.BaseOffset = int64(partition.Partition) * 1000

			errorCode := ks.partitionError(topic.Topic, int(partition.Partition))
			if errorCode == 0 {
				if ks.cfg.AppendRawBatchFunc != nil {
					// Zero-copy path: pass raw RecordBatch bytes directly.
					rawBatches, err := extractRawRecordBatches(partition.Records)
					if err == nil && len(rawBatches) > 0 {
						var firstOffset int64 = -1
						for _, rb := range rawBatches {
							baseOff, appendErr := ks.cfg.AppendRawBatchFunc(context.Background(), topic.Topic, int(partition.Partition), rb)
							if appendErr != nil {
								errorCode = mapKafkaError(appendErr)
								break
							}
							if firstOffset < 0 {
								firstOffset = baseOff
							}
						}
						if firstOffset >= 0 {
							partResp.BaseOffset = firstOffset
						}
					} else if err != nil {
						errorCode = kafkaErrorUnknownServer
					}
				} else if ks.cfg.AppendBatchFunc != nil {
					batches, err := decodeKafkaProduceBatches(partition.Records)
					if err == nil {
						var firstOffset int64 = -1
						for _, batch := range batches {
							offsets, appendErr := ks.cfg.AppendBatchFunc(topic.Topic, int(partition.Partition), batch)
							if appendErr != nil {
								errorCode = mapKafkaError(appendErr)
								break
							}
							if firstOffset < 0 && len(offsets) > 0 {
								firstOffset = int64(offsets[0])
							}
						}
						if firstOffset >= 0 {
							partResp.BaseOffset = firstOffset
						}
					} else if ks.cfg.AppendFunc != nil {
						msgs := []log.Message{{Value: partition.Records}}
						offsets, appendErr := ks.cfg.AppendFunc(topic.Topic, int(partition.Partition), msgs)
						if appendErr != nil {
							errorCode = mapKafkaError(appendErr)
						} else if len(offsets) > 0 {
							partResp.BaseOffset = int64(offsets[0])
						}
					}
				} else if ks.cfg.AppendFunc != nil {
					msgs, err := decodeKafkaProduceMessages(partition.Records)
					if err != nil {
						msgs = []log.Message{{Value: partition.Records}}
					}
					offsets, err := ks.cfg.AppendFunc(topic.Topic, int(partition.Partition), msgs)
					if err != nil {
						errorCode = mapKafkaError(err)
					} else if len(offsets) > 0 {
						partResp.BaseOffset = int64(offsets[0])
					}
				}
			}
			partResp.ErrorCode = errorCode
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

func (ks *KafkaServer) handleFetch(req *kmsg.FetchRequest) (kmsg.Response, error) {
	resp := kmsg.NewPtrFetchResponse()
	setKafkaResponseVersion(resp, req.GetVersion())

	for _, topic := range req.Topics {
		topicResp := kmsg.NewFetchResponseTopic()
		topicResp.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			partResp := kmsg.NewFetchResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.LastStableOffset = partition.FetchOffset

			errorCode := ks.partitionError(topic.Topic, int(partition.Partition))
			if errorCode == 0 && ks.cfg.FetchRawBatchesFunc != nil {
				raw, hw, err := ks.cfg.FetchRawBatchesFunc(context.Background(), topic.Topic, int(partition.Partition), int64(partition.FetchOffset), int(partition.PartitionMaxBytes))
				if err != nil {
					errorCode = mapKafkaError(err)
				} else {
					partResp.RecordBatches = raw
					partResp.HighWatermark = hw
					partResp.LastStableOffset = hw
				}
			} else if errorCode == 0 && ks.cfg.FetchFunc != nil {
				fetch, err := ks.cfg.FetchFunc(topic.Topic, int(partition.Partition), uint64(partition.FetchOffset), partition.PartitionMaxBytes)
				if err != nil {
					errorCode = mapKafkaError(err)
				} else {
					partResp.RecordBatches = fetch.RecordBatches
					partResp.HighWatermark = fetch.HighWatermark
					partResp.LastStableOffset = fetch.LastStableOffset
				}
			}
			partResp.ErrorCode = errorCode
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}
