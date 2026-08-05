package diskless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type dynamoUploadState struct {
	NextOffset int64                            `json:"next_offset"`
	Producers  map[string][]dynamoProducerBatch `json:"producers,omitempty"`
}

type dynamoProducerBatch struct {
	FirstSequence int64
	BaseOffset    int64
	Count         int
}

func allocateRetryBackoff(attempt int) time.Duration {
	backoff := time.Duration(50*(1<<uint(attempt))) * time.Millisecond
	if backoff > time.Second {
		return time.Second
	}
	return backoff
}

// DynamoMetaStoreConfig configures a DynamoDB-backed MetaStore.
type DynamoMetaStoreConfig struct {
	TablePrefix string // e.g. "camu" -> tables "camu_offsets", "camu_segments"
	Region      string
	Endpoint    string // optional, for local DynamoDB
}

// DynamoMetaStore implements MetaStore on top of DynamoDB.
type DynamoMetaStore struct {
	client        *dynamodb.Client
	offsetsTable  string
	segmentsTable string
}

// NewDynamoMetaStore creates a DynamoDB-backed MetaStore.
func NewDynamoMetaStore(ctx context.Context, cfg DynamoMetaStoreConfig) (*DynamoMetaStore, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	var ddbOpts []func(*dynamodb.Options)
	if cfg.Endpoint != "" {
		ddbOpts = append(ddbOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	prefix := cfg.TablePrefix
	if prefix == "" {
		prefix = "camu"
	}

	return &DynamoMetaStore{
		client:        dynamodb.NewFromConfig(awsCfg, ddbOpts...),
		offsetsTable:  prefix + "_offsets",
		segmentsTable: prefix + "_segments",
	}, nil
}

// CommitUploadedBatches uses a two-item transaction per batch: the partition
// ordering state and its readable ref change together. The conditional state
// write makes concurrent writers reload and revalidate their producer sequence.
func (d *DynamoMetaStore) CommitUploadedBatches(ctx context.Context, batches []UploadedBatch) ([]OffsetResult, error) {
	if len(batches) > 1 {
		return nil, fmt.Errorf("commit uploaded batches accepts one batch per invocation")
	}
	results := make([]OffsetResult, len(batches))
	for i, b := range batches {
		if b.BatchID == "" || b.Count <= 0 {
			return nil, fmt.Errorf("uploaded batch %s has invalid count %d", b.FileKey, b.Count)
		}
		pk := partitionKey(b.Topic, b.Partition)
		for attempt := 0; ; attempt++ {
			out, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &d.offsetsTable, Key: map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: pk}}, ProjectionExpression: aws.String("upload_state")})
			if err != nil {
				return nil, fmt.Errorf("read upload state %s: %w", pk, err)
			}
			state := dynamoUploadState{Producers: map[string][]dynamoProducerBatch{}}
			var old string
			if out.Item != nil {
				if a, ok := out.Item["upload_state"].(*ddbtypes.AttributeValueMemberS); ok {
					old = a.Value
					if err := json.Unmarshal([]byte(old), &state); err != nil {
						return nil, fmt.Errorf("parse upload state %s: %w", pk, err)
					}
				}
			}
			pid := strconv.FormatInt(b.ProducerID, 10)
			duplicate := false
			if b.ProducerID != 0 {
				h := state.Producers[pid]
				for _, r := range h {
					if r.FirstSequence == b.Sequence {
						if r.Count != b.Count {
							return nil, fmt.Errorf("producer %d retried sequence %d with different count", b.ProducerID, b.Sequence)
						}
						results[i] = OffsetResult{BaseOffset: r.BaseOffset, Duplicate: true}
						duplicate = true
						break
					}
				}
				if len(h) > 0 {
					last := h[len(h)-1]
					if _, err := checkProducerSequence(b.ProducerID, b.Sequence, b.Count, last.FirstSequence, last.Count); err != nil {
						return nil, err
					}
				} else if err := checkInitialProducerSequence(b.ProducerID, b.Sequence); err != nil {
					return nil, err
				}
			}
			if duplicate {
				break
			}
			base := state.NextOffset
			end := base + int64(b.Count)
			state.NextOffset = end
			if b.ProducerID != 0 {
				h := state.Producers[pid]
				h = append(h, dynamoProducerBatch{FirstSequence: b.Sequence, BaseOffset: base, Count: b.Count})
				if len(h) > uploadedProducerHistory {
					h = h[len(h)-uploadedProducerHistory:]
				}
				state.Producers[pid] = h
			}
			encoded, err := json.Marshal(state)
			if err != nil {
				return nil, err
			}
			condition := "attribute_not_exists(upload_state)"
			values := map[string]ddbtypes.AttributeValue{":new": &ddbtypes.AttributeValueMemberS{Value: string(encoded)}, ":next": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(end, 10)}}
			if old != "" {
				condition = "upload_state = :old"
				values[":old"] = &ddbtypes.AttributeValueMemberS{Value: old}
			}
			_, err = d.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []ddbtypes.TransactWriteItem{
				{Update: &ddbtypes.Update{TableName: &d.offsetsTable, Key: map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: pk}}, UpdateExpression: aws.String("SET upload_state = :new, next_offset = :next, committed_offset = :next"), ConditionExpression: aws.String(condition), ExpressionAttributeValues: values}},
				{Put: &ddbtypes.Put{TableName: &d.segmentsTable, ConditionExpression: aws.String("attribute_not_exists(pk)"), Item: map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: pk}, "sk": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(base, 10)}, "file_key": &ddbtypes.AttributeValueMemberS{Value: b.FileKey}, "byte_offset": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(b.ByteOffset, 10)}, "byte_length": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(b.ByteLength, 10)}, "end_offset": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(end, 10)}, "created_at": &ddbtypes.AttributeValueMemberS{Value: b.CreatedAt.Format(time.RFC3339)}}}},
			}})
			if err == nil {
				results[i] = OffsetResult{BaseOffset: base}
				break
			}
			if attempt >= 7 {
				return nil, fmt.Errorf("commit uploaded batch %s: %w", b.FileKey, err)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(allocateRetryBackoff(attempt)):
			}
			continue
		}
	}
	return results, nil
}

// ReplaceSegmentRefs atomically removes the refs identified by remove and
// inserts add into the segment table using a single DynamoDB transaction, so
// readers never observe a gap or duplicate for the covered range. The committed
// watermark is not modified. A transaction is limited to 100 operations, so a
// merge run must stay within that bound.
func (d *DynamoMetaStore) ReplaceSegmentRefs(ctx context.Context, topic string, partition int, remove []RefKey, add []SegmentRef) error {
	if len(remove)+len(add) > 100 {
		return fmt.Errorf("replace segment refs: %d operations exceed the 100-item transaction limit", len(remove)+len(add))
	}
	pk := partitionKey(topic, partition)
	now := time.Now()

	// DynamoDB forbids two operations on the same item in one transaction. A
	// merged ref often starts at the same base offset as the first ref it
	// replaces, so a remove and an add can collide on sk. When that happens the
	// Put alone is sufficient (it overwrites the removed ref); emit only one
	// operation per sk.
	addedByBase := make(map[int64]bool, len(add))
	for _, ref := range add {
		addedByBase[ref.BaseOffset] = true
	}
	writes := make([]ddbtypes.TransactWriteItem, 0, len(remove)+len(add))
	for _, rk := range remove {
		if addedByBase[rk.BaseOffset] {
			continue // replaced by the Put below
		}
		writes = append(writes, ddbtypes.TransactWriteItem{
			Delete: &ddbtypes.Delete{
				TableName: &d.segmentsTable,
				Key: map[string]ddbtypes.AttributeValue{
					"pk": &ddbtypes.AttributeValueMemberS{Value: pk},
					"sk": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(rk.BaseOffset, 10)},
				},
			},
		})
	}
	for _, ref := range add {
		writes = append(writes, ddbtypes.TransactWriteItem{
			Put: &ddbtypes.Put{
				TableName: &d.segmentsTable,
				Item: map[string]ddbtypes.AttributeValue{
					"pk":          &ddbtypes.AttributeValueMemberS{Value: pk},
					"sk":          &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(ref.BaseOffset, 10)},
					"file_key":    &ddbtypes.AttributeValueMemberS{Value: ref.FileKey},
					"byte_offset": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(ref.ByteOffset, 10)},
					"byte_length": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(ref.ByteLength, 10)},
					"end_offset":  &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(ref.EndOffset, 10)},
					"created_at":  &ddbtypes.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
				},
			},
		})
	}
	if len(writes) == 0 {
		return nil
	}
	if _, err := d.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: writes,
	}); err != nil {
		return fmt.Errorf("replace segment refs %s: %w", pk, err)
	}
	return nil
}

// readCommittedOffset returns the partition's committed offset, or 0 if none.
func (d *DynamoMetaStore) readCommittedOffset(ctx context.Context, pk string) (int64, error) {
	out, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &d.offsetsTable,
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: pk},
		},
		ProjectionExpression: aws.String("committed_offset"),
	})
	if err != nil {
		return 0, fmt.Errorf("get committed offset for %s: %w", pk, err)
	}
	if out.Item == nil {
		return 0, nil
	}
	if v, ok := offsetFromItem(out.Item, "committed_offset"); ok {
		return v, nil
	}
	return 0, nil
}

// offsetFromItem extracts an integer attribute value from a DynamoDB item.
func offsetFromItem(item map[string]ddbtypes.AttributeValue, name string) (int64, bool) {
	v, ok := item[name].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v.Value, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// QuerySegments returns segment references covering [fromOffset, ...) up to maxBytes.
func (d *DynamoMetaStore) QuerySegments(ctx context.Context, topic string, partition int, fromOffset int64, maxBytes int) ([]SegmentRef, error) {
	pk := partitionKey(topic, partition)
	forward := true

	input := &dynamodb.QueryInput{
		TableName:              &d.segmentsTable,
		KeyConditionExpression: aws.String("pk = :pk AND sk >= :from"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":   &ddbtypes.AttributeValueMemberS{Value: pk},
			":from": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(fromOffset, 10)},
		},
		ScanIndexForward: &forward,
	}

	var refs []SegmentRef
	var totalBytes int64

	paginator := dynamodb.NewQueryPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("query segments for %s: %w", pk, err)
		}
		for _, item := range page.Items {
			ref, err := itemToSegmentRef(item)
			if err != nil {
				return nil, err
			}
			refs = append(refs, ref)
			totalBytes += ref.ByteLength
			if totalBytes >= int64(maxBytes) {
				return refs, nil
			}
		}
	}
	return refs, nil
}

func itemToSegmentRef(item map[string]ddbtypes.AttributeValue) (SegmentRef, error) {
	var ref SegmentRef
	if v, ok := item["file_key"].(*ddbtypes.AttributeValueMemberS); ok {
		ref.FileKey = v.Value
	}
	if v, ok := item["sk"].(*ddbtypes.AttributeValueMemberN); ok {
		ref.BaseOffset, _ = strconv.ParseInt(v.Value, 10, 64)
	}
	if v, ok := item["end_offset"].(*ddbtypes.AttributeValueMemberN); ok {
		ref.EndOffset, _ = strconv.ParseInt(v.Value, 10, 64)
	}
	if v, ok := item["byte_offset"].(*ddbtypes.AttributeValueMemberN); ok {
		ref.ByteOffset, _ = strconv.ParseInt(v.Value, 10, 64)
	}
	if v, ok := item["byte_length"].(*ddbtypes.AttributeValueMemberN); ok {
		ref.ByteLength, _ = strconv.ParseInt(v.Value, 10, 64)
	}
	if v, ok := item["created_at"].(*ddbtypes.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			ref.CreatedAt = t
		}
	}
	return ref, nil
}

// GetPartitionHead returns the next offset for a partition, or 0 if none allocated.
func (d *DynamoMetaStore) GetPartitionHead(ctx context.Context, topic string, partition int) (int64, error) {
	pk := partitionKey(topic, partition)
	out, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &d.offsetsTable,
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: pk},
		},
		ProjectionExpression: aws.String("next_offset"),
	})
	if err != nil {
		return 0, fmt.Errorf("get partition head for %s: %w", pk, err)
	}
	if out.Item == nil {
		return 0, nil
	}
	if v, ok := out.Item["next_offset"].(*ddbtypes.AttributeValueMemberN); ok {
		return strconv.ParseInt(v.Value, 10, 64)
	}
	return 0, nil
}

// GetCommittedHead returns the highest offset durably materialized for a
// partition, or 0 if nothing has been registered yet.
func (d *DynamoMetaStore) GetCommittedHead(ctx context.Context, topic string, partition int) (int64, error) {
	return d.readCommittedOffset(ctx, partitionKey(topic, partition))
}

// GetPartitionStart returns the first readable offset for a partition, or the
// current head if all prior segments have been expired.
func (d *DynamoMetaStore) GetPartitionStart(ctx context.Context, topic string, partition int) (int64, error) {
	pk := partitionKey(topic, partition)
	forward := true
	out, err := d.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &d.segmentsTable,
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: pk},
		},
		ProjectionExpression: aws.String("sk"),
		ScanIndexForward:     &forward,
		Limit:                aws.Int32(1),
	})
	if err != nil {
		return 0, fmt.Errorf("get partition start for %s: %w", pk, err)
	}
	if len(out.Items) == 0 {
		return d.GetCommittedHead(ctx, topic, partition)
	}
	if v, ok := out.Items[0]["sk"].(*ddbtypes.AttributeValueMemberN); ok {
		return strconv.ParseInt(v.Value, 10, 64)
	}
	return d.GetPartitionHead(ctx, topic, partition)
}

// PlanExpiredFileDeletes returns file keys whose refs for the given
// topic-partition are expired and whose remaining refs, if any, are also
// expired.
func (d *DynamoMetaStore) PlanExpiredFileDeletes(ctx context.Context, topic string, partition int, cutoff time.Time) ([]string, error) {
	pk := partitionKey(topic, partition)
	forward := true
	input := &dynamodb.QueryInput{
		TableName:              &d.segmentsTable,
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: pk},
		},
		ScanIndexForward: &forward,
	}

	candidates := make(map[string]struct{})
	paginator := dynamodb.NewQueryPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("query expired segments for %s: %w", pk, err)
		}
		for _, item := range page.Items {
			createdAtValue, ok := item["created_at"].(*ddbtypes.AttributeValueMemberS)
			if !ok {
				continue
			}
			createdAt, err := time.Parse(time.RFC3339, createdAtValue.Value)
			if err != nil || createdAt.After(cutoff) {
				continue
			}
			fileKeyValue, ok := item["file_key"].(*ddbtypes.AttributeValueMemberS)
			if ok {
				candidates[fileKeyValue.Value] = struct{}{}
			}
		}
	}

	var deletable []string
	for fileKey := range candidates {
		hasFreshRef, err := d.hasFileFreshReference(ctx, fileKey, cutoff)
		if err != nil {
			return nil, err
		}
		if !hasFreshRef {
			deletable = append(deletable, fileKey)
		}
	}
	return deletable, nil
}

// DeleteFileRefs removes all segment refs pointing at fileKey.
func (d *DynamoMetaStore) DeleteFileRefs(ctx context.Context, fileKey string) error {
	input := &dynamodb.ScanInput{
		TableName:        &d.segmentsTable,
		FilterExpression: aws.String("file_key = :file_key"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":file_key": &ddbtypes.AttributeValueMemberS{Value: fileKey},
		},
		ProjectionExpression: aws.String("pk, sk"),
	}

	paginator := dynamodb.NewScanPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("scan file refs for delete %s: %w", fileKey, err)
		}
		var requests []ddbtypes.WriteRequest
		for _, item := range page.Items {
			requests = append(requests, ddbtypes.WriteRequest{
				DeleteRequest: &ddbtypes.DeleteRequest{
					Key: map[string]ddbtypes.AttributeValue{
						"pk": item["pk"],
						"sk": item["sk"],
					},
				},
			})
		}
		for i := 0; i < len(requests); i += 25 {
			end := i + 25
			if end > len(requests) {
				end = len(requests)
			}
			out, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]ddbtypes.WriteRequest{
					d.segmentsTable: requests[i:end],
				},
			})
			if err != nil {
				return fmt.Errorf("delete file refs %s: %w", fileKey, err)
			}
			if len(out.UnprocessedItems) > 0 {
				if _, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: out.UnprocessedItems,
				}); err != nil {
					return fmt.Errorf("retry delete file refs %s: %w", fileKey, err)
				}
			}
		}
	}
	return nil
}

// ArchiveCommitted is a no-op: the DynamoDB metastore stores segment refs as
// individual rows, so no bounded head object needs archiving.
func (d *DynamoMetaStore) ArchiveCommitted(_ context.Context, _ string, _ int, _ int64, _ time.Time) (int, error) {
	return 0, nil
}

// DeleteTopic removes all MetaStore state for a topic.
func (d *DynamoMetaStore) DeleteTopic(ctx context.Context, topic string) error {
	// Delete segments: scan for all partition keys matching this topic.
	if err := d.deleteTopicFromTable(ctx, d.segmentsTable, topic, true); err != nil {
		return fmt.Errorf("delete topic segments: %w", err)
	}
	// Delete offsets: scan for all partition keys matching this topic.
	if err := d.deleteTopicFromTable(ctx, d.offsetsTable, topic, false); err != nil {
		return fmt.Errorf("delete topic offsets: %w", err)
	}
	return nil
}

func (d *DynamoMetaStore) hasFileReference(ctx context.Context, fileKey string) (bool, error) {
	input := &dynamodb.ScanInput{
		TableName:        &d.segmentsTable,
		FilterExpression: aws.String("file_key = :file_key"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":file_key": &ddbtypes.AttributeValueMemberS{Value: fileKey},
		},
		ProjectionExpression: aws.String("file_key"),
	}
	paginator := dynamodb.NewScanPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("scan file references for %s: %w", fileKey, err)
		}
		if len(page.Items) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// PlanUnreferencedFileDeletes returns the subset of fileKeys with no segment
// refs anywhere, so their data objects can be deleted after compaction.
func (d *DynamoMetaStore) PlanUnreferencedFileDeletes(ctx context.Context, fileKeys []string) ([]string, error) {
	var deletable []string
	for _, fileKey := range fileKeys {
		referenced, err := d.hasFileReference(ctx, fileKey)
		if err != nil {
			return nil, err
		}
		if !referenced {
			deletable = append(deletable, fileKey)
		}
	}
	return deletable, nil
}

// ListFileRefs returns every segment reference across all partitions that
// points at fileKey.
func (d *DynamoMetaStore) ListFileRefs(ctx context.Context, fileKey string) ([]FileRef, error) {
	input := &dynamodb.ScanInput{
		TableName:        &d.segmentsTable,
		FilterExpression: aws.String("file_key = :file_key"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":file_key": &ddbtypes.AttributeValueMemberS{Value: fileKey},
		},
	}
	var refs []FileRef
	paginator := dynamodb.NewScanPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list file refs for %s: %w", fileKey, err)
		}
		for _, item := range page.Items {
			ref, err := itemToSegmentRef(item)
			if err != nil {
				return nil, err
			}
			pk, ok := item["pk"].(*ddbtypes.AttributeValueMemberS)
			if !ok {
				continue
			}
			topic, partition, err := parsePartitionKey(pk.Value)
			if err != nil {
				continue
			}
			refs = append(refs, FileRef{Topic: topic, Partition: partition, Ref: ref})
		}
	}
	return refs, nil
}

func (d *DynamoMetaStore) hasFileFreshReference(ctx context.Context, fileKey string, cutoff time.Time) (bool, error) {
	input := &dynamodb.ScanInput{
		TableName:        &d.segmentsTable,
		FilterExpression: aws.String("file_key = :file_key"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":file_key": &ddbtypes.AttributeValueMemberS{Value: fileKey},
		},
		ProjectionExpression: aws.String("created_at"),
	}
	paginator := dynamodb.NewScanPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("scan fresh file refs for %s: %w", fileKey, err)
		}
		for _, item := range page.Items {
			createdAtValue, ok := item["created_at"].(*ddbtypes.AttributeValueMemberS)
			if !ok {
				continue
			}
			createdAt, err := time.Parse(time.RFC3339, createdAtValue.Value)
			if err == nil && createdAt.After(cutoff) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (d *DynamoMetaStore) deleteTopicFromTable(ctx context.Context, table, topic string, hasSortKey bool) error {
	prefix := topic + "#"
	input := &dynamodb.ScanInput{
		TableName:        &table,
		FilterExpression: aws.String("begins_with(pk, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
		},
		ProjectionExpression: aws.String("pk, sk"),
	}
	if !hasSortKey {
		input.ProjectionExpression = aws.String("pk")
	}

	paginator := dynamodb.NewScanPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		var requests []ddbtypes.WriteRequest
		for _, item := range page.Items {
			key := map[string]ddbtypes.AttributeValue{
				"pk": item["pk"],
			}
			if hasSortKey {
				key["sk"] = item["sk"]
			}
			requests = append(requests, ddbtypes.WriteRequest{
				DeleteRequest: &ddbtypes.DeleteRequest{Key: key},
			})
		}
		// BatchWriteItem in chunks of 25.
		for i := 0; i < len(requests); i += 25 {
			end := i + 25
			if end > len(requests) {
				end = len(requests)
			}
			_, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]ddbtypes.WriteRequest{
					table: requests[i:end],
				},
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Close is a no-op for DynamoDB.
func (d *DynamoMetaStore) Close() error {
	return nil
}

// EnsureTables creates the offsets and segments tables if they don't exist.
func (d *DynamoMetaStore) EnsureTables(ctx context.Context) error {
	if err := d.ensureTable(ctx, d.offsetsTable, false); err != nil {
		return err
	}
	return d.ensureTable(ctx, d.segmentsTable, true)
}

func (d *DynamoMetaStore) ensureTable(ctx context.Context, name string, hasSortKey bool) error {
	keySchema := []ddbtypes.KeySchemaElement{
		{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
	}
	attrDefs := []ddbtypes.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
	}
	if hasSortKey {
		keySchema = append(keySchema, ddbtypes.KeySchemaElement{
			AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange,
		})
		attrDefs = append(attrDefs, ddbtypes.AttributeDefinition{
			AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeN,
		})
	}

	_, err := d.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            &name,
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		// Ignore "already exists" error.
		var resourceInUse *ddbtypes.ResourceInUseException
		if errors.As(err, &resourceInUse) {
			return nil
		}
		return fmt.Errorf("create table %s: %w", name, err)
	}
	return nil
}
