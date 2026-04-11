package diskless

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

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

// AllocateOffsets assigns offset ranges using per-partition UpdateItem with
// UPDATED_OLD. Each partition counter is incremented independently and the
// pre-increment value (base offset) is returned atomically. Concurrent calls
// from different nodes are safe — DynamoDB ADD is commutative and UPDATED_OLD
// returns the value before this specific increment.
func (d *DynamoMetaStore) AllocateOffsets(ctx context.Context, allocs []OffsetAllocation) ([]OffsetResult, error) {
	if len(allocs) == 0 {
		return nil, nil
	}

	results := make([]OffsetResult, len(allocs))
	g, gctx := errgroup.WithContext(ctx)

	for i, alloc := range allocs {
		g.Go(func() error {
			pk := partitionKey(alloc.Topic, alloc.Partition)
			out, err := d.client.UpdateItem(gctx, &dynamodb.UpdateItemInput{
				TableName: &d.offsetsTable,
				Key: map[string]ddbtypes.AttributeValue{
					"pk": &ddbtypes.AttributeValueMemberS{Value: pk},
				},
				UpdateExpression: aws.String("ADD next_offset :count"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":count": &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(alloc.Count)},
				},
				ReturnValues: ddbtypes.ReturnValueUpdatedOld,
			})
			if err != nil {
				return fmt.Errorf("update offsets for %s: %w", pk, err)
			}

			var base int64
			if v, ok := out.Attributes["next_offset"]; ok {
				if nv, ok := v.(*ddbtypes.AttributeValueMemberN); ok {
					base, err = strconv.ParseInt(nv.Value, 10, 64)
					if err != nil {
						return fmt.Errorf("parse next_offset for %s: %w", pk, err)
					}
				}
			}
			results[i] = OffsetResult{BaseOffset: base}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// RegisterSegment writes segment batch references using BatchWriteItem.
func (d *DynamoMetaStore) RegisterSegment(ctx context.Context, seg SegmentRecord) error {
	var requests []ddbtypes.WriteRequest
	for _, batch := range seg.Batches {
		pk := partitionKey(batch.Topic, batch.Partition)
		requests = append(requests, ddbtypes.WriteRequest{
			PutRequest: &ddbtypes.PutRequest{
				Item: map[string]ddbtypes.AttributeValue{
					"pk":          &ddbtypes.AttributeValueMemberS{Value: pk},
					"sk":          &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(batch.BaseOffset, 10)},
					"file_key":    &ddbtypes.AttributeValueMemberS{Value: seg.FileKey},
					"byte_offset": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(batch.ByteOffset, 10)},
					"byte_length": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(batch.ByteLength, 10)},
					"end_offset":  &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(batch.EndOffset, 10)},
					"created_at":  &ddbtypes.AttributeValueMemberS{Value: seg.CreatedAt.Format(time.RFC3339)},
				},
			},
		})
	}

	// BatchWriteItem supports up to 25 items per call.
	for i := 0; i < len(requests); i += 25 {
		end := i + 25
		if end > len(requests) {
			end = len(requests)
		}
		chunk := requests[i:end]
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]ddbtypes.WriteRequest{
				d.segmentsTable: chunk,
			},
		}
		out, err := d.client.BatchWriteItem(ctx, input)
		if err != nil {
			return fmt.Errorf("batch write segments: %w", err)
		}
		// Retry unprocessed items (simple single retry).
		if len(out.UnprocessedItems) > 0 {
			_, err = d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: out.UnprocessedItems,
			})
			if err != nil {
				return fmt.Errorf("retry batch write segments: %w", err)
			}
		}
	}
	return nil
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
		return d.GetPartitionHead(ctx, topic, partition)
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
