package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict: etag mismatch")
)

// S3Config holds configuration for the S3 client.
type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
}

// PutOpts holds optional parameters for Put operations.
type PutOpts struct {
	ContentType string
}

// memObject is a stored object in the in-memory backend.
type memObject struct {
	data []byte
	etag string
}

// s3Backend is the interface for storage backends.
type s3Backend interface {
	put(ctx context.Context, key string, data []byte, opts PutOpts) error
	get(ctx context.Context, key string) ([]byte, error)
	getWithETag(ctx context.Context, key string) ([]byte, string, error)
	delete(ctx context.Context, key string) error
	list(ctx context.Context, prefix string) ([]string, error)
	conditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error)
}

// S3Client is the public S3 client, backed by either in-memory or real AWS S3.
type S3Client struct {
	cfg     S3Config
	backend s3Backend
}

// NewS3Client constructs an S3Client. If Endpoint is "memory://", uses in-memory backend.
func NewS3Client(cfg S3Config) (*S3Client, error) {
	var backend s3Backend
	if cfg.Endpoint == "memory://" {
		backend = newMemBackend()
	} else {
		b, err := newS3Backend(cfg)
		if err != nil {
			return nil, fmt.Errorf("NewS3Client: %w", err)
		}
		backend = b
	}
	return &S3Client{cfg: cfg, backend: backend}, nil
}

// Put stores data at key.
func (c *S3Client) Put(ctx context.Context, key string, data []byte, opts PutOpts) error {
	return c.backend.put(ctx, key, data, opts)
}

// Get retrieves data at key. Returns ErrNotFound if missing.
func (c *S3Client) Get(ctx context.Context, key string) ([]byte, error) {
	return c.backend.get(ctx, key)
}

// GetWithETag retrieves data and the current ETag for key. Returns ErrNotFound if missing.
func (c *S3Client) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	return c.backend.getWithETag(ctx, key)
}

// Delete removes key. Does not error if key does not exist.
func (c *S3Client) Delete(ctx context.Context, key string) error {
	return c.backend.delete(ctx, key)
}

// List returns keys with the given prefix.
func (c *S3Client) List(ctx context.Context, prefix string) ([]string, error) {
	return c.backend.list(ctx, prefix)
}

// ConditionalPut writes data to key only if the current ETag matches etag.
// An empty etag means "write unconditionally on first creation".
// Returns the new ETag on success, or ErrConflict on mismatch.
func (c *S3Client) ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error) {
	return c.backend.conditionalPut(ctx, key, data, etag)
}

// ---- In-memory backend ----

type memBackend struct {
	mu      sync.RWMutex
	objects map[string]memObject
}

func newMemBackend() *memBackend {
	return &memBackend{objects: make(map[string]memObject)}
}

func (m *memBackend) put(_ context.Context, key string, data []byte, _ PutOpts) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = memObject{data: cp, etag: uuid.NewString()}
	return nil
}

func (m *memBackend) get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := make([]byte, len(obj.data))
	copy(cp, obj.data)
	return cp, nil
}

func (m *memBackend) getWithETag(_ context.Context, key string) ([]byte, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	cp := make([]byte, len(obj.data))
	copy(cp, obj.data)
	return cp, obj.etag, nil
}

func (m *memBackend) delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memBackend) list(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *memBackend) conditionalPut(_ context.Context, key string, data []byte, etag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.objects[key]

	if etag == "" {
		// Create-if-not-exists: only succeed if the key doesn't exist yet.
		if exists {
			return "", ErrConflict
		}
	} else {
		if !exists {
			return "", ErrConflict
		}
		if existing.etag != etag {
			return "", ErrConflict
		}
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	newEtag := uuid.NewString()
	m.objects[key] = memObject{data: cp, etag: newEtag}
	return newEtag, nil
}

// ---- Real AWS S3 backend ----

type awsS3Backend struct {
	client *s3.Client
	bucket string
}

func newS3Backend(cfg S3Config) (*awsS3Backend, error) {
	var optFns []func(*awsconfig.LoadOptions) error

	if cfg.Region != "" {
		optFns = append(optFns, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		optFns = append(optFns, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		ep := cfg.Endpoint
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	return &awsS3Backend{client: client, bucket: cfg.Bucket}, nil
}

func (b *awsS3Backend) put(ctx context.Context, key string, data []byte, opts PutOpts) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	_, err := b.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 Put %q: %w", key, err)
	}
	return nil
}

func (b *awsS3Backend) get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := b.getWithETag(ctx, key)
	return data, err
}

func (b *awsS3Backend) getWithETag(ctx context.Context, key string) ([]byte, string, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("s3 Get %q: %w", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("s3 Get %q read body: %w", key, err)
	}
	etag := ""
	if out.ETag != nil {
		etag = strings.Trim(*out.ETag, `"`)
	}
	return data, etag, nil
}

func (b *awsS3Backend) delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 Delete %q: %w", key, err)
	}
	return nil
}

func (b *awsS3Backend) list(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 List %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}
	return keys, nil
}

func (b *awsS3Backend) conditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	if etag != "" {
		quoted := `"` + etag + `"`
		input.IfMatch = aws.String(quoted)
	} else {
		// Create-if-not-exists: fail if object already exists.
		input.IfNoneMatch = aws.String("*")
	}
	out, err := b.client.PutObject(ctx, input)
	if err != nil {
		if isS3Conflict(err) {
			return "", ErrConflict
		}
		return "", fmt.Errorf("s3 ConditionalPut %q: %w", key, err)
	}
	newEtag := ""
	if out.ETag != nil {
		newEtag = strings.Trim(*out.ETag, `"`)
	}
	return newEtag, nil
}

// isS3NotFound checks if an AWS error is a 404 / NoSuchKey.
func isS3NotFound(err error) bool {
	var noKey *s3types.NoSuchKey
	if errors.As(err, &noKey) {
		return true
	}
	var notFound *s3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	// Fallback for MinIO and other S3-compatible stores.
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "StatusCode: 404")
}

// isS3Conflict checks if an AWS error is a 412 Precondition Failed.
func isS3Conflict(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "PreconditionFailed") || strings.Contains(msg, "StatusCode: 412")
}
