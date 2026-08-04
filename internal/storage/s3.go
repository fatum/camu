package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/maksim/camu/internal/metrics"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict: etag mismatch")
)

// ConflictError is returned when an S3 conditional write fails its
// precondition. It wraps ErrConflict so errors.Is(err, ErrConflict) remains
// true, and preserves the underlying cause for typed detection across
// providers.
type ConflictError struct {
	Key   string
	Cause error
}

func (e *ConflictError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("conflict on %q: %v", e.Key, e.Cause)
	}
	return fmt.Sprintf("conflict on %q", e.Key)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

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
	getRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	getWithETag(ctx context.Context, key string) ([]byte, string, error)
	delete(ctx context.Context, key string) error
	list(ctx context.Context, prefix string) ([]string, error)
	conditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error)
	conditionalPutFile(ctx context.Context, key string, file io.ReadSeeker, size int64, etag string) (string, error)
	equalsFile(ctx context.Context, key string, file io.ReadSeeker, size int64) (bool, error)
}

// S3Client is the public S3 client, backed by either in-memory or real AWS S3.
type S3Client struct {
	cfg     S3Config
	backend s3Backend
	// metrics is atomic because a shared client (e.g. the camutest harness)
	// can be re-pointed at a new server's registry while the previous server's
	// goroutines are still draining.
	metrics atomic.Pointer[metrics.Registry]
	// fault is an optional test-only injector called before every operation.
	fault atomic.Pointer[faultInjector]
}

type faultInjector func(op string) error

// SetFaultInjector installs a test-only fault injector invoked before every
// operation. op is the operation name ("put", "get", "get_range",
// "get_etag", "delete", "list", "conditional_put"). Returning a non-nil error
// fails the operation. Passing nil removes the injector. It is safe for
// concurrent use and has no effect on production paths when unset.
func (c *S3Client) SetFaultInjector(fn func(op string) error) {
	if fn == nil {
		c.fault.Store(nil)
		return
	}
	f := faultInjector(fn)
	c.fault.Store(&f)
}

func (c *S3Client) checkFault(op string) error {
	if p := c.fault.Load(); p != nil {
		return (*p)(op)
	}
	return nil
}

func (c *S3Client) SetMetrics(registry *metrics.Registry) { c.metrics.Store(registry) }

func (c *S3Client) observe(op string, started time.Time, bytes int64, err error) {
	m := c.metrics.Load()
	if m == nil {
		return
	}
	labels := map[string]string{"operation": op, "result": "ok"}
	if err != nil {
		labels["result"] = "error"
	}
	m.Inc("camu_s3_operations_total", "S3 operations", labels)
	if bytes > 0 {
		m.Add("camu_s3_bytes_total", "S3 payload bytes", map[string]string{"operation": op, "direction": direction(op)}, float64(bytes))
	}
	m.Observe("camu_s3_operation_duration", "S3 operation duration", map[string]string{"operation": op}, time.Since(started))
}

func direction(op string) string {
	if op == "get" || op == "get_range" || op == "get_etag" {
		return "read"
	}
	return "write"
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
	if err := c.checkFault("put"); err != nil {
		return err
	}
	started := time.Now()
	err := c.backend.put(ctx, key, data, opts)
	c.observe("put", started, int64(len(data)), err)
	return err
}

// Get retrieves data at key. Returns ErrNotFound if missing.
func (c *S3Client) Get(ctx context.Context, key string) ([]byte, error) {
	if err := c.checkFault("get"); err != nil {
		return nil, err
	}
	started := time.Now()
	data, err := c.backend.get(ctx, key)
	c.observe("get", started, int64(len(data)), err)
	return data, err
}

// GetRange retrieves a byte range from key. Returns ErrNotFound if missing.
func (c *S3Client) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := c.checkFault("get_range"); err != nil {
		return nil, err
	}
	started := time.Now()
	data, err := c.backend.getRange(ctx, key, offset, length)
	c.observe("get_range", started, int64(len(data)), err)
	return data, err
}

// GetWithETag retrieves data and the current ETag for key. Returns ErrNotFound if missing.
func (c *S3Client) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	if err := c.checkFault("get_etag"); err != nil {
		return nil, "", err
	}
	started := time.Now()
	data, etag, err := c.backend.getWithETag(ctx, key)
	c.observe("get_etag", started, int64(len(data)), err)
	return data, etag, err
}

// Delete removes key. Does not error if key does not exist.
func (c *S3Client) Delete(ctx context.Context, key string) error {
	if err := c.checkFault("delete"); err != nil {
		return err
	}
	started := time.Now()
	err := c.backend.delete(ctx, key)
	c.observe("delete", started, 0, err)
	return err
}

// List returns keys with the given prefix.
func (c *S3Client) List(ctx context.Context, prefix string) ([]string, error) {
	if err := c.checkFault("list"); err != nil {
		return nil, err
	}
	started := time.Now()
	keys, err := c.backend.list(ctx, prefix)
	c.observe("list", started, 0, err)
	return keys, err
}

// ConditionalPut writes data to key only if the current ETag matches etag.
// An empty etag means "write unconditionally on first creation".
// Returns the new ETag on success, or ErrConflict on mismatch.
func (c *S3Client) ConditionalPut(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if err := c.checkFault("conditional_put"); err != nil {
		return "", err
	}
	started := time.Now()
	newETag, err := c.backend.conditionalPut(ctx, key, data, etag)
	c.observe("conditional_put", started, int64(len(data)), err)
	return newETag, err
}

// ConditionalPutFile conditionally uploads exactly size bytes from file. The
// reader is rewound before use so callers can safely reuse an encoded temp
// file after writing it. Unlike ConditionalPut, this path never needs to make
// a complete in-memory copy of the upload.
func (c *S3Client) ConditionalPutFile(ctx context.Context, key string, file io.ReadSeeker, size int64, etag string) (string, error) {
	if err := c.checkFault("conditional_put"); err != nil {
		return "", err
	}
	if size < 0 {
		return "", fmt.Errorf("conditional put file %q: negative size", key)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewind conditional put file %q: %w", key, err)
	}
	started := time.Now()
	newETag, err := c.backend.conditionalPutFile(ctx, key, file, size, etag)
	c.observe("conditional_put", started, size, err)
	return newETag, err
}

// ObjectEqualsFile compares an object with a seekable file in bounded chunks.
// It is used to make immutable create retries idempotent after a conditional
// create reports a conflict.
func (c *S3Client) ObjectEqualsFile(ctx context.Context, key string, file io.ReadSeeker, size int64) (bool, error) {
	if err := c.checkFault("get"); err != nil {
		return false, err
	}
	if size < 0 {
		return false, fmt.Errorf("compare object %q: negative size", key)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("rewind comparison file %q: %w", key, err)
	}
	started := time.Now()
	equal, err := c.backend.equalsFile(ctx, key, file, size)
	c.observe("get", started, size, err)
	return equal, err
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

func (m *memBackend) getRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	end := offset + length
	if end > int64(len(obj.data)) {
		end = int64(len(obj.data))
	}
	if offset >= int64(len(obj.data)) {
		return nil, nil
	}
	cp := make([]byte, end-offset)
	copy(cp, obj.data[offset:end])
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

func (m *memBackend) conditionalPutFile(ctx context.Context, key string, file io.ReadSeeker, size int64, etag string) (string, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return "", fmt.Errorf("read conditional put file: %w", err)
	}
	if _, err := file.Read(make([]byte, 1)); err != io.EOF {
		if err == nil {
			return "", fmt.Errorf("conditional put file exceeds declared size")
		}
		return "", fmt.Errorf("read conditional put file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return m.conditionalPut(ctx, key, data, etag)
}

func (m *memBackend) equalsFile(ctx context.Context, key string, file io.ReadSeeker, size int64) (bool, error) {
	m.mu.RLock()
	obj, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return false, ErrNotFound
	}
	if int64(len(obj.data)) != size {
		return false, nil
	}
	buffer := make([]byte, 64*1024)
	for offset := 0; offset < len(obj.data); {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		count := len(buffer)
		if remaining := len(obj.data) - offset; remaining < count {
			count = remaining
		}
		if _, err := io.ReadFull(file, buffer[:count]); err != nil {
			return false, fmt.Errorf("read comparison file: %w", err)
		}
		if !bytes.Equal(obj.data[offset:offset+count], buffer[:count]) {
			return false, nil
		}
		offset += count
	}
	return true, nil
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

	s3Opts := []func(*s3.Options){func(o *s3.Options) {
		// DigitalOcean Spaces does not return the optional x-amz-checksum-* response
		// headers. The SDK still validates responses when those headers exist; this
		// only suppresses one warning for every otherwise-successful GET.
		o.DisableLogOutputChecksumValidationSkipped = true
	}}
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

func (b *awsS3Backend) getRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("s3 GetRange %q: %w", key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 GetRange %q read body: %w", key, err)
	}
	return data, nil
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
		input.IfMatch = aws.String(etag)
	} else {
		// Create-if-not-exists: fail if object already exists.
		input.IfNoneMatch = aws.String("*")
	}
	out, err := b.client.PutObject(ctx, input)
	if err != nil {
		if cause := conflictCause(err); cause != nil {
			return "", &ConflictError{Key: key, Cause: cause}
		}
		return "", fmt.Errorf("s3 ConditionalPut %q: %w", key, err)
	}
	newEtag := ""
	if out.ETag != nil {
		newEtag = strings.Trim(*out.ETag, `"`)
	}
	return newEtag, nil
}

func (b *awsS3Backend) conditionalPutFile(ctx context.Context, key string, file io.ReadSeeker, size int64, etag string) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewind conditional put file %q: %w", key, err)
	}
	input := &s3.PutObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key), Body: file, ContentLength: aws.Int64(size)}
	if etag != "" {
		input.IfMatch = aws.String(etag)
	} else {
		input.IfNoneMatch = aws.String("*")
	}
	out, err := b.client.PutObject(ctx, input)
	if err != nil {
		if cause := conflictCause(err); cause != nil {
			return "", &ConflictError{Key: key, Cause: cause}
		}
		return "", fmt.Errorf("s3 ConditionalPutFile %q: %w", key, err)
	}
	newETag := ""
	if out.ETag != nil {
		newETag = strings.Trim(*out.ETag, `"`)
	}
	return newETag, nil
}

func (b *awsS3Backend) equalsFile(ctx context.Context, key string, file io.ReadSeeker, size int64) (bool, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("rewind comparison file %q: %w", key, err)
	}
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		if isS3NotFound(err) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("s3 Get %q for comparison: %w", key, err)
	}
	defer out.Body.Close()
	if out.ContentLength == nil || *out.ContentLength != size {
		return false, nil
	}
	remote := make([]byte, 64*1024)
	local := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		count, readErr := out.Body.Read(remote)
		if count > 0 {
			if _, err := io.ReadFull(file, local[:count]); err != nil {
				return false, fmt.Errorf("read comparison file: %w", err)
			}
			if !bytes.Equal(remote[:count], local[:count]) {
				return false, nil
			}
		}
		if readErr == io.EOF {
			return true, nil
		}
		if readErr != nil {
			return false, fmt.Errorf("read object %q for comparison: %w", key, readErr)
		}
	}
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

// conflictCause reports whether err is a conditional-write precondition failure
// (HTTP 412) and returns a non-nil cause when it is. AWS SDK v2 surfaces these
// as typed API errors; MinIO and other S3-compatible stores may only provide a
// status line.
func conflictCause(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "IfMatchFailed", "IfNoneMatchFailed":
			return err
		}
	}
	msg := err.Error()
	if strings.Contains(msg, "PreconditionFailed") || strings.Contains(msg, "StatusCode: 412") {
		return err
	}
	return nil
}
