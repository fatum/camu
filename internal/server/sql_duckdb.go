package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

const (
	defaultSQLQueryLimit = 1000
	maxSQLQueryLimit     = 10000
)

var (
	sqlWritePattern = regexp.MustCompile(`(?i)\b(copy|attach|detach|install|load|create|alter|drop|insert|update|delete|export|call|pragma)\b`)
	// topicNamePattern constrains topic names to a conservative safe charset
	// before they are interpolated into DuckDB view DDL. This is defense in
	// depth — the identifier quoter escapes quotes, but restricting the
	// charset keeps the view name legible and eliminates a whole class of
	// escaping bugs.
	topicNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
)

var errSQLScanBudgetExceeded = errors.New("sql scan budget exceeded")

type sqlQueryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type sqlQueryResponse struct {
	Columns []sqlQueryColumn `json:"columns"`
	Rows    [][]any          `json:"rows"`
}

type localParquetFile struct {
	Path      string
	Temporary bool
	release   func()
}

func (f localParquetFile) Release() {
	if f.release != nil {
		f.release()
	}
}

type parquetFetchResult struct {
	CachePath   string
	Data        []byte
	Cached      bool
	reservation *parquetCacheReservation
}

// parquetCacheReservation keeps a newly acquired cache entry pinned until one
// caller takes ownership of the pin. A singleflight fetch can have several
// callers, so only one consumes the reservation; the remaining callers acquire
// their own pins before using the path.
type parquetCacheReservation struct {
	mu       sync.Mutex
	consumed bool
}

func (r *parquetCacheReservation) consume() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.consumed {
		return false
	}
	r.consumed = true
	return true
}

type parquetCacheEntry struct {
	path    string
	size    int64
	modTime time.Time
}

func (s *Server) executeSQLQuery(ctx context.Context, req sqlQueryRequest, scope sqlQueryScope) (sqlQueryResponse, error) {
	if err := validateSQLQueryStatement(req.SQL); err != nil {
		return sqlQueryResponse{}, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = defaultSQLQueryLimit
	}
	if limit > maxSQLQueryLimit {
		return sqlQueryResponse{}, fmt.Errorf("limit must be <= %d", maxSQLQueryLimit)
	}

	db, err := s.sqlDBHandle()
	if err != nil {
		return sqlQueryResponse{}, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return sqlQueryResponse{}, fmt.Errorf("open duckdb connection: %w", err)
	}
	defer conn.Close()

	cleanup, err := s.prepareSQLConnection(ctx, conn, scope)
	if err != nil {
		return sqlQueryResponse{}, err
	}
	defer cleanup()

	query := req.SQL
	if limit > 0 {
		query = fmt.Sprintf("SELECT * FROM (%s) AS camu_query LIMIT %d", req.SQL, limit)
	}
	rows, err := conn.QueryContext(ctx, query, req.Params...)
	if err != nil {
		return sqlQueryResponse{}, fmt.Errorf("execute sql query: %w", err)
	}
	defer rows.Close()

	resp, err := scanSQLRows(rows)
	if err != nil {
		return sqlQueryResponse{}, err
	}
	return resp, rows.Err()
}

func (s *Server) sqlDBHandle() (*sql.DB, error) {
	s.sqlDBMu.Lock()
	defer s.sqlDBMu.Unlock()
	if s.sqlDB != nil {
		return s.sqlDB, nil
	}
	tempDir := s.cfg.SQL.TempDirectoryValue()
	cacheDir := s.cfg.SQL.CacheDirectoryValue()
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, fmt.Errorf("create duckdb temp directory: %w", err)
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create sql cache directory: %w", err)
	}

	dsn := fmt.Sprintf("?temp_directory=%s", url.QueryEscape(filepath.ToSlash(tempDir)))
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open duckdb database: %w", err)
	}
	db.SetMaxOpenConns(s.cfg.SQL.MaxConcurrencyValue())
	db.SetMaxIdleConns(s.cfg.SQL.MaxConcurrencyValue())
	if s.cfg.SQL.MemoryLimit != "" {
		query := "SET memory_limit = '" + strings.ReplaceAll(s.cfg.SQL.MemoryLimit, "'", "''") + "'"
		if _, err := db.Exec(query); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("set duckdb memory_limit: %w", err)
		}
	}
	s.sqlDB = db
	return db, nil
}

func (s *Server) prepareSQLConnection(ctx context.Context, conn *sql.Conn, scope sqlQueryScope) (func(), error) {
	maxFiles := s.cfg.SQL.MaxScanFilesValue()
	totalFiles := 0
	var localFiles []localParquetFile
	cleanupFiles := func() {
		cleanupLocalParquetFiles(localFiles)
	}
	var createdViews []string
	dropViews := func(topics []string) {
		// Use a fresh context: cleanup must still run after a query
		// timeout or cancellation, otherwise the TEMP VIEW persists on
		// the pooled connection and a later query on a different topic
		// set sees a stale view. Background() is fine — DROP is fast.
		for _, t := range topics {
			_, _ = conn.ExecContext(context.Background(), "DROP VIEW IF EXISTS "+quoteDuckDBIdentifier(t))
		}
	}
	for _, topic := range scope.Topics {
		objectKeys := parquetObjectKeysForTopic(scope.Manifests[topic])
		if len(objectKeys) == 0 {
			dropViews(createdViews)
			cleanupFiles()
			return nil, fmt.Errorf("no parquet data available for topic %q", topic)
		}
		totalFiles += len(objectKeys)
		if maxFiles > 0 && totalFiles > maxFiles {
			dropViews(createdViews)
			cleanupFiles()
			return nil, fmt.Errorf("%w: scan would read %d files, max is %d", errSQLScanBudgetExceeded, totalFiles, maxFiles)
		}
		files, err := s.localParquetFilesForObjectKeys(ctx, objectKeys)
		if err != nil {
			dropViews(createdViews)
			cleanupFiles()
			return nil, err
		}
		paths := make([]string, 0, len(files))
		for _, file := range files {
			paths = append(paths, file.Path)
		}
		localFiles = append(localFiles, files...)
		if _, err := conn.ExecContext(ctx, buildTopicViewSQL(topic, paths)); err != nil {
			dropViews(createdViews)
			cleanupFiles()
			return nil, fmt.Errorf("create sql topic view %q: %w", topic, err)
		}
		createdViews = append(createdViews, topic)
	}
	return func() {
		dropViews(createdViews)
		cleanupFiles()
	}, nil
}

func parquetObjectKeysForTopic(manifests []ParquetManifest) []string {
	keys := make(map[string]struct{})
	for _, manifest := range manifests {
		for _, entry := range manifest.Entries {
			keys[entry.ObjectKey] = struct{}{}
		}
	}
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	slices.Sort(out)
	return out
}

func (s *Server) localParquetFilesForObjectKeys(ctx context.Context, objectKeys []string) ([]localParquetFile, error) {
	files := make([]localParquetFile, 0, len(objectKeys))
	for _, key := range objectKeys {
		file, err := s.ensureLocalParquetObject(ctx, key)
		if err != nil {
			cleanupLocalParquetFiles(files)
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

func cleanupLocalParquetFiles(files []localParquetFile) {
	for _, file := range files {
		if file.Temporary {
			_ = os.Remove(file.Path)
			continue
		}
		file.Release()
	}
}

// ensureLocalParquetObject coalesces concurrent fetches of the same
// cache key via s.parquetFetchGroup so two SQL queries racing on the
// same object do not both download it and clobber each other's temp
// file.
func (s *Server) ensureLocalParquetObject(ctx context.Context, objectKey string) (localParquetFile, error) {
	cachePath := s.parquetCachePath(objectKey)
	if file, ok := s.acquireCachedParquetFile(cachePath); ok {
		return file, nil
	}

	result, err, _ := s.parquetFetchGroup.Do(cachePath, func() (any, error) {
		if reservation, ok := s.reserveCachedParquetFile(cachePath); ok {
			return parquetFetchResult{CachePath: cachePath, Cached: true, reservation: reservation}, nil
		}

		data, err := s.s3Client.Get(ctx, objectKey)
		if err != nil {
			return parquetFetchResult{}, fmt.Errorf("fetch parquet object %q: %w", objectKey, err)
		}

		reservation, cached, err := s.cacheParquetObject(cachePath, data)
		if err != nil {
			return parquetFetchResult{}, err
		}
		if !cached {
			return parquetFetchResult{Data: data}, nil
		}
		return parquetFetchResult{CachePath: cachePath, Cached: true, reservation: reservation}, nil
	})
	if err != nil {
		return localParquetFile{}, err
	}
	fetch := result.(parquetFetchResult)
	if fetch.Cached {
		if fetch.reservation.consume() {
			return s.pinnedParquetFile(fetch.CachePath), nil
		}
		if file, ok := s.acquireCachedParquetFile(fetch.CachePath); ok {
			return file, nil
		}
		return localParquetFile{}, fmt.Errorf("cached parquet object %q was evicted before it could be acquired", objectKey)
	}
	tempPath, err := s.writeTemporaryParquetFile(fetch.Data)
	if err != nil {
		return localParquetFile{}, err
	}
	return localParquetFile{Path: tempPath, Temporary: true}, nil
}

func (s *Server) acquireCachedParquetFile(cachePath string) (localParquetFile, bool) {
	s.parquetCacheMu.Lock()
	defer s.parquetCacheMu.Unlock()
	if _, err := os.Stat(cachePath); err != nil {
		return localParquetFile{}, false
	}
	// Modification time is the LRU clock. Failure to update it does not make
	// the cache incorrect; it only makes a recently-read object evictable.
	_ = os.Chtimes(cachePath, time.Now(), time.Now())
	if _, err := s.reconcileParquetCacheLocked(filepath.Dir(cachePath), 0); err != nil {
		return localParquetFile{}, false
	}
	if _, err := os.Stat(cachePath); err != nil {
		return localParquetFile{}, false
	}
	s.pinParquetCachePathLocked(cachePath)
	return s.pinnedParquetFile(cachePath), true
}

func (s *Server) reserveCachedParquetFile(cachePath string) (*parquetCacheReservation, bool) {
	s.parquetCacheMu.Lock()
	defer s.parquetCacheMu.Unlock()
	if _, err := os.Stat(cachePath); err != nil {
		return nil, false
	}
	_ = os.Chtimes(cachePath, time.Now(), time.Now())
	if _, err := s.reconcileParquetCacheLocked(filepath.Dir(cachePath), 0); err != nil {
		return nil, false
	}
	if _, err := os.Stat(cachePath); err != nil {
		return nil, false
	}
	s.pinParquetCachePathLocked(cachePath)
	return &parquetCacheReservation{}, true
}

func (s *Server) pinnedParquetFile(cachePath string) localParquetFile {
	var once sync.Once
	return localParquetFile{Path: cachePath, release: func() {
		once.Do(func() { s.releaseParquetCachePath(cachePath) })
	}}
}

func (s *Server) pinParquetCachePathLocked(cachePath string) {
	if s.parquetCachePins == nil {
		s.parquetCachePins = make(map[string]int)
	}
	s.parquetCachePins[cachePath]++
}

func (s *Server) releaseParquetCachePath(cachePath string) {
	s.parquetCacheMu.Lock()
	defer s.parquetCacheMu.Unlock()
	if s.parquetCachePins[cachePath] <= 1 {
		delete(s.parquetCachePins, cachePath)
		return
	}
	s.parquetCachePins[cachePath]--
}

// cacheParquetObject installs data only when the aggregate cache stays within
// sql.cache_max_size. It evicts least-recently-used cache files first. When
// every candidate cannot be removed, callers fall back to a temporary file
// rather than allowing the cache to exceed its configured capacity.
func (s *Server) cacheParquetObject(cachePath string, data []byte) (*parquetCacheReservation, bool, error) {
	maxSize := s.cfg.SQL.CacheMaxSizeValue()
	if maxSize > 0 && int64(len(data)) > maxSize {
		return nil, false, nil
	}

	s.parquetCacheMu.Lock()
	defer s.parquetCacheMu.Unlock()
	if _, err := os.Stat(cachePath); err == nil {
		_ = os.Chtimes(cachePath, time.Now(), time.Now())
		if _, err := s.reconcileParquetCacheLocked(filepath.Dir(cachePath), 0); err != nil {
			return nil, false, err
		}
		if _, err := os.Stat(cachePath); err != nil {
			return nil, false, nil
		}
		s.pinParquetCachePathLocked(cachePath)
		return &parquetCacheReservation{}, true, nil
	}
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		return nil, false, fmt.Errorf("create parquet cache directory: %w", err)
	}
	if maxSize > 0 {
		fits, err := s.reconcileParquetCacheLocked(filepath.Dir(cachePath), int64(len(data)))
		if err != nil {
			return nil, false, err
		}
		if !fits {
			return nil, false, nil
		}
	}

	// Use a unique temp name so a crash between write and rename leaves no
	// half-written file at the canonical cache path.
	tmp, err := os.CreateTemp(filepath.Dir(cachePath), ".camu-cache-*.parquet.tmp")
	if err != nil {
		return nil, false, fmt.Errorf("create parquet cache temp: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return nil, false, fmt.Errorf("write parquet cache temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, false, fmt.Errorf("close parquet cache temp: %w", err)
	}
	if err := os.Rename(tmpPath, cachePath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, false, fmt.Errorf("install parquet cache file: %w", err)
	}
	s.pinParquetCachePathLocked(cachePath)
	return &parquetCacheReservation{}, true, nil
}

// reconcileParquetCacheLocked evicts unpinned LRU cache entries until the
// existing cache plus incomingSize is within sql.cache_max_size. Callers must
// hold parquetCacheMu. Only canonical parquet entries are considered, so query
// temporary files are never disturbed.
func (s *Server) reconcileParquetCacheLocked(cacheDir string, incomingSize int64) (bool, error) {
	maxSize := s.cfg.SQL.CacheMaxSizeValue()
	if maxSize <= 0 {
		return true, nil
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return incomingSize <= maxSize, nil
		}
		return false, fmt.Errorf("read parquet cache directory: %w", err)
	}
	var (
		total      int64
		candidates []parquetCacheEntry
	)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".parquet") {
			continue
		}
		path := filepath.Join(cacheDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return false, fmt.Errorf("stat parquet cache entry %q: %w", path, err)
		}
		total += info.Size()
		if s.parquetCachePins[path] == 0 {
			candidates = append(candidates, parquetCacheEntry{path: path, size: info.Size(), modTime: info.ModTime()})
		}
	}
	if total+incomingSize <= maxSize {
		return true, nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].modTime.Before(candidates[j].modTime) })
	for _, entry := range candidates {
		if err := os.Remove(entry.path); err != nil && !os.IsNotExist(err) {
			return false, fmt.Errorf("evict parquet cache entry %q: %w", entry.path, err)
		}
		total -= entry.size
		if total+incomingSize <= maxSize {
			return true, nil
		}
	}
	return false, nil
}

func (s *Server) parquetCachePath(objectKey string) string {
	sum := sha256.Sum256([]byte(objectKey))
	filename := hex.EncodeToString(sum[:]) + ".parquet"
	return filepath.Join(s.cfg.SQL.CacheDirectoryValue(), filename)
}

func (s *Server) writeTemporaryParquetFile(data []byte) (string, error) {
	tempDir := s.cfg.SQL.TempDirectoryValue()
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return "", fmt.Errorf("create sql temp directory: %w", err)
	}
	file, err := os.CreateTemp(tempDir, "camu-query-*.parquet")
	if err != nil {
		return "", fmt.Errorf("create temp parquet file: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("write temp parquet file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("close temp parquet file: %w", err)
	}
	return file.Name(), nil
}

func buildTopicViewSQL(topic string, paths []string) string {
	pathArgs := make([]string, 0, len(paths))
	for _, path := range paths {
		pathArgs = append(pathArgs, "'"+strings.ReplaceAll(filepath.ToSlash(path), "'", "''")+"'")
	}
	return fmt.Sprintf(
		`CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM read_parquet([%s], union_by_name = true)`,
		quoteDuckDBIdentifier(topic),
		strings.Join(pathArgs, ", "),
	)
}

func quoteDuckDBIdentifier(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func validateTopicName(topic string) error {
	if !topicNamePattern.MatchString(topic) {
		return fmt.Errorf("invalid topic name %q", topic)
	}
	return nil
}

func validateSQLQueryStatement(query string) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return fmt.Errorf("sql is required")
	}
	if strings.Contains(trimmed, ";") {
		return fmt.Errorf("multi-statement sql is not allowed")
	}
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return fmt.Errorf("only SELECT and WITH queries are allowed")
	}
	// Strip quoted identifiers ("...") and string literals ('...') before
	// scanning for mutating keywords. Otherwise a topic or column name
	// that contains a blocked word — e.g. "events-real-export" — would
	// produce a false positive on \bexport\b.
	stripped := stripSQLQuotedLiterals(trimmed)
	if sqlWritePattern.MatchString(stripped) {
		return fmt.Errorf("mutating sql statements are not allowed")
	}
	return nil
}

// stripSQLQuotedLiterals replaces every span between matching single or
// double quotes with a single space, preserving token boundaries so the
// remaining structural keywords can be matched with \b. SQL doubled-quote
// escapes ("" or ”) are treated as a literal quote inside the span.
func stripSQLQuotedLiterals(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '"' || c == '\'' {
			quote := c
			out.WriteByte(' ')
			i++
			for i < len(s) {
				if s[i] == quote {
					if i+1 < len(s) && s[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		out.WriteByte(c)
		i++
	}
	return out.String()
}

func scanSQLRows(rows *sql.Rows) (sqlQueryResponse, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return sqlQueryResponse{}, fmt.Errorf("describe sql result columns: %w", err)
	}
	resp := sqlQueryResponse{
		Columns: make([]sqlQueryColumn, len(columnTypes)),
	}
	for i, ct := range columnTypes {
		resp.Columns[i] = sqlQueryColumn{
			Name: ct.Name(),
			Type: ct.DatabaseTypeName(),
		}
	}
	for rows.Next() {
		values := make([]any, len(columnTypes))
		scanArgs := make([]any, len(columnTypes))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return sqlQueryResponse{}, fmt.Errorf("scan sql row: %w", err)
		}
		row := make([]any, len(values))
		copy(row, values)
		resp.Rows = append(resp.Rows, row)
	}
	return resp, nil
}
