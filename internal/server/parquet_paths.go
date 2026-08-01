package server

// Thin re-exports of path helpers from internal/parquet so existing
// server-side code keeps its historical identifiers.

import "github.com/maksim/camu/internal/parquet"

const parquetDataPrefix = parquet.DataPrefix

var (
	parquetExportObjectKey      = parquet.ExportObjectKey
	parquetManifestKey          = parquet.ManifestKey
	parquetQueryCatalogTopicKey = parquet.QueryCatalogTopicKey
)
