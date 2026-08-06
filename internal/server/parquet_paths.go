package server

// Thin re-exports of path helpers from internal/iceberg so existing
// server-side code keeps its historical identifiers.

import "github.com/maksim/camu/internal/iceberg"

const parquetDataPrefix = iceberg.DataPrefix

var (
	parquetExportObjectKey      = iceberg.ExportObjectKey
	parquetManifestKey          = iceberg.ManifestKey
	parquetQueryCatalogTopicKey = iceberg.QueryCatalogTopicKey
)
