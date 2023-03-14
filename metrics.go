// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import "github.com/prometheus/client_golang/prometheus"

const (
	MetricCreateIndex                     = "create_index_total"
	MetricDeleteIndex                     = "delete_index_total"
	MetricCreateField                     = "create_field_total"
	MetricDeleteField                     = "delete_field_total"
	MetricDeleteAvailableShard            = "delete_available_shard_total"
	MetricRecalculateCache                = "recalculate_cache_total"
	MetricInvalidateCache                 = "invalidate_cache_total"
	MetricInvalidateCacheSkipped          = "invalidate_cache_skipped_total"
	MetricReadDirtyCache                  = "dirty_cache_total"
	MetricRankCacheLength                 = "rank_cache_length"
	MetricCacheThresholdReached           = "cache_threshold_reached_total"
	MetricRow                             = "query_row_total"
	MetricRowBSI                          = "query_row_bsi_total"
	MetricSetBit                          = "set_bit_total"
	MetricClearBit                        = "clear_bit_total"
	MetricImportingN                      = "importing_total"
	MetricImportedN                       = "imported_total"
	MetricClearingN                       = "clearing_total"
	MetricClearedN                        = "cleared_total"
	MetricSnapshotDurationSeconds         = "snapshot_duration_seconds"
	MetricBlockRepair                     = "block_repair_total"
	MetricSyncFieldDurationSeconds        = "sync_field_duration_seconds"
	MetricSyncIndexDurationSeconds        = "sync_index_duration_seconds"
	MetricHTTPRequest                     = "http_request_duration_seconds"
	MetricGRPCUnaryQueryDurationSeconds   = "grpc_request_pql_unary_query_duration_seconds"
	MetricGRPCUnaryFormatDurationSeconds  = "grpc_request_pql_unary_format_duration_seconds"
	MetricGRPCStreamQueryDurationSeconds  = "grpc_request_pql_stream_query_duration_seconds"
	MetricGRPCStreamFormatDurationSeconds = "grpc_request_pql_stream_format_duration_seconds"
	MetricMaxShard                        = "maximum_shard"
	MetricAntiEntropy                     = "antientropy_total"
	MetricAntiEntropyDurationSeconds      = "antientropy_duration_seconds"
	MetricGarbageCollection               = "garbage_collection_total"
	MetricGoroutines                      = "goroutines"
	MetricOpenFiles                       = "open_files"
	MetricHeapAlloc                       = "heap_alloc"
	MetricHeapInuse                       = "heap_inuse"
	MetricStackInuse                      = "stack_inuse"
	MetricMallocs                         = "mallocs"
	MetricFrees                           = "frees"
	MetricTransactionStart                = "transaction_start"
	MetricTransactionEnd                  = "transaction_end"
	MetricTransactionBlocked              = "transaction_blocked"
	MetricExclusiveTransactionRequest     = "transaction_exclusive_request"
	MetricExclusiveTransactionActive      = "transaction_exclusive_active"
	MetricExclusiveTransactionEnd         = "transaction_exclusive_end"
	MetricExclusiveTransactionBlocked     = "transaction_exclusive_blocked"
	MetricPqlQueries                      = "pql_queries_total"
	MetricSqlQueries                      = "sql_queries_total"
	MetricDeleteDataframe                 = "delete_dataframe"
)

const (
	// MetricBatchImportDurationSeconds records the full time of the
	// RecordBatch.Import call. This includes starting and finishing a
	// transaction, doing key translation, building fragments locally,
	// importing all data, and resetting internal structures.
	MetricBatchImportDurationSeconds = "batch_import_duration_seconds"

	// MetricBatchFlushDurationSeconds records the full time for
	// RecordBatch.Flush (if splitBatchMode is in use). This includes
	// starting and finishing a transaction, importing all data, and
	// resetting internal structures.
	MetricBatchFlushDurationSeconds = "batch_flush_duration_seconds"

	// MetricBatchShardImportBuildRequestsSeconds is the time it takes
	// after making fragments to build the shard-transactional request
	// objects (but not actually import them or do any network activity).
	MetricBatchShardImportBuildRequestsSeconds = "batch_shard_import_build_requests_seconds"

	// MetricBatchShardImportDurationSeconds is the time it takes to
	// import all data for all shards in the batch using the
	// shard-transactional endpoint. This does not include the time it
	// takes to build the requests locally.
	MetricBatchShardImportDurationSeconds = "batch_shard_import_duration_seconds"
)

// server related

var CounterJobTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "job_total",
		Help:      "TODO",
	},
)

var GaugeWorkerTotal = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      "worker_total",
		Help:      "TODO",
	},
)

var CounterPQLQueries = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricPqlQueries,
		Help:      "TODO",
	},
)

var CounterSQLQueries = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricSqlQueries,
		Help:      "TODO",
	},
)

var CounterGarbageCollection = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricGarbageCollection,
		Help:      "TODO",
	},
)

var GaugeGoroutines = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricGoroutines,
		Help:      "TODO",
	},
)

var GaugeOpenFiles = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricOpenFiles,
		Help:      "TODO",
	},
)

var GaugeHeapAlloc = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricHeapAlloc,
		Help:      "TODO",
	},
)

var GaugeHeapInUse = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricHeapInuse,
		Help:      "TODO",
	},
)

var GaugeStackInUse = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricStackInuse,
		Help:      "TODO",
	},
)

var GaugeMallocs = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricMallocs,
		Help:      "TODO",
	},
)

var GaugeFrees = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricFrees,
		Help:      "TODO",
	},
)

var SummaryHttpRequests = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricHTTPRequest,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
	[]string{
		"method",
		"path",
		"slow",
		"useragent",
		"where",
	},
)

var CounterCreateIndex = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricCreateIndex,
		Help:      "TODO",
	},
)

var CounterDeleteIndex = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricDeleteIndex,
		Help:      "TODO",
	},
)

var CounterDeleteDataframe = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricDeleteDataframe,
		Help:      "TODO",
	},
)

var CounterCreateField = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricCreateField,
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterDeleteField = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricDeleteField,
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterDeleteAvailableShard = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricDeleteAvailableShard,
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterExclusiveTransactionRequest = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricExclusiveTransactionRequest,
		Help:      "TODO",
	},
)

var CounterTransactionStart = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricTransactionStart,
		Help:      "TODO",
	},
)

var CounterExclusiveTransactionBlocked = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricExclusiveTransactionBlocked,
		Help:      "TODO",
	},
)

var CounterTransactionBlocked = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricTransactionBlocked,
		Help:      "TODO",
	},
)

var CounterExclusiveTransactionActive = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricExclusiveTransactionActive,
		Help:      "TODO",
	},
)

var CounterExclusiveTransactionEnd = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricExclusiveTransactionEnd,
		Help:      "TODO",
	},
)

var CounterTransactionEnd = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricTransactionEnd,
		Help:      "TODO",
	},
)

// TODO(pok) do these need index names?

var CounterRecalculateCache = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricRecalculateCache,
		Help:      "TODO",
	},
)

var CounterInvalidateCacheSkipped = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricInvalidateCacheSkipped,
		Help:      "TODO",
	},
)

var CounterInvalidateCache = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricInvalidateCache,
		Help:      "TODO",
	},
)

var GaugeRankCacheLength = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricRankCacheLength,
		Help:      "TODO",
	},
)

var CounterCacheThresholdReached = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricCacheThresholdReached,
		Help:      "TODO",
	},
)

var CounterReadDirtyCache = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricReadDirtyCache,
		Help:      "TODO",
	},
)

var CounterSetBit = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricSetBit,
		Help:      "TODO",
	},
)

var CounterClearBit = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricClearBit,
		Help:      "TODO",
	},
)

var CounterSetRow = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "setRow",
		Help:      "TODO",
	},
)

var CounterImportingN = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricImportingN,
		Help:      "TODO",
	},
)

var CounterImportedN = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricImportedN,
		Help:      "TODO",
	},
)

var CounterClearingingN = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricClearingN,
		Help:      "TODO",
	},
)

var CounterClearedN = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      MetricClearedN,
		Help:      "TODO",
	},
)

var SummaryGRPCStreamQueryDurationSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricGRPCStreamQueryDurationSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

var SummaryGRPCStreamFormatDurationSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricGRPCStreamFormatDurationSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

var SummaryGRPCUnaryQueryDurationSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricGRPCUnaryQueryDurationSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

var SummaryGRPCUnaryFormatDurationSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricGRPCUnaryFormatDurationSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

var SummaryBatchImportDurationSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricBatchImportDurationSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

var SummaryBatchFlushDurationSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricBatchFlushDurationSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

var SummaryBatchShardImportBuildRequestsSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricBatchShardImportBuildRequestsSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

var SummaryBatchShardImportDurationSeconds = prometheus.NewSummary(
	prometheus.SummaryOpts{
		Namespace:  "pilosa",
		Name:       MetricBatchShardImportDurationSeconds,
		Help:       "TODO",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
)

// index pql call related

var CounterQuerySumTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_sum_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryMinTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_min_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryMaxTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_max_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryMinRowTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_minrow_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryMaxRowTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_maxrow_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryClearTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_clear_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryClearRowTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_clearrow_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryDistinctTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_distinct_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryStoreTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_store_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryCountTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_count_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQuerySetTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_set_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryTopKTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_topk_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryTopNTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_topn_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryRowsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_rows_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryExternalLookupTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_externallookup_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryExtractTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_extract_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryGroupByTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_groupby_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryOptionsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_options_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryIncludesColumnTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_includescolumn_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryFieldValueTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_fieldvalue_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryPrecomputedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_precomputed_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryUnionRowsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_unionrows_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryConstRowTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_constrow_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryLimitTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_limit_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryPercentileTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_percentile_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryDeleteTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_delete_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQuerySortTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_sort_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryApplyTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_apply_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryArrowTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_arrow_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

// CounterQueryBitmapTotal represents bitmap calls.
var CounterQueryBitmapTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_bitmap_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryRowTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_row_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryRowBSITotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_row_bsi_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryRangeTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_range_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryDifferenceTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_difference_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryIntersectTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_intersect_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryUnionTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_union_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryInnerUnionRowsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_innerunionrows_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryXorTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_xor_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryNotTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_not_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryShiftTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_shift_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

var CounterQueryAllTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pilosa",
		Name:      "query_all_total",
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

// index related

var GaugeIndexMaxShard = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pilosa",
		Name:      MetricMaxShard,
		Help:      "TODO",
	},
	[]string{
		"index",
	},
)

func init() {
	// server related
	prometheus.MustRegister(CounterJobTotal)
	prometheus.MustRegister(GaugeWorkerTotal)
	prometheus.MustRegister(CounterPQLQueries)
	prometheus.MustRegister(CounterSQLQueries)
	prometheus.MustRegister(CounterGarbageCollection)
	prometheus.MustRegister(GaugeGoroutines)
	prometheus.MustRegister(GaugeOpenFiles)
	prometheus.MustRegister(GaugeHeapAlloc)
	prometheus.MustRegister(GaugeHeapInUse)
	prometheus.MustRegister(GaugeStackInUse)
	prometheus.MustRegister(GaugeMallocs)
	prometheus.MustRegister(GaugeFrees)
	prometheus.MustRegister(SummaryHttpRequests)
	prometheus.MustRegister(CounterCreateIndex)
	prometheus.MustRegister(CounterDeleteIndex)
	prometheus.MustRegister(CounterCreateField)
	prometheus.MustRegister(CounterDeleteField)
	prometheus.MustRegister(CounterDeleteAvailableShard)
	prometheus.MustRegister(CounterDeleteDataframe)
	prometheus.MustRegister(CounterExclusiveTransactionRequest)
	prometheus.MustRegister(CounterTransactionStart)
	prometheus.MustRegister(CounterExclusiveTransactionBlocked)
	prometheus.MustRegister(CounterTransactionBlocked)
	prometheus.MustRegister(CounterExclusiveTransactionActive)
	prometheus.MustRegister(CounterExclusiveTransactionEnd)
	prometheus.MustRegister(CounterTransactionEnd)
	prometheus.MustRegister(CounterRecalculateCache)
	prometheus.MustRegister(CounterInvalidateCacheSkipped)
	prometheus.MustRegister(CounterInvalidateCache)
	prometheus.MustRegister(GaugeRankCacheLength)
	prometheus.MustRegister(CounterCacheThresholdReached)
	prometheus.MustRegister(CounterReadDirtyCache)
	prometheus.MustRegister(CounterSetBit)
	prometheus.MustRegister(CounterClearBit)
	prometheus.MustRegister(CounterSetRow)
	prometheus.MustRegister(CounterImportingN)
	prometheus.MustRegister(CounterImportedN)
	prometheus.MustRegister(CounterClearingingN)
	prometheus.MustRegister(CounterClearedN)
	prometheus.MustRegister(SummaryGRPCStreamQueryDurationSeconds)
	prometheus.MustRegister(SummaryGRPCStreamFormatDurationSeconds)
	prometheus.MustRegister(SummaryGRPCUnaryQueryDurationSeconds)
	prometheus.MustRegister(SummaryGRPCUnaryFormatDurationSeconds)
	prometheus.MustRegister(SummaryBatchImportDurationSeconds)
	prometheus.MustRegister(SummaryBatchFlushDurationSeconds)
	prometheus.MustRegister(SummaryBatchShardImportBuildRequestsSeconds)
	prometheus.MustRegister(SummaryBatchShardImportDurationSeconds)

	// pql calls
	prometheus.MustRegister(CounterQuerySumTotal)
	prometheus.MustRegister(CounterQueryMinTotal)
	prometheus.MustRegister(CounterQueryMaxTotal)
	prometheus.MustRegister(CounterQueryMinRowTotal)
	prometheus.MustRegister(CounterQueryMaxRowTotal)
	prometheus.MustRegister(CounterQueryClearTotal)
	prometheus.MustRegister(CounterQueryClearRowTotal)
	prometheus.MustRegister(CounterQueryDistinctTotal)
	prometheus.MustRegister(CounterQueryStoreTotal)
	prometheus.MustRegister(CounterQueryCountTotal)
	prometheus.MustRegister(CounterQuerySetTotal)
	prometheus.MustRegister(CounterQueryTopKTotal)
	prometheus.MustRegister(CounterQueryTopNTotal)
	prometheus.MustRegister(CounterQueryRowsTotal)
	prometheus.MustRegister(CounterQueryExternalLookupTotal)
	prometheus.MustRegister(CounterQueryExtractTotal)
	prometheus.MustRegister(CounterQueryGroupByTotal)
	prometheus.MustRegister(CounterQueryOptionsTotal)
	prometheus.MustRegister(CounterQueryIncludesColumnTotal)
	prometheus.MustRegister(CounterQueryFieldValueTotal)
	prometheus.MustRegister(CounterQueryPrecomputedTotal)
	prometheus.MustRegister(CounterQueryUnionRowsTotal)
	prometheus.MustRegister(CounterQueryConstRowTotal)
	prometheus.MustRegister(CounterQueryLimitTotal)
	prometheus.MustRegister(CounterQueryPercentileTotal)
	prometheus.MustRegister(CounterQueryDeleteTotal)
	prometheus.MustRegister(CounterQuerySortTotal)
	prometheus.MustRegister(CounterQueryApplyTotal)
	prometheus.MustRegister(CounterQueryArrowTotal)
	prometheus.MustRegister(CounterQueryBitmapTotal)
	prometheus.MustRegister(CounterQueryRowTotal)
	prometheus.MustRegister(CounterQueryRowBSITotal)
	prometheus.MustRegister(CounterQueryRangeTotal)
	prometheus.MustRegister(CounterQueryDifferenceTotal)
	prometheus.MustRegister(CounterQueryIntersectTotal)
	prometheus.MustRegister(CounterQueryUnionTotal)
	prometheus.MustRegister(CounterQueryInnerUnionRowsTotal)
	prometheus.MustRegister(CounterQueryXorTotal)
	prometheus.MustRegister(CounterQueryNotTotal)
	prometheus.MustRegister(CounterQueryShiftTotal)
	prometheus.MustRegister(CounterQueryAllTotal)

	// index related
	prometheus.MustRegister(GaugeIndexMaxShard)

}
