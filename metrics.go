// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

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
	MetricSetRowAttrs                     = "query_setrowattrs_total"
	MetricSetColumnAttrs                  = "query_setcolumnattrs_total"
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
	MetricColumnAttrStoreBlocks           = "column_attr_store_blocks_total"
	MetricColumnAttrDiff                  = "column_attr_diff_total"
	MetricRowAttrStoreBlocks              = "row_attr_store_blocks_total"
	MetricRowAttrDiff                     = "row_attr_diff_total"
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
)
