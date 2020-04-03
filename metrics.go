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
	MetricCreateIndex           = "create_index_total"
	MetricDeleteIndex           = "delete_index_total"
	MetricCreateField           = "create_field_total"
	MetricDeleteField           = "delete_field_total"
	MetricDeleteAvailableShard  = "delete_available_shard_total"
	MetricRecalculateCache      = "recalculate_cache_total"
	MetricInvalidateCache       = "invalidate_cache_total"
	MetricRankCacheLength       = "rank_cache_length"
	MetricCacheThresholdReached = "cache_threshold_reached_total"
	MetricRow                   = "query_row_total"
	MetricRowBSI                = "query_row_bsi_total"
	MetricSetRowAttrs           = "query_setrowattrs_total"
	MetricSetColumnAttrs        = "query_setcolumnattrs_total"
	MetricMaximumRow            = "maximum_row"
	MetricSetBit                = "set_bit_total"
	MetricClearBit              = "clear_bit_total"
	MetricSetRow                = "set_row_total"
	MetricClearRow              = "clear_row_total"
	MetricImportingN            = "importing_total"
	MetricImportedN             = "imported_total"
	MetricClearingN             = "clearing_total"
	MetricClearedN              = "cleared_total"
	MetricSnapshotDuration      = "snapshot_duration_seconds"
	MetricBlockRepairPrimary    = "block_repair_primary_total"
	MetricBlockRepair           = "block_repair_total"
	MetricSyncFieldDuration     = "sync_field_duration_seconds"
	MetricSyncIndexDuration     = "sync_index_duration_seconds"
	MetricColumnAttrStoreBlocks = "ColumnAttrStoreBlocks"
	MetricColumnAttrDiff        = "ColumnAttrDiff"
	MetricRowAttrStoreBlocks    = "RowAttrStoreBlocks"
	MetricRowAttrDiff           = "RowAttrDiff"
	MetricHttpRequest           = "http_request_total"
	MetricMaxShard              = "maximum_shard"
	MetricAntiEntropy           = "antientropy_total"
	MetricAntiEntropyDuration   = "antientropy_duration_seconds"
	MetricGarbageCollection     = "garbage_collection_total"
	MetricGoroutines            = "goroutines"
	MetricOpenFiles             = "open_files"
	MetricHeapAlloc             = "heap_alloc"
	MetricHeapInuse             = "heap_inuse"
	MetricStackInuse            = "stack_inuse"
	MetricMallocs               = "mallocs"
	MetricFrees                 = "frees"
)
