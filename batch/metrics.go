// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package batch

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
