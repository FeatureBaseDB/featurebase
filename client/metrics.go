// Copyright 2021 Molecula Corp. All rights reserved.
package client

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
)
