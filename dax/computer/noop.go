package computer

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v3/dax"
)

// Ensure type implements interface.
var _ WriteLogWriter = (*NopWriteLogWriter)(nil)

// NopWriteLogWriter is a no-op implementation of the WriteLogWriter interface.
type NopWriteLogWriter struct{}

func NewNopWriteLogWriter() *NopWriteLogWriter {
	return &NopWriteLogWriter{}
}

func (w *NopWriteLogWriter) CreateTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, m map[string]uint64) error {
	return nil
}

func (w *NopWriteLogWriter) DeleteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) error {
	return nil
}

func (w *NopWriteLogWriter) CreateFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, m map[string]uint64) error {
	return nil
}

func (w *NopWriteLogWriter) DeleteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) error {
	return nil
}

func (w *NopWriteLogWriter) WriteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, msg LogMessage) error {
	return nil
}

func (w *NopWriteLogWriter) DeleteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) error {
	return nil
}

// Ensure type implements interface.
var _ WriteLogReader = (*NopWriteLogReader)(nil)

// NopWriteLogReader is a no-op implementation of the WriteLogReader interface.
type NopWriteLogReader struct{}

func NewNopWriteLogReader() *NopWriteLogReader {
	return &NopWriteLogReader{}
}

func (w *NopWriteLogReader) TableKeyReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) TableKeyReader {
	return NewNopTableKeyReader()
}

func (w *NopWriteLogReader) FieldKeyReader(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) FieldKeyReader {
	return NewNopFieldKeyReader()
}

func (w *NopWriteLogReader) ShardReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) ShardReader {
	return NewNopShardReader()
}

////////////////////////////////////////////////

// Ensure type implements interface.
var _ TableKeyReader = &NopTableKeyReader{}

// NopTableKeyReader is a no-op implementation of the TableKeyReader
// interface.
type NopTableKeyReader struct{}

func NewNopTableKeyReader() *NopTableKeyReader {
	return &NopTableKeyReader{}
}

func (r *NopTableKeyReader) Open() error { return nil }
func (r *NopTableKeyReader) Read() (PartitionKeyMap, error) {
	return PartitionKeyMap{}, io.EOF
}
func (r *NopTableKeyReader) Close() error { return nil }

////////////////////////////////////////////////

// Ensure type implements interface.
var _ FieldKeyReader = &NopFieldKeyReader{}

// NopFieldKeyReader is a no-op implementation of the FieldKeyReader
// interface.
type NopFieldKeyReader struct{}

func NewNopFieldKeyReader() *NopFieldKeyReader {
	return &NopFieldKeyReader{}
}

func (r *NopFieldKeyReader) Open() error { return nil }
func (r *NopFieldKeyReader) Read() (FieldKeyMap, error) {
	return FieldKeyMap{}, io.EOF
}
func (r *NopFieldKeyReader) Close() error { return nil }

////////////////////////////////////////////////

// Ensure type implements interface.
var _ ShardReader = &NopShardReader{}

// NopShardReader is a no-op implementation of the ShardReader interface.
type NopShardReader struct{}

func NewNopShardReader() *NopShardReader {
	return &NopShardReader{}
}

func (r *NopShardReader) Open() error { return nil }
func (r *NopShardReader) Read() (LogMessage, error) {
	return nil, io.EOF
}
func (r *NopShardReader) Close() error { return nil }

////////////// SNAPSHOT ////////////////////////

// Ensure type implements interface.
var _ SnapshotReadWriter = &NopSnapshotReadWriter{}

// NopSnapshotReadWriter is a no-op implementation of the SnapshotReadWriter
// interface.
type NopSnapshotReadWriter struct{}

func NewNopSnapshotReadWriter() *NopSnapshotReadWriter {
	return &NopSnapshotReadWriter{}
}

func (w *NopSnapshotReadWriter) WriteShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, rc io.ReadCloser) error {
	return nil
}

func (w *NopSnapshotReadWriter) ReadShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) (io.ReadCloser, error) {
	return &nopReadCloser{}, nil
}

func (w *NopSnapshotReadWriter) WriteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, wrTo io.WriterTo) error {
	return nil
}

func (w *NopSnapshotReadWriter) ReadTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) (io.ReadCloser, error) {
	return &nopReadCloser{}, nil
}

func (w *NopSnapshotReadWriter) WriteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, wrTo io.WriterTo) error {
	return nil
}

func (w *NopSnapshotReadWriter) ReadFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) (io.ReadCloser, error) {
	return &nopReadCloser{}, nil
}

type nopReadCloser struct{}

func (n *nopReadCloser) Read([]byte) (int, error) { return 0, nil }
func (n *nopReadCloser) Close() error             { return nil }
