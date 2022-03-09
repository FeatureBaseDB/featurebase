package computer

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v3/dax"
)

// SnapshotReadWriter provides the interface for all snapshot read and writes in
// FeatureBase.
type SnapshotReadWriter interface {
	WriteShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, rc io.ReadCloser) error
	ReadShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) (io.ReadCloser, error)

	WriteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, wrTo io.WriterTo) error
	ReadTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) (io.ReadCloser, error)

	WriteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, wrTo io.WriterTo) error
	ReadFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) (io.ReadCloser, error)
}

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
