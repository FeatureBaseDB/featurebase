package computer

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v3/dax"
)

// Registrar represents the methods which Computer uses to register itself with
// MDS.
type Registrar interface {
	RegisterNode(ctx context.Context, node *dax.Node) error
	CheckInNode(ctx context.Context, node *dax.Node) error
}

// WriteLogService represents the WriteLogService methods which Computer uses.
// These are typically implemented by the WriteLogger client.
type WriteLogService interface {
	AppendMessage(bucket string, key string, version int, msg []byte) error
	LogReader(bucket string, key string, version int) (io.Reader, io.Closer, error)
	DeleteLog(bucket string, key string, version int) error
}

// SnapshotService represents the SnapshotService methods which Computer uses.
// These are typically implemented by both the Snapshotter client.
type SnapshotService interface {
	Read(bucket string, key string, version int) (io.ReadCloser, error)
	Write(bucket string, key string, version int, rc io.ReadCloser) error
	WriteTo(bucket string, key string, version int, wrTo io.WriterTo) error
}

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

// WriteLogWriter provides the interface for all data writes to FeatureBase. After
// data has been written to the local FeatureBase node, the respective interface
// method(s) will be called.
type WriteLogWriter interface {
	// CreateTableKeys sends a map of string key to uint64 ID for the table and
	// partition provided.
	CreateTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, _ map[string]uint64) error

	// DeleteTableKeys deletes all table keys for the table and partition
	// provided.
	DeleteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) error

	// CreateFieldKeys sends a map of string key to uint64 ID for the table and
	// field provided.
	CreateFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, _ map[string]uint64) error

	// DeleteTableKeys deletes all field keys for the table and field provided.
	DeleteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) error

	// WriteShard sends shard data for the table and shard provided.
	WriteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, msg LogMessage) error

	// DeleteShard deletes all data for the table and shard provided.
	DeleteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) error
}

// WriteLogReader provides the interface for all reads from the write log.
type WriteLogReader interface {
	ShardReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) ShardReader
	TableKeyReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) TableKeyReader
	FieldKeyReader(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) FieldKeyReader
}

type TableKeyReader interface {
	Open() error
	Read() (PartitionKeyMap, error)
	Close() error
}

type FieldKeyReader interface {
	Open() error
	Read() (FieldKeyMap, error)
	Close() error
}

type ShardReader interface {
	Open() error
	Read() (LogMessage, error)
	Close() error
}

// LogMessage is implemented by a variety of types which can be serialized as
// messages to the WriteLogger.
type LogMessage interface{}
