package computer

import (
	"context"
	"io"

	"github.com/featurebasedb/featurebase/v3/dax"
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
	LogReader(bucket string, key string, version int) (io.ReadCloser, error)
	LogReaderFrom(bucket string, key string, version int, offset int) (io.ReadCloser, error)
	DeleteLog(bucket string, key string, version int) error
	List(bucket, key string) ([]WriteLogInfo, error)
	Lock(bucket, key string) error
	Unlock(bucket, key string) error
}

// SnapshotService represents the SnapshotService methods which Computer uses.
// These are typically implemented by both the Snapshotter client.
type SnapshotService interface {
	Read(bucket string, key string, version int) (io.ReadCloser, error)
	Write(bucket string, key string, version int, rc io.ReadCloser) error
	WriteTo(bucket string, key string, version int, wrTo io.WriterTo) error
	List(bucket, key string) ([]SnapInfo, error)
}

// ServerlessStorage is the interface to a particular shard or
// translation store that contains both writelogger and
// snapshotter. The interface is designed such that implementations
// are expected to be stateful.
//
// One must not call LoadWriteLog until after calling
// LoadLatestSnapshot. One must not call Append, IncrementWLVersion,
// or Snapshot until after successfully calling Lock.
type ServerlessStorage interface {
	// LoadLatestSnapshot loads the latest available snapshot in the snapshot store.
	LoadLatestSnapshot() (data io.ReadCloser, err error)

	// // Potential future methods to support getting older versions. SnapInfo would have timestamp information as well.
	//
	// ListSnapshots() []SnapInfo
	// LoadSnapshot(version int) (data io.ReadCloser, err error)

	// LoadWriteLog can be called after LoadLatestSnapshot. It loads
	// any writelog data which has been written since the latest
	// snapshot. Subsequent calls to LoadWriteLog will only return new
	// data that hasn't previously been returned from LoadWriteLog.
	LoadWriteLog() (data io.ReadCloser, err error)

	// Lock acquires an advisory lock for this resource which grants
	// us exclusive access to write to it.  The normal pattern is to
	// call:
	//
	// 1. LoadLatestSnapshot
	// 2. LoadWriteLog
	// 3. Lock
	// 4. LoadWriteLog
	//
	// The second call to LoadWriteLog is necessary in case any writes
	// occurred between the last load and acquiring the lock. Once the
	// lock is acquired it should not be possible for any more writes
	// to occur. Lock will error if (a) we fail to acquire the lock or
	// (b) the state of the snapshot store for this resource is not
	// identical to what is was before the lock was acquired. Case (b)
	// means that quite a lot has happened in between LoadWriteLog and
	// Lock, and we should probably just die and start over.
	Lock() error

	// Append appends the msg to the write log. It will fail if we
	// haven't properly loaded and gotten a lock for the resource
	// we're writing to.
	Append(msg []byte) error

	// IncrementWLVersion should be called during snapshotting with a
	// write Tx held on the local resource. This ensures that any
	// writes which completed prior to the snapshot are in the prior
	// WL and any that complete after the snapshot are in the
	// incremented WL.
	IncrementWLVersion() error

	// Snapshot takes a ReadCloser which has the contents of the
	// resource being tracked at a particular point in time and writes
	// them to the Snapshot Store. Upon a successful write it will
	// truncate any write logs which are now incorporated into the
	// snapshot.
	Snapshot(rc io.ReadCloser) error
	SnapshotTo(wt io.WriterTo) error

	// Unlock releases the lock. This should be called if control of
	// the underlying resource is being transitioned to another
	// node. Ideally it's also called if the process crashes (e.g. via
	// a defer), but an implementation based on filesystem locks
	// should have those removed by the operating system when the
	// process exits anyway.
	Unlock() error
}

// SnapInfo holds metadata about a snapshot.
type SnapInfo struct {
	Version int
	// Date    time.Time
}

// WriteLogInfo holds metadata about a write log.
type WriteLogInfo SnapInfo

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
