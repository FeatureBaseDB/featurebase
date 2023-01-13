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

// WritelogService represents the WritelogService methods which Computer uses.
// These are typically implemented by the Writelogger client.
type WritelogService interface {
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

// SnapInfo holds metadata about a snapshot.
type SnapInfo struct {
	Version int
	// Date    time.Time
}

// WriteLogInfo holds metadata about a write log.
type WriteLogInfo SnapInfo

// LogMessage is implemented by a variety of types which can be serialized as
// messages to the Writelogger.
type LogMessage interface{}
