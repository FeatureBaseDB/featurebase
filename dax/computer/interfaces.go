package computer

import (
	"context"
	"io"

	"github.com/featurebasedb/featurebase/v3/dax"
)

// Registrar represents the methods which Computer uses to register itself with
// the Controller.
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

/////////// No-op implementations of the interfaces //////////////

// nopWritelogService is a no-op implementation of the WritelogService
// interface.
type nopWritelogService struct{}

func NewNopWritelogService() *nopWritelogService {
	return &nopWritelogService{}
}
func (b *nopWritelogService) AppendMessage(bucket string, key string, version int, msg []byte) error {
	return nil
}
func (b *nopWritelogService) LogReader(bucket string, key string, version int) (io.ReadCloser, error) {
	return nil, nil
}
func (b *nopWritelogService) LogReaderFrom(bucket string, key string, version int, offset int) (io.ReadCloser, error) {
	return nil, nil
}
func (b *nopWritelogService) DeleteLog(bucket string, key string, version int) error {
	return nil
}
func (b *nopWritelogService) List(bucket, key string) ([]WriteLogInfo, error) {
	return nil, nil
}
func (b *nopWritelogService) Lock(bucket, key string) error {
	return nil
}
func (b *nopWritelogService) Unlock(bucket, key string) error {
	return nil
}

// nopSnapshotterService is a no-op implementation of the WritelogService
// interface.
type nopSnapshotterService struct{}

func NewNopSnapshotterService() *nopSnapshotterService {
	return &nopSnapshotterService{}
}
func (b *nopSnapshotterService) Read(bucket string, key string, version int) (io.ReadCloser, error) {
	return nil, nil
}
func (b *nopSnapshotterService) Write(bucket string, key string, version int, rc io.ReadCloser) error {
	return nil
}
func (b *nopSnapshotterService) WriteTo(bucket string, key string, version int, wrTo io.WriterTo) error {
	return nil
}
func (b *nopSnapshotterService) List(bucket, key string) ([]SnapInfo, error) {
	return nil, nil
}
