package pilosa

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v3/dax"
)

// MDS represents the MDS methods which Computer uses. These are typically
// implemented by both the MDS service and the MSD client.
type MDS interface {
	RegisterNode(ctx context.Context, node *dax.Node) error
	CheckInNode(ctx context.Context, node *dax.Node) error
}

// WriteLogger represents the WriteLogger methods which Computer uses. These are
// typically implemented by both the WriteLogger service and the WriteLogger
// client.
type WriteLogger interface {
	AppendMessage(bucket string, key string, version int, msg []byte) error
	LogReader(bucket string, key string, version int) (io.Reader, io.Closer, error)
	DeleteLog(bucket string, key string, version int) error
}

// Snapshotter represents the Snapshotter methods which Computer uses. These are
// typically implemented by both the Snapshotter service and the Snapshotter
// client.
type Snapshotter interface {
	Read(bucket string, key string, version int) (io.ReadCloser, error)
	Write(bucket string, key string, version int, rc io.ReadCloser) error
	WriteTo(bucket string, key string, version int, wrTo io.WriterTo) error
}
