package batch

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
)

// Batcher is an interface implemented by anything which can allocate new
// batches.
type Batcher interface {
	NewBatch(cfg Config, tbl *dax.Table, fields []*dax.Field) (RecordBatch, error)
}

// Config is the configuration options passed to NewBatch for any implementation
// of the Batcher interface.
type Config struct {
	Size         int
	MaxStaleness time.Duration
}
