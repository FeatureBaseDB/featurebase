package controller

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
)

type Transactor interface {
	BeginTx(ctx context.Context, writable bool) (dax.Transaction, error)
	Close() error
}
