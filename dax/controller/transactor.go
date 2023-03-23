package controller

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
)

type Transactor interface {
	// Start is useful for Transactor implementations which need to establish a
	// connection. We don't want to do that in the NewImplementation() function;
	// we want that to happen upon Start().
	Start() error

	BeginTx(ctx context.Context, writable bool) (dax.Transaction, error)
	Close() error
}
