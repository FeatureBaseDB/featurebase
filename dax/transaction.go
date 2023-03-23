package dax

import (
	"context"
	"strings"

	"github.com/featurebasedb/featurebase/v3/errors"
)

type Transaction interface {
	Commit() error
	Context() context.Context
	Rollback() error
}

type Transactor interface {
	// Start is useful for Transactor implementations which need to establish a
	// connection. We don't want to do that in the NewImplementation() function;
	// we want that to happen upon Start().
	Start() error

	BeginTx(ctx context.Context, writable bool) (Transaction, error)
	Close() error
}

type txFunc func(tx Transaction, writable bool) error

func RetryWithTx(ctx context.Context, trans Transactor, fn txFunc, reads int, writes int) error {
	// Try with a read lock first.
	for reads >= 1 {
		reads--

		// Begin a read transaction.
		tx, err := trans.BeginTx(ctx, false)
		if err != nil {
			return errors.Wrap(err, "beginning read tx")
		}
		defer tx.Rollback()

		// Call the function as read.
		if err := fn(tx, false); err != nil {
			return errors.Wrap(err, "calling function with read tx")
		}
	}

	// Try with a write lock.
	for writes >= 1 {
		writes--

		// Begin a write transaction.
		tx, err := trans.BeginTx(ctx, true)
		if err != nil {
			return errors.Wrap(err, "beginning read tx")
		}
		defer tx.Rollback()

		// Call the function as write.
		if err := fn(tx, true); err != nil {
			if writes > 0 && strings.Contains(err.Error(), "(SQLSTATE 40001)") {
				continue
			}
			return errors.Wrap(err, "calling function with write tx")
		}

		// If we get a serialization error, and we still have some write
		// attempts remaining, then continue trying.
		if err := tx.Commit(); err != nil {
			if writes > 0 && strings.Contains(err.Error(), "(SQLSTATE 40001)") {
				continue
			}
			return errors.Wrap(err, "committing tx")
		}
	}

	return nil
}
