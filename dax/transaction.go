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

const (
	postgresTxConflictError = "(SQLSTATE 40001)"
)

// txFunc is the function signature for a function which can be retried using
// the RetryWithTx function.
type txFunc func(tx Transaction, writable bool) error

// RetryWithTx will retry the txFunc up to maxTries, or a try succeeds,
// whichever comes first. If writable is set to true, RetryWithTx will use a
// writable transaction for each try, and attempt to Commit the transaction. If
// the transaction fails with an error related to invalid serialization, and
// there are still tries remaining, the transaction will be retried.
func RetryWithTx(ctx context.Context, trans Transactor, fn txFunc, writable bool, maxTries int) error {
	// stopRetry can be set to true to abort the retry loop. This is useful when
	// a transaction completes successfully, but maxTries has not been reached;
	// i.e, because the transaction succeeded, there's no reason to keep trying.
	var stopRetry bool

	for maxTries >= 1 && !stopRetry {
		maxTries--

		if err := func() error {
			// Begin a read transaction.
			tx, err := trans.BeginTx(ctx, writable)
			if err != nil {
				return errors.Wrapf(err, "beginning tx, writable: %v", writable)
			}
			defer tx.Rollback()

			// Call the function with the transaction. We pass in writable in
			// case the function operates differently based on whether it is a
			// read or write transaction.
			if err := fn(tx, writable); err != nil {
				return errors.Wrapf(err, "calling function with tx, writable: %v", writable)
			}

			if writable {
				if err := tx.Commit(); err != nil {
					return errors.Wrap(err, "committing tx")
				}
			}

			stopRetry = true
			return nil
		}(); err != nil {
			// If we get a serialization error, and we still have some write
			// attempts remaining, then continue trying.
			if maxTries > 0 && strings.Contains(err.Error(), postgresTxConflictError) {
				continue
			}
			return err
		}
	}

	return nil
}
