package sqldb

import (
	"context"

	"database/sql"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/gobuffalo/pop/v6"
)

// Transactor wraps a pop Connection to make it into a dax.Transactor
// which can be used by the controller agnostic of implementation.
type Transactor struct {
	*pop.Connection
}

func (t Transactor) BeginTx(ctx context.Context, writable bool) (dax.Transaction, error) {
	cn, err := t.NewTransactionContextOptions(ctx, &sql.TxOptions{ReadOnly: !writable})
	if err != nil {
		return nil, errors.Wrap(err, "getting SQL transaction")
	}
	return &DaxTransaction{C: cn}, nil
}

func (t Transactor) Close() error {
	return t.Connection.Close()
}

// sqldb.DaxTransaction is a thin wrapper to create a dax.Transaction
// from a pop Transaction/Connection.
type DaxTransaction struct {
	C *pop.Connection
}

func (w *DaxTransaction) Commit() error {
	return w.C.TX.Commit()
}

func (w *DaxTransaction) Context() context.Context {
	return w.C.Context()
}
func (w *DaxTransaction) Rollback() error {
	return w.C.TX.Rollback()
}
