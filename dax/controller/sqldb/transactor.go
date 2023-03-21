package sqldb

import (
	"context"

	"database/sql"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gobuffalo/pop/v6"
)

// Transactor wraps a pop Connection to make it into a dax.Transactor
// which can be used by the controller agnostic of implementation.
type Transactor struct {
	*pop.Connection

	logger logger.Logger
}

func NewTransactor(cfg *controller.SQLDBConfig, log logger.Logger) (Transactor, error) {
	conn, err := pop.NewConnection(&pop.ConnectionDetails{
		Dialect:         cfg.Dialect,
		Database:        cfg.Database,
		Host:            cfg.Host,
		Port:            cfg.Port,
		User:            cfg.User,
		Password:        cfg.Password,
		URL:             cfg.URL,
		Pool:            cfg.Pool,
		IdlePool:        cfg.IdlePool,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
	})
	if err != nil {
		return Transactor{Connection: nil}, errors.Wrap(err, "creating new connection")
	}

	return Transactor{
		Connection: conn,
		logger:     log,
	}, nil
}

// Start creates the database specified in the database connection, then runs
// any outstanding migrations.
func (t Transactor) Start() error {
	conn := t.Connection

	// Create the database if it doesn't exist.
	if err := pop.CreateDB(conn); err != nil {
		t.logger.Warnf("auto-creating database, got error '%v'", err)
	}

	// Open a connection to the database.
	if err := conn.Open(); err != nil {
		return errors.Wrap(err, "opening connection")
	}

	// Run migrations.
	if mig, err := NewEmbedMigrator(dax.MigrationsFS, conn, t.logger); err != nil {
		return errors.Wrap(err, "getting embedded migrator")
	} else if err = mig.Up(); err != nil {
		return errors.Wrap(err, "migrating DB")
	}

	return nil
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

// DaxTransaction is a thin wrapper to create a dax.Transaction from a
// pop Transaction/Connection.
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

// DropDatabase drops the database associated with the given
// Transactor (which embeds a live database connection). This is
// destructive, you will lose data.
func DropDatabase(trans Transactor) error {
	conn := trans.Connection
	return pop.DropDB(conn)
}
