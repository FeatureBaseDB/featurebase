package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gobuffalo/pop/v6"
)

func Connect(cfg *controller.SQLDBConfig, log logger.Logger) (Transactor, error) {
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

	err = pop.CreateDB(conn)

	if err != nil {
		log.Warnf("auto-creating database, got error '%v'", err)
	}

	err = conn.Open()
	if err != nil {
		return Transactor{Connection: nil}, errors.Wrap(err, "opening connection")
	}

	mig, err := NewEmbedMigrator(dax.MigrationsFS, conn, log)
	if err != nil {
		return Transactor{Connection: nil}, errors.Wrap(err, "getting embedded migrator")
	}
	err = mig.Up()
	if err != nil {
		return Transactor{Connection: nil}, errors.Wrap(err, "migrating DB")
	}

	return Transactor{Connection: conn, Migrator: mig}, nil
}

// DropDatabase drops the database associated with the given
// Transactor (which embeds a live database connection). This is
// destructive, you will lose data.
func DropDatabase(trans Transactor) error {
	conn := trans.Connection
	return pop.DropDB(conn)
}
