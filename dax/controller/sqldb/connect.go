package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/gobuffalo/pop/v6"
)

func Connect(cfg *controller.SQLDBConfig) (controller.Transactor, error) {
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
		return nil, errors.Wrap(err, "creating new connection")
	}

	err = conn.Open()
	if err != nil {
		return nil, errors.Wrap(err, "opening connection")
	}

	return Transactor{Connection: conn}, nil
}
