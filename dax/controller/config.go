package controller

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/logger"
)

type NewBalancerFn func(string, logger.Logger) Balancer

// TODO honestly, I think a lot of this stuff should be moved into the
// service package. It's config for specific implementations of things
// that are going to be injected into the controller.

type Config struct {
	Director Director

	// Poller
	PollInterval time.Duration `toml:"poll-interval"`

	// Storage
	StorageMethod string `toml:"storage-method"`

	SQLDB *SQLDBConfig `toml:"sqldb"`

	SnapshotterDir string `toml:"snapshotter-dir"`
	WriteloggerDir string `toml:"writelogger-dir"`

	// RegistrationBatchTimeout is the time that the controller will
	// wait after a node registers itself to see if any more nodes
	// will register before sending out directives to all nodes which
	// have been registered.
	RegistrationBatchTimeout time.Duration

	// SnappingTurtleTimeout is the period on which the automatic
	// snapshotting routine will run. If performing all the snapshots
	// takes longer than this amount of time, snapshotting will run
	// continuously. If it finishes before the timeout, it will wait
	// until the timeout expires to start another round of snapshots.
	SnappingTurtleTimeout time.Duration

	Logger logger.Logger `toml:"-"`
}

type SQLDBConfig struct {
	// Dialect is the pop dialect to use. Example: "postgres" or "sqlite3" or "mysql"
	Dialect string
	// The name of your database. Example: "foo_development"
	Database string
	// The host of your database. Example: "127.0.0.1"
	Host string
	// The port of your database. Example: 1234
	// Will default to the "default" port for each dialect.
	Port string
	// The username of the database user. Example: "root"
	User string
	// The password of the database user. Example: "password"
	Password string
	// Instead of specifying each individual piece of the
	// connection you can instead just specify the URL of the
	// database. Example: "postgres://postgres:postgres@localhost:5432/pop_test?sslmode=disable"
	URL string
	// Defaults to 0 "unlimited". See https://golang.org/pkg/database/sql/#DB.SetMaxOpenConns
	Pool int
	// Defaults to 2. See https://golang.org/pkg/database/sql/#DB.SetMaxIdleConns
	IdlePool int
	// Defaults to 0 "unlimited". See https://golang.org/pkg/database/sql/#DB.SetConnMaxLifetime
	ConnMaxLifetime time.Duration
	// Defaults to 0 "unlimited". See https://golang.org/pkg/database/sql/#DB.SetConnMaxIdleTime
	ConnMaxIdleTime time.Duration
}

func NewSQLDBConfig() *SQLDBConfig {
	return &SQLDBConfig{
		Dialect:  "postgres",
		Database: "controller",
		Host:     "127.0.0.1",
		Pool:     0,
		IdlePool: 2,
	}
}
