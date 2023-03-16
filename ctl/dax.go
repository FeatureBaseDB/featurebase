package ctl

import (
	"github.com/featurebasedb/featurebase/v3/dax/server"
	"github.com/spf13/cobra"
)

// BuildDAXFlags attaches a set of flags to the command for a server instance.
func BuildDAXFlags(cmd *cobra.Command, srv *server.Command) {
	flags := cmd.Flags()

	flags.StringVarP(&srv.Config.Bind, "bind", "b", srv.Config.Bind, "Default URI on which this service should listen.")
	flags.StringVar(&srv.Config.Advertise, "advertise", srv.Config.Advertise, "Address to advertise externally.")
	flags.BoolVar(&srv.Config.Verbose, "verbose", srv.Config.Verbose, "Enable verbose logging")
	flags.StringVar(&srv.Config.LogPath, "log-path", srv.Config.LogPath, "Log path")

	// Controller
	flags.BoolVar(&srv.Config.Controller.Run, "controller.run", srv.Config.Controller.Run, "Run the Controller service in process.")
	flags.DurationVar(&srv.Config.Controller.Config.RegistrationBatchTimeout, "controller.config.registration-batch-timeout", srv.Config.Controller.Config.RegistrationBatchTimeout, "Timeout for node registration batches.")
	flags.StringVar(&srv.Config.Controller.Config.DataDir, "controller.config.data-dir", srv.Config.Controller.Config.DataDir, "Controller directory to use in process.")
	flags.StringVar(&srv.Config.Controller.Config.StorageMethod, "controller.config.storage-method", srv.Config.Controller.Config.StorageMethod, "Backing store. boltdb or sqldb.")
	flags.DurationVar(&srv.Config.Controller.Config.SnappingTurtleTimeout, "controller.config.snapping-turtle-timeout", srv.Config.Controller.Config.SnappingTurtleTimeout, "Period for running automatic snapshotting routine.")

	// Controller.SQLDB
	flags.StringVar(&srv.Config.Controller.Config.SQLDB.Database, "controller.config.sqldb.database", srv.Config.Controller.Config.SQLDB.Database, "Database name.")
	flags.StringVar(&srv.Config.Controller.Config.SQLDB.Host, "controller.config.sqldb.host", srv.Config.Controller.Config.SQLDB.Host, "Hostname of SQL Database")
	flags.StringVar(&srv.Config.Controller.Config.SQLDB.Port, "controller.config.sqldb.port", srv.Config.Controller.Config.SQLDB.Port, "Port of SQL Database")
	flags.StringVar(&srv.Config.Controller.Config.SQLDB.User, "controller.config.sqldb.user", srv.Config.Controller.Config.SQLDB.User, "Username for connection to SQL Database")
	flags.StringVar(&srv.Config.Controller.Config.SQLDB.Password, "controller.config.sqldb.password", srv.Config.Controller.Config.SQLDB.Password, "Password for connection to SQL Database")
	flags.StringVar(&srv.Config.Controller.Config.SQLDB.URL, "controller.config.sqldb.url", srv.Config.Controller.Config.SQLDB.URL, "URL for connection to SQL Database (supersedes host/port/username etc)")
	flags.IntVar(&srv.Config.Controller.Config.SQLDB.Pool, "controller.config.sqldb.pool", srv.Config.Controller.Config.SQLDB.Pool, "Max number of open connections to database. 0=unlimited")
	flags.IntVar(&srv.Config.Controller.Config.SQLDB.IdlePool, "controller.config.sqldb.idle-pool", srv.Config.Controller.Config.SQLDB.IdlePool, "Maximum number of idle connections to database.")
	flags.DurationVar(&srv.Config.Controller.Config.SQLDB.ConnMaxLifetime, "controller.config.sqldb.conn-max-lifetime", srv.Config.Controller.Config.SQLDB.ConnMaxLifetime, "See https://golang.org/pkg/database/sql/#DB.SetConnMaxLifetime")
	flags.DurationVar(&srv.Config.Controller.Config.SQLDB.ConnMaxIdleTime, "controller.config.sqldb.conn-max-idle-time", srv.Config.Controller.Config.SQLDB.ConnMaxIdleTime, "See https://golang.org/pkg/database/sql/#DB.SetConnMaxIdletime")

	// Queryer
	flags.BoolVar(&srv.Config.Queryer.Run, "queryer.run", srv.Config.Queryer.Run, "Run the Queryer service in process.")
	flags.StringVar(&srv.Config.Queryer.Config.ControllerAddress, "queryer.config.controller-address", srv.Config.Queryer.Config.ControllerAddress, "Address of remote Controller process.")

	// Computer
	flags.BoolVar(&srv.Config.Computer.Run, "computer.run", srv.Config.Computer.Run, "Run the Computer service in process.")
	flags.IntVar(&srv.Config.Computer.N, "computer.n", srv.Config.Computer.N, "The number of Computer services to run in process.")
	flags.AddFlagSet(serverFlagSet(&srv.Config.Computer.Config, "computer.config"))
}
