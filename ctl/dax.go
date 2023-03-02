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

	// Queryer
	flags.BoolVar(&srv.Config.Queryer.Run, "queryer.run", srv.Config.Queryer.Run, "Run the Queryer service in process.")
	flags.StringVar(&srv.Config.Queryer.Config.ControllerAddress, "queryer.config.controller-address", srv.Config.Queryer.Config.ControllerAddress, "Address of remote Controller process.")

	// Computer
	flags.BoolVar(&srv.Config.Computer.Run, "computer.run", srv.Config.Computer.Run, "Run the Computer service in process.")
	flags.IntVar(&srv.Config.Computer.N, "computer.n", srv.Config.Computer.N, "The number of Computer services to run in process.")
	flags.AddFlagSet(serverFlagSet(&srv.Config.Computer.Config, "computer.config"))
}
