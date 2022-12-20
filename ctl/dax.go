package ctl

import (
	"github.com/molecula/featurebase/v3/dax/server"
	"github.com/spf13/cobra"
)

// BuildDAXFlags attaches a set of flags to the command for a server instance.
func BuildDAXFlags(cmd *cobra.Command, srv *server.Command) {
	flags := cmd.Flags()

	flags.StringVarP(&srv.Config.Bind, "bind", "b", srv.Config.Bind, "Default URI on which this service should listen.")
	flags.StringVar(&srv.Config.Advertise, "advertise", srv.Config.Advertise, "Address to advertise externally.")
	flags.BoolVar(&srv.Config.Verbose, "verbose", srv.Config.Verbose, "Enable verbose logging")
	flags.StringVar(&srv.Config.LogPath, "log-path", srv.Config.LogPath, "Log path")

	// MDS
	flags.BoolVar(&srv.Config.MDS.Run, "mds.run", srv.Config.MDS.Run, "Run the MDS service in process.")
	flags.DurationVar(&srv.Config.MDS.Config.RegistrationBatchTimeout, "mds.config.registration-batch-timeout", srv.Config.MDS.Config.RegistrationBatchTimeout, "Timeout for node registration batches.")
	flags.StringVar(&srv.Config.MDS.Config.DataDir, "mds.config.data-dir", srv.Config.MDS.Config.DataDir, "MDS directory to use in process.")
	flags.DurationVar(&srv.Config.MDS.Config.SnappingTurtleTimeout, "mds.config.snapping-turtle-timeout", srv.Config.MDS.Config.SnappingTurtleTimeout, "Period for running automatic snapshotting routine.")

	// WriteLogger
	flags.BoolVar(&srv.Config.WriteLogger.Run, "writelogger.run", srv.Config.WriteLogger.Run, "Run the WriteLogger service in process.")
	flags.StringVar(&srv.Config.WriteLogger.Config.DataDir, "writelogger.config.data-dir", srv.Config.WriteLogger.Config.DataDir, "WriteLogger directory to use in process.")

	// Snapshotter
	flags.BoolVar(&srv.Config.Snapshotter.Run, "snapshotter.run", srv.Config.Snapshotter.Run, "Run the Snapshotter service in process.")
	flags.StringVar(&srv.Config.Snapshotter.Config.DataDir, "snapshotter.config.data-dir", srv.Config.Snapshotter.Config.DataDir, "Snapshotter directory to use in process.")

	// Queryer
	flags.BoolVar(&srv.Config.Queryer.Run, "queryer.run", srv.Config.Queryer.Run, "Run the Queryer service in process.")
	flags.StringVar(&srv.Config.Queryer.Config.MDSAddress, "queryer.config.mds-address", srv.Config.Queryer.Config.MDSAddress, "Address of remote MDS process.")

	// Computer
	flags.BoolVar(&srv.Config.Computer.Run, "computer.run", srv.Config.Computer.Run, "Run the Computer service in process.")
	flags.IntVar(&srv.Config.Computer.N, "computer.n", srv.Config.Computer.N, "The number of Computer services to run in process.")
	flags.AddFlagSet(serverFlagSet(&srv.Config.Computer.Config, "computer.config"))
}
