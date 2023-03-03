// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

// newImportCommand runs the FeatureBase import subcommand for ingesting bulk data.
func newDataframeCsvLoaderCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewDataframeCsvLoaderCommand(logdest)
	loaderCmd := &cobra.Command{
		Use:   "dataframe-csv-loader",
		Short: "load dataframe integer and floating point values into featurebase",
		Long: `
`,
		RunE: usageErrorWrapper(cmd),
	}
	flags := loaderCmd.Flags()
	flags.StringVar(&cmd.Path, "csv", "", "path to csv input file")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	flags.StringVar(&cmd.Pprof, "pprof", cmd.Pprof, "host:port to listen for profiling requests at /debug/pprof and /debug/fgprof.")
	flags.StringVar(&cmd.AuthToken, "auth-token", "", "Authentication token")
	flags.StringVar(&cmd.Index, "index", "", "Destination Index. ")
	flags.IntVar(&cmd.MaxCapacity, "buffer", 0, "Maximum size of of the line buffer defaults to go bufio default ")
	flags.IntVar(&cmd.BatchSize, "batch-size", 1048576, "Maximum number of records to send in a single batch ")
	ctl.SetTLSConfig(
		flags, "",
		&cmd.TLS.CertificatePath,
		&cmd.TLS.CertificateKeyPath,
		&cmd.TLS.CACertPath,
		&cmd.TLS.SkipVerify,
		&cmd.TLS.EnableClientVerification,
	)

	return loaderCmd
}
