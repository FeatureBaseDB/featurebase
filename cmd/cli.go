// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/spf13/cobra"
)

var cli *ctl.CLICommand

// newCLICommand runs the FeatureBase CLI subcommand for ingesting bulk data.
func newCLICommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	cli = ctl.NewCLICommand(stdin, stdout, stderr)
	cliCmd := &cobra.Command{
		Use:   "cli",
		Short: "Query FB with SQL3 from the command line",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cli.Run(context.Background())
		},
	}

	flags := cliCmd.Flags()
	flags.StringVarP(&cli.Host, "host", "", cli.Host, "hostname of FeatureBase.")
	flags.StringVarP(&cli.Port, "port", "", cli.Port, "port of FeatureBase.")

	return cliCmd
}
