// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

var cliCmd *cli.Command

// newCLICommand runs the FeatureBase CLI subcommand.
func newCLICommand(logdest logger.Logger) *cobra.Command {
	cliCmd = cli.NewCommand(logdest)
	cobraCmd := &cobra.Command{
		Use:   "cli",
		Short: "Query FeatureBase with SQL from the command line",
		Long:  ``,
		RunE:  usageErrorWrapper(cliCmd),
	}

	// Attach flags to the command.
	ctl.BuildCLIFlags(cobraCmd, cliCmd)
	return cobraCmd
}
