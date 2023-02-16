// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"io"

	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cliCmd *cli.Command

// NewCLICommand runs the FeatureBase CLI subcommand.
func NewCLICommand(stderr io.Writer) *cobra.Command {
	logdest := logger.NewStandardLogger(stderr)
	cliCmd = cli.NewCommand(logdest)
	cobraCmd := &cobra.Command{
		Use:   "fbsql",
		Short: "Query FeatureBase with SQL from the command line",
		Long:  ``,
		RunE:  usageErrorWrapper(cliCmd),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			v := viper.New()
			return setAllConfig(v, cmd.Flags(), "FBSQL")
		},
		SilenceErrors: true,
	}

	// Attach flags to the command.
	ctl.BuildCLIFlags(cobraCmd, cliCmd)
	return cobraCmd
}
