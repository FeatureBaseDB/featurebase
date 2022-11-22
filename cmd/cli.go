// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/molecula/featurebase/v3/ctl"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

var cli *ctl.CLICommand

// newCLICommand runs the FeatureBase CLI subcommand for ingesting bulk data.
func newCLICommand(logdest logger.Logger) *cobra.Command {
	cli = ctl.NewCLICommand(logdest)
	cliCmd := &cobra.Command{
		Use:   "cli",
		Short: "Query FB with SQL3 from the command line",
		Long:  ``,
		RunE:  usageErrorWrapper(cli),
	}

	flags := cliCmd.Flags()
	flags.StringVarP(&cli.Host, "host", "", cli.Host, "hostname of FeatureBase.")
	flags.StringVarP(&cli.Port, "port", "", cli.Port, "port of FeatureBase.")
	flags.StringVar(&cli.HistoryPath, "history-path", cli.HistoryPath, "path for history files.")
	flags.StringVar(&cli.OrganizationID, "org-id", cli.OrganizationID, "OrganizationID.")
	flags.StringVar(&cli.DatabaseID, "db-id", cli.DatabaseID, "DatabaseID.")

	flags.StringVar(&cli.ClientID, "client-id", cli.ClientID, "Cognito Client ID for FeatureBase Cloud access.")
	flags.StringVar(&cli.Region, "region", cli.Region, "Cloud region for FeatureBase Cloud access (e.g. us-east-2).")
	flags.StringVar(&cli.Email, "email", cli.Email, "Email address for FeatureBase Cloud access.")
	flags.StringVar(&cli.Password, "password", cli.Password, "Password for FeatureBase Cloud access.")

	return cliCmd
}
