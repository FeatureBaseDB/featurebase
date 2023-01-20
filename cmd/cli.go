// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

var cliCmd *cli.CLICommand

// newCLICommand runs the FeatureBase CLI subcommand for ingesting bulk data.
func newCLICommand(logdest logger.Logger) *cobra.Command {
	cliCmd = cli.NewCLICommand(logdest)
	cobraCmd := &cobra.Command{
		Use:   "cli",
		Short: "Query FB with SQL3 from the command line",
		Long:  ``,
		RunE:  usageErrorWrapper(cliCmd),
	}

	flags := cobraCmd.Flags()
	flags.StringVarP(&cliCmd.Host, "host", "", cliCmd.Host, "hostname of FeatureBase.")
	flags.StringVarP(&cliCmd.Port, "port", "", cliCmd.Port, "port of FeatureBase.")
	flags.StringVar(&cliCmd.HistoryPath, "history-path", cliCmd.HistoryPath, "path for history files.")
	flags.StringVar(&cliCmd.OrganizationID, "org-id", cliCmd.OrganizationID, "OrganizationID.")
	flags.StringVar(&cliCmd.DatabaseID, "db-id", cliCmd.DatabaseID, "DatabaseID.")

	flags.StringVar(&cliCmd.ClientID, "client-id", cliCmd.ClientID, "Cognito Client ID for FeatureBase Cloud access.")
	flags.StringVar(&cliCmd.Region, "region", cliCmd.Region, "Cloud region for FeatureBase Cloud access (e.g. us-east-2).")
	flags.StringVar(&cliCmd.Email, "email", cliCmd.Email, "Email address for FeatureBase Cloud access.")
	flags.StringVar(&cliCmd.Password, "password", cliCmd.Password, "Password for FeatureBase Cloud access.")

	return cobraCmd
}
