package ctl

import (
	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/spf13/cobra"
)

// BuildCLIFlags attaches a set of flags to the command for a cli instance.
func BuildCLIFlags(cmd *cobra.Command, cliCmd *cli.Command) {
	flags := cmd.Flags()

	flags.StringVarP(&cliCmd.Host, "host", "", cliCmd.Host, "hostname of FeatureBase.")
	flags.StringVarP(&cliCmd.Port, "port", "", cliCmd.Port, "port of FeatureBase.")
	flags.StringVar(&cliCmd.HistoryPath, "history-path", cliCmd.HistoryPath, "path for history files.")
	flags.StringVar(&cliCmd.OrganizationID, "org-id", cliCmd.OrganizationID, "OrganizationID.")
	flags.StringVar(&cliCmd.Database, "db", cliCmd.Database, "Name of the database to connect to.")

	flags.StringVar(&cliCmd.ClientID, "client-id", cliCmd.ClientID, "Cognito Client ID for FeatureBase Cloud access.")
	flags.StringVar(&cliCmd.Region, "region", cliCmd.Region, "Cloud region for FeatureBase Cloud access (e.g. us-east-2).")
	flags.StringVar(&cliCmd.Email, "email", cliCmd.Email, "Email address for FeatureBase Cloud access.")
	flags.StringVar(&cliCmd.Password, "password", cliCmd.Password, "Password for FeatureBase Cloud access.")

	flags.StringVarP(&cliCmd.Command, "command", "c", cliCmd.Command, "Command to run.")

	flags.String("config", "", "Configuration file to read from.")

	flags.AddFlagSet(flags)
}
