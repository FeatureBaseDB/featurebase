package ctl

import (
	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// BuildCLIFlags attaches a set of flags to the command for a cli instance.
func BuildCLIFlags(cmd *cobra.Command, cliCmd *cli.Command) {
	flags := cmd.Flags()

	// Base struct flags.
	flags.StringSliceVarP(&cliCmd.Commands, "command", "c", cliCmd.Commands, "Command to run in non-interactive mode. Provide multiple flags to execute more than one command. All `--command` flags run before all `--file` flags.")
	flags.StringSliceVarP(&cliCmd.Files, "file", "f", cliCmd.Files, "File to run in non-interactive mode. Provide multiple flags to execute more than one file. All `--command` flags run before all `--file` flags.")

	// Config flags.
	flags.AddFlagSet(cliConfigFlagSet(cliCmd.Config))
}

// cliConfigFlagSet returns a pflag.FlagSet for the CLI Config struct.
func cliConfigFlagSet(cfg *cli.Config) *pflag.FlagSet {
	flags := pflag.NewFlagSet("cli", pflag.ExitOnError)

	flags.StringVarP(&cfg.Host, "host", "", cfg.Host, "hostname of FeatureBase.")
	flags.StringVarP(&cfg.Port, "port", "", cfg.Port, "port of FeatureBase.")
	flags.StringVar(&cfg.HistoryPath, "history-path", cfg.HistoryPath, "path for history files.")
	flags.StringVar(&cfg.OrganizationID, "org-id", cfg.OrganizationID, "OrganizationID.")
	flags.StringVar(&cfg.Database, "db", cfg.Database, "Name of the database to connect to.")

	flags.StringVar(&cfg.CloudAuth.ClientID, "client-id", cfg.CloudAuth.ClientID, "Cognito Client ID for FeatureBase Cloud access.")
	flags.StringVar(&cfg.CloudAuth.Region, "region", cfg.CloudAuth.Region, "Cloud region for FeatureBase Cloud access (e.g. us-east-2).")
	flags.StringVar(&cfg.CloudAuth.Email, "email", cfg.CloudAuth.Email, "Email address for FeatureBase Cloud access.")
	flags.StringVar(&cfg.CloudAuth.Password, "password", cfg.CloudAuth.Password, "Password for FeatureBase Cloud access.")

	flags.String("config", "", "Configuration file to read from.")

	return flags
}
