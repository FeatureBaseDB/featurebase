// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"io"
	"os"

	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/featurebasedb/featurebase/v3/cmd"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	command := newCLICommand(os.Stderr)
	command.Execute()
}

// newCLICommand runs the FeatureBase CLI subcommand.
func newCLICommand(stderr io.Writer) *cobra.Command {
	logdest := logger.NewStandardLogger(stderr)
	cliCmd := cli.NewCommand(logdest)
	cobraCmd := &cobra.Command{
		Use:   "fbsql",
		Short: "Query FeatureBase with SQL from the command line",
		Long:  ``,
		RunE:  cmd.UsageErrorWrapper(cliCmd),
		PersistentPreRunE: func(cobraCmd *cobra.Command, args []string) error {
			v := viper.New()
			return cmd.SetAllConfig(v, cobraCmd.Flags(), "FBSQL")
		},
		SilenceErrors: true,
	}

	// Attach flags to the command.
	buildFlags(cobraCmd, cliCmd)
	return cobraCmd
}

// buildFlags attaches a set of flags to the command for a cli instance.
func buildFlags(cmd *cobra.Command, cliCmd *cli.Command) {
	flags := cmd.Flags()

	// Base struct flags.
	flags.StringSliceVarP(&cliCmd.Commands, "command", "c", cliCmd.Commands, "Command to run in non-interactive mode. Provide multiple flags to execute more than one command. All `--command` flags run before all `--file` flags.")
	flags.StringSliceVarP(&cliCmd.Files, "file", "f", cliCmd.Files, "File to run in non-interactive mode. Provide multiple flags to execute more than one file. All `--command` flags run before all `--file` flags.")

	// Config flags.
	flags.StringVarP(&cliCmd.Config.Host, "host", "", cliCmd.Config.Host, "hostname of FeatureBase.")
	flags.StringVarP(&cliCmd.Config.Port, "port", "p", cliCmd.Config.Port, "port of FeatureBase.")
	flags.StringVar(&cliCmd.Config.HistoryPath, "history-path", cliCmd.Config.HistoryPath, "path for history files.")
	flags.StringVar(&cliCmd.Config.OrganizationID, "org-id", cliCmd.Config.OrganizationID, "OrganizationID.")
	flags.StringVarP(&cliCmd.Config.Database, "dbname", "d", cliCmd.Config.Database, "Name of the database to connect to.")

	flags.StringVar(&cliCmd.Config.CloudAuth.ClientID, "client-id", cliCmd.Config.CloudAuth.ClientID, "Cognito Client ID for FeatureBase Cloud access.")
	flags.StringVar(&cliCmd.Config.CloudAuth.Region, "region", cliCmd.Config.CloudAuth.Region, "Cloud region for FeatureBase Cloud access (e.g. us-east-2).")
	flags.StringVar(&cliCmd.Config.CloudAuth.Email, "email", cliCmd.Config.CloudAuth.Email, "Email address for FeatureBase Cloud access.")
	flags.StringVar(&cliCmd.Config.CloudAuth.Password, "password", cliCmd.Config.CloudAuth.Password, "Password for FeatureBase Cloud access.")

	flags.StringVar(&cliCmd.Config.KafkaConfig, "kafka-config", cliCmd.Config.KafkaConfig, "Kafka configuration file to read from.")

	flags.String("config", "", "Configuration file to read from.")
}
