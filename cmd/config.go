// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"context"
	"io"

	"github.com/spf13/cobra"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/server"
)

var conf *ctl.ConfigCommand

func newConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	conf = ctl.NewConfigCommand(stdin, stdout, stderr)
	Server := server.NewCommand(stdin, stdout, stderr)
	confCmd := &cobra.Command{
		Use:   "config",
		Short: "Print the current configuration.",
		Long:  `config prints the current configuration to stdout`,

		RunE: func(cmd *cobra.Command, args []string) error {
			conf.Config = Server.Config
			return conf.Run(context.Background())
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(confCmd, Server)

	return confCmd
}
