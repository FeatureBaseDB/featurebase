// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v3/ctl"
	"github.com/molecula/featurebase/v3/server"
)

var conf *ctl.ConfigCommand

func newConfigCommand(stderr io.Writer) *cobra.Command {
	conf = ctl.NewConfigCommand(stderr)
	Server := server.NewCommand(stderr)
	confCmd := &cobra.Command{
		Use:   "config",
		Short: "Print the current configuration.",
		Long:  `config prints the current configuration to stdout`,

		RunE: func(cmd *cobra.Command, args []string) error {
			conf.Config = Server.Config
			return considerUsageError(cmd, conf.Run(context.Background()))
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(confCmd, Server)

	return confCmd
}
