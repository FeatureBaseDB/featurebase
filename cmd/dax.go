// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"io"

	"github.com/molecula/featurebase/v3/ctl"
	"github.com/molecula/featurebase/v3/dax/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// newDAXCommand runs the FeatureBase CLI subcommand for ingesting bulk data.
func newDAXCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	server := server.NewCommand(stdin, stdout, stderr)
	daxCmd := &cobra.Command{
		Use:   "dax",
		Short: "Run a collection of DAX services",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := server.Start(); err != nil {
				return errors.Wrap(err, "running server")
			}
			return errors.Wrap(server.Wait(), "waiting on Server")
		},
	}

	// Attach flags to the command.
	ctl.BuildDAXFlags(daxCmd, server)

	return daxCmd
}
