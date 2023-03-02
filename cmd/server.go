// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"io"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Server is global so that tests can control and verify it.
var Server *server.Command
var holder *server.Command

// newHolderCmd creates a FeatureBase server for just long enough to open the
// holder, then shuts it down again.
func newHolderCmd(stderr io.Writer) *cobra.Command {
	holder = server.NewCommand(stderr)
	serveCmd := &cobra.Command{
		Use:   "holder",
		Short: "Load FeatureBase.",
		Long: `featurebase holder starts (and immediately stops) FeatureBase.

It opens the data directory and loads it, then shuts down immediately.
This is only useful for diagnostic use.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Start & run the server.
			if err := holder.UpAndDown(); err != nil {
				return errors.Wrap(err, "running server")
			}
			return nil
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(serveCmd, holder)
	return serveCmd
}

// newServeCmd creates a FeatureBase server and runs it with command line flags.
func newServeCmd(stderr io.Writer) *cobra.Command {
	Server = server.NewCommand(stderr)
	serveCmd := &cobra.Command{
		Use:   "server",
		Short: "Run FeatureBase.",
		Long: `featurebase server runs FeatureBase.
It will load existing data from the configured
directory and start listening for client connections
on the configured port.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Start & run the server.
			if err := Server.Start(); err != nil {
				return considerUsageError(cmd, errors.Wrap(err, "running server"))
			}

			return errors.Wrap(Server.Wait(), "waiting on Server")
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(serveCmd, Server)
	return serveCmd
}
