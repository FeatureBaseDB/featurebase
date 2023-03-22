// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newBoltCommand(logdest logger.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bolt",
		Short: "Inspect bolt data files.",
		Long: `
Provides a set of commands for inspecting bolt data files.
`,
	}
	cmd.AddCommand(newBoltKeysCommand(logdest))
	return cmd
}

func newBoltKeysCommand(logdest logger.Logger) *cobra.Command {
	c := ctl.NewBoltKeysCommand(logdest)
	cmd := &cobra.Command{
		Use:   "keys [flags] PATH",
		Short: "Get keys from bolt data file.",
		Long: `
"Get keys from bolt data file."
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("data directory path required")
			} else if len(args) > 1 {
				return fmt.Errorf("too many command line arguments")
			}
			c.Path = args[0]
			return nil
		},
		RunE: UsageErrorWrapper(c),
	}

	flags := cmd.Flags()
	flags.BoolVar(&c.Hexa, "hexa", false, "Print hexadecimal rather than plain text")

	return cmd
}
