// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newParquetInfoCommand(logdest logger.Logger) *cobra.Command {
	c := ctl.NewParquetInfoCommand(logdest)
	cmd := &cobra.Command{
		Use:   "parquet-info PATH|URL",
		Short: "Inspect Parquet Files.",
		Long: `
Displays schema and sample data from the specified file
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
	return cmd
}
