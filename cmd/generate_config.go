// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"github.com/spf13/cobra"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
)

var generateConf *ctl.GenerateConfigCommand

func newGenerateConfigCommand(logdest logger.Logger) *cobra.Command {
	generateConf = ctl.NewGenerateConfigCommand(logdest)
	confCmd := &cobra.Command{
		Use:   "generate-config",
		Short: "Print the default configuration.",
		Long: `generate-config prints the default configuration to stdout
`,
		RunE: usageErrorWrapper(generateConf),
	}

	return confCmd
}
