// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newKeygenCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewKeygenCommand(logdest)
	ccmd := &cobra.Command{
		Use:   "keygen",
		Short: "Generate secret key for authentication.",
		Long: `
Generate secret key to configure FeatureBase for Authentication.
`,
		RunE: UsageErrorWrapper(cmd),
	}

	flags := ccmd.Flags()
	flags.IntVarP(&cmd.KeyLength, "length", "l", 32, "length of the key to produce")
	return ccmd
}
