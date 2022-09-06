// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"context"
	"io"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/spf13/cobra"
)

func newKeygenCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewKeygenCommand(stdin, stdout, stderr)
	ccmd := &cobra.Command{
		Use:   "keygen",
		Short: "Generate secret key for authentication.",
		Long: `
Generate secret key to configure FeatureBase for Authentication.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}

	flags := ccmd.Flags()
	flags.IntVarP(&cmd.KeyLength, "length", "l", 32, "length of the key to produce")
	return ccmd
}
