// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v2/ctl"
	"github.com/spf13/cobra"
)

func newKeygenCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewKeygenCommand(stdin, stdout, stderr)
	ccmd := &cobra.Command{
		Use:   "keygen",
		Short: "Generate keys for authentication.",
		Long: `
Generate hash and block keys to configure FeatureBase for Authentication.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}

	flags := ccmd.Flags()
	flags.IntVarP(&cmd.KeyLength, "length", "l", 32, "length of keys to produce")
	return ccmd
}
