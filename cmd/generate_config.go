// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v2/ctl"
)

var generateConf *ctl.GenerateConfigCommand

func newGenerateConfigCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	generateConf = ctl.NewGenerateConfigCommand(stdin, stdout, stderr)
	confCmd := &cobra.Command{
		Use:   "generate-config",
		Short: "Print the default configuration.",
		Long: `generate-config prints the default configuration to stdout
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return generateConf.Run(context.Background())
		},
	}

	return confCmd
}
