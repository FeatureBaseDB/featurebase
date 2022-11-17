// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v3/ctl"
)

var generateConf *ctl.GenerateConfigCommand

func newGenerateConfigCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	generateConf = ctl.NewGenerateConfigCommand(stdin, stdout, stderr)
	confCmd := &cobra.Command{
		Use:   "generate-config",
		Short: "Print the default configuration.",
		Long: `generate-config prints the default configuration to stdout
`,
		RunE: usageErrorWrapper(generateConf),
	}

	return confCmd
}
