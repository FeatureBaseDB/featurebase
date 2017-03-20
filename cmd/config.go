package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Conf *ctl.ConfigCommand

func NewConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Conf = ctl.NewConfigCommand(os.Stdin, os.Stdout, os.Stderr)
	confCmd := &cobra.Command{
		Use:   "config",
		Short: "Print the default configuration.",
		Long: `config prints the default configuration to stdout
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Conf.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}

	return confCmd
}

func init() {
	subcommandFns["config"] = NewConfigCommand
}
