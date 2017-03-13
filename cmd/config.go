package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

func NewConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	conf := ctl.NewConfigCommand(os.Stdin, os.Stdout, os.Stderr)
	confCmd := &cobra.Command{
		Use:   "config",
		Short: "Print the default configuration.",
		Long: `config prints the default configuration to stdout
`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := conf.Run(context.Background()); err != nil {
				fmt.Println(err)
			}
		},
	}

	return confCmd
}

func init() {
	subcommandFns["config"] = NewConfigCommand
}
