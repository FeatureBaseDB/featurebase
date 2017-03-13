package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

func NewInspectCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	inspecter := ctl.NewInspectCommand(os.Stdin, os.Stdout, os.Stderr)

	inspectCmd := &cobra.Command{
		Use:   "inspect",
		Short: "Get stats on a pilosa data file.",
		Long: `
Inspects a data file and provides stats.
`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Println("path required")
				return
			} else if len(args) > 1 {
				fmt.Println("only one path allowed")
				return
			}
			inspecter.Path = args[0]
			if err := inspecter.Run(context.Background()); err != nil {
				fmt.Println(err)
			}
		},
	}
	return inspectCmd
}

func init() {
	subcommandFns["inspect"] = NewInspectCommand
}
