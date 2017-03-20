package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Checker *ctl.CheckCommand

func NewCheckCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Checker = ctl.NewCheckCommand(os.Stdin, os.Stdout, os.Stderr)
	checkCmd := &cobra.Command{
		Use:   "check <path> [path2]...",
		Short: "Do a consistency check on a pilosa data file.",
		Long: `
Performs a consistency check on data files.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("path required")
			}
			Checker.Paths = args
			if err := Checker.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	return checkCmd
}

func init() {
	subcommandFns["check"] = NewCheckCommand
}
