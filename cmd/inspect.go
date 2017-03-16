package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Inspector *ctl.InspectCommand

func NewInspectCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Inspector = ctl.NewInspectCommand(os.Stdin, os.Stdout, os.Stderr)

	inspectCmd := &cobra.Command{
		Use:   "inspect",
		Short: "Get stats on a pilosa data file.",
		Long: `
Inspects a data file and provides stats.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("path required")
			} else if len(args) > 1 {
				return fmt.Errorf("only one path allowed")
			}
			Inspector.Path = args[0]
			if err := Inspector.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	return inspectCmd
}

func init() {
	subcommandFns["inspect"] = NewInspectCommand
}
