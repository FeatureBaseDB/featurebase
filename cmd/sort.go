package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Sorter *ctl.SortCommand

func NewSortCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {

	Sorter = ctl.NewSortCommand(os.Stdin, os.Stdout, os.Stderr)

	sortCmd := &cobra.Command{
		Use:   "sort <path>",
		Short: "Sort import data for optimal import performance.",
		Long: `
Sorts the import data at PATH into the optimal sort order for importing.

The format of the CSV file is:

	ROWID,COLUMNID

The file should contain no headers.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("path required")
			} else if len(args) > 1 {
				return fmt.Errorf("only one path supported")
			}
			Sorter.Path = args[0]
			if err := Sorter.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	return sortCmd
}

func init() {
	subcommandFns["sort"] = NewSortCommand
}
