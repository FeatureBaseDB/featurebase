package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var sorter = ctl.NewSortCommand(os.Stdin, os.Stdout, os.Stderr)

var sortCmd = &cobra.Command{
	Use:   "sort <path>",
	Short: "Sort import data for optimal import performance.",
	Long: `
Sorts the import data at PATH into the optimal sort order for importing.

The format of the CSV file is:

	BITMAPID,PROFILEID

The file should contain no headers.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			fmt.Println("path required")
			return
		} else if len(args) > 1 {
			fmt.Println("only one path supported")
			return
		}
		sorter.Path = args[0]
		if err := sorter.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(sortCmd)
}
