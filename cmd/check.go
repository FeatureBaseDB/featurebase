package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var checker = ctl.NewCheckCommand(os.Stdin, os.Stdout, os.Stderr)

var checkCmd = &cobra.Command{
	Use:   "check <path> [path2]...",
	Short: "check - check a pilosa data file",
	Long: `
Performs a consistency check on data files.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			fmt.Println("path required")
			return
		}
		checker.Paths = args
		if err := checker.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(checkCmd)
}
