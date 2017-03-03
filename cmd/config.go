package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var conf = ctl.NewConfigCommand(os.Stdin, os.Stdout, os.Stderr)

var confCmd = &cobra.Command{
	Use:   "config",
	Short: "config - prints the default configuration",
	Long: `config prints the default configuration to stdout
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := conf.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(confCmd)
}
