package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/pilosa/pilosa/ctl"
)

var inspecter = ctl.NewInspectCommand(os.Stdin, os.Stdout, os.Stderr)

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "inspect - inspect a pilosa data file",
	Long: `
Inspects a data file and provides stats.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := inspecter.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	inspectCmd.Flags().StringVarP(&inspecter.Path, "file", "i", "", "File to inspect")

	err := viper.BindPFlags(inspectCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding inspect flags: %v", err)
	}

	RootCmd.AddCommand(inspectCmd)
}
