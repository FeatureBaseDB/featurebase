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

var restorer = ctl.NewRestoreCommand(os.Stdin, os.Stdout, os.Stderr)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "restore - restore data to pilosa",
	Long: `
Restores a frame to the cluster from a backup file.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := restorer.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	restoreCmd.Flags().StringVarP(&restorer.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	restoreCmd.Flags().StringVarP(&restorer.Database, "database", "d", "", "Pilosa database to restore into.")
	restoreCmd.Flags().StringVarP(&restorer.Frame, "frame", "f", "", "Frame to restore into.")
	restoreCmd.Flags().StringVarP(&restorer.Path, "input-file", "i", "", "File to write restore from")

	err := viper.BindPFlags(restoreCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding restore flags: %v", err)
	}

	RootCmd.AddCommand(restoreCmd)
}
