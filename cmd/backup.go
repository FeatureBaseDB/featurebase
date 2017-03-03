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

var backuper = ctl.NewBackupCommand(os.Stdin, os.Stdout, os.Stderr)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "backup - backup data from pilosa",
	Long: `
Backs up the database and frame from across the cluster into a single file.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := backuper.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	backupCmd.Flags().StringVarP(&backuper.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	backupCmd.Flags().StringVarP(&backuper.Database, "database", "d", "", "Pilosa database to backup into.")
	backupCmd.Flags().StringVarP(&backuper.Frame, "frame", "f", "", "Frame to backup into.")
	backupCmd.Flags().StringVarP(&backuper.Path, "output-file", "o", "", "File to write backup to - default stdout")

	err := viper.BindPFlags(backupCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding backup flags: %v", err)
	}

	RootCmd.AddCommand(backupCmd)
}
