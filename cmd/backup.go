package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

func NewBackupCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	backuper := ctl.NewBackupCommand(os.Stdin, os.Stdout, os.Stderr)
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup data from pilosa.",
		Long: `
Backs up the database and frame from across the cluster into a single file.
`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := backuper.Run(context.Background()); err != nil {
				fmt.Println(err)
			}
		},
	}
	flags := backupCmd.Flags()
	flags.StringVarP(&backuper.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	flags.StringVarP(&backuper.Database, "database", "d", "", "Pilosa database to backup into.")
	flags.StringVarP(&backuper.Frame, "frame", "f", "", "Frame to backup into.")
	flags.StringVarP(&backuper.Path, "output-file", "o", "", "File to write backup to - default stdout")

	return backupCmd
}

func init() {
	subcommandFns["backup"] = NewBackupCmd
}
