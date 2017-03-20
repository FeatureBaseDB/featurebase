package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Backuper *ctl.BackupCommand

func NewBackupCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Backuper = ctl.NewBackupCommand(os.Stdin, os.Stdout, os.Stderr)
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup data from pilosa.",
		Long: `
Backs up the view from across the cluster into a single file.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Backuper.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	flags := backupCmd.Flags()
	flags.StringVarP(&Backuper.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&Backuper.Database, "database", "d", "", "Pilosa database to backup into.")
	flags.StringVarP(&Backuper.Frame, "frame", "f", "", "Frame to backup into.")
	flags.StringVarP(&Backuper.View, "view", "v", "", "View to backup into.")
	flags.StringVarP(&Backuper.Path, "output-file", "o", "", "File to write backup to - default stdout")

	return backupCmd
}

func init() {
	subcommandFns["backup"] = NewBackupCmd
}
