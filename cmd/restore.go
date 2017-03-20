package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Restorer *ctl.RestoreCommand

func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Restorer = ctl.NewRestoreCommand(os.Stdin, os.Stdout, os.Stderr)

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore data to pilosa from a backup file.",
		Long: `
Restores a frame to the cluster from a backup file.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Restorer.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&Restorer.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&Restorer.Database, "database", "d", "", "Pilosa database to restore into.")
	flags.StringVarP(&Restorer.Frame, "frame", "f", "", "Frame to restore into.")
	flags.StringVarP(&Restorer.Path, "input-file", "i", "", "File to restore from.")

	return restoreCmd
}

func init() {
	subcommandFns["restore"] = NewRestoreCommand
}
