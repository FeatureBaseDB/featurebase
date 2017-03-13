package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	restorer := ctl.NewRestoreCommand(os.Stdin, os.Stdout, os.Stderr)

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore data to pilosa from a backup file.",
		Long: `
Restores a frame to the cluster from a backup file.
`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := restorer.Run(context.Background()); err != nil {
				fmt.Println(err)
			}
		},
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&restorer.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	flags.StringVarP(&restorer.Database, "database", "d", "", "Pilosa database to restore into.")
	flags.StringVarP(&restorer.Frame, "frame", "f", "", "Frame to restore into.")
	flags.StringVarP(&restorer.Path, "input-file", "i", "", "File to restore from.")

	return restoreCmd
}

func init() {
	subcommandFns["restore"] = NewRestoreCommand
}
