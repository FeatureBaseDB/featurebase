package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Exporter *ctl.ExportCommand

func NewExportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Exporter = ctl.NewExportCommand(os.Stdin, os.Stdout, os.Stderr)
	exportCmd := &cobra.Command{
		Use:   "export",
		Short: "Export data from pilosa.",
		Long: `
Bulk exports a fragment to a CSV file. If the OUTFILE is not specified then
the output is written to STDOUT.

The format of the CSV file is:

	ROWID,COLUMNID

The file does not contain any headers.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Exporter.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	flags := exportCmd.Flags()

	flags.StringVarP(&Exporter.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&Exporter.Database, "database", "d", "", "Pilosa database to export into.")
	flags.StringVarP(&Exporter.Frame, "frame", "f", "", "Frame to export into.")
	flags.StringVarP(&Exporter.Path, "output-file", "o", "", "File to write export to - default stdout")

	return exportCmd
}

func init() {
	subcommandFns["export"] = NewExportCommand
}
