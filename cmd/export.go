package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

func NewExportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	exporter := ctl.NewExportCommand(os.Stdin, os.Stdout, os.Stderr)
	exportCmd := &cobra.Command{
		Use:   "export",
		Short: "Export data from pilosa.",
		Long: `
Bulk exports a fragment to a CSV file. If the OUTFILE is not specified then
the output is written to STDOUT.

The format of the CSV file is:

	BITMAPID,PROFILEID

The file does not contain any headers.
`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := exporter.Run(context.Background()); err != nil {
				fmt.Println(err)
			}
		},
	}
	flags := exportCmd.Flags()

	flags.StringVarP(&exporter.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	flags.StringVarP(&exporter.Database, "database", "d", "", "Pilosa database to export into.")
	flags.StringVarP(&exporter.Frame, "frame", "f", "", "Frame to export into.")
	flags.StringVarP(&exporter.Path, "output-file", "o", "", "File to write export to - default stdout")

	return exportCmd
}

func init() {
	subcommandFns["export"] = NewExportCommand
}
