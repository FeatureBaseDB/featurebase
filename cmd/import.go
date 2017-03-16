package cmd

import (
	"context"
	"io"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Importer *ctl.ImportCommand

func NewImportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Importer = ctl.NewImportCommand(stdin, stdout, stderr)
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Bulk load data into pilosa.",
		Long: `Bulk imports one or more CSV files to a host's database and frame. The bits
of the CSV file are grouped by slice for the most efficient import.

The format of the CSV file is:

	BITMAPID,PROFILEID,[TIME]

The file should contain no headers. The TIME column is optional and can be
omitted. If it is present then its format should be YYYY-MM-DDTHH:MM.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			Importer.Paths = args
			if err := Importer.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	flags := importCmd.Flags()
	flags.StringVarP(&Importer.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	flags.StringVarP(&Importer.Database, "database", "d", "", "Pilosa database to import into.")
	flags.StringVarP(&Importer.Frame, "frame", "f", "", "Frame to import into.")
	flags.IntVarP(&Importer.BufferSize, "buffer-size", "s", 10000000, "Number of bits to buffer/sort before importing.")

	return importCmd
}

func init() {
	subcommandFns["import"] = NewImportCommand
}
