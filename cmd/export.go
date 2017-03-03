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

var exporter = ctl.NewExportCommand(os.Stdin, os.Stdout, os.Stderr)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "export - export data from pilosa",
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

func init() {
	exportCmd.Flags().StringVarP(&exporter.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	exportCmd.Flags().StringVarP(&exporter.Database, "database", "d", "", "Pilosa database to export into.")
	exportCmd.Flags().StringVarP(&exporter.Frame, "frame", "f", "", "Frame to export into.")
	exportCmd.Flags().StringVarP(&exporter.Path, "output-file", "o", "", "File to write export to - default stdout")

	err := viper.BindPFlags(exportCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding export flags: %v", err)
	}

	RootCmd.AddCommand(exportCmd)
}
