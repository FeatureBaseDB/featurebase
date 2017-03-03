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

var importer = ctl.NewImportCommand(os.Stdin, os.Stdout, os.Stderr)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "import - import data to pilosa",
	Long: `Bulk imports one or more CSV files to a host's database and frame. The bits
of the CSV file are grouped by slice for the most efficient import.

The format of the CSV file is:

	BITMAPID,PROFILEID,[TIME]

The file should contain no headers. The TIME column is optional and can be
omitted. If it is present then its format should be YYYY-MM-DDTHH:MM.
`,
	Run: func(cmd *cobra.Command, args []string) {
		importer.Paths = args
		if err := importer.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	importCmd.Flags().StringVarP(&importer.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	importCmd.Flags().StringVarP(&importer.Database, "database", "d", "", "Pilosa database to import into.")
	importCmd.Flags().StringVarP(&importer.Frame, "frame", "f", "", "Frame to import into.")
	importCmd.Flags().IntVarP(&importer.BufferSize, "buffer-size", "s", 10000000, "Number of bits to buffer/sort before importing.")

	err := viper.BindPFlags(importCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding import flags: %v", err)
	}

	RootCmd.AddCommand(importCmd)
}
