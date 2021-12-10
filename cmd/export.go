package cmd

import (
	"context"
	"io"

	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v2/ctl"
)

var Exporter *ctl.ExportCommand

func newExportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Exporter = ctl.NewExportCommand(stdin, stdout, stderr)
	exportCmd := &cobra.Command{
		Use:   "export",
		Short: "Export data from FeatureBase.",
		Long: `
Bulk exports a fragment to a CSV file. If the OUTFILE is not specified then
the output is written to STDOUT.

The format of the CSV file is:

	ROWID,COLUMNID

The file does not contain any headers.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Exporter.Run(context.Background())
		},
	}
	flags := exportCmd.Flags()

	flags.StringVarP(&Exporter.Host, "host", "", "localhost:10101", "host:port of FeatureBase.")
	flags.StringVarP(&Exporter.Index, "index", "i", "", "FeatureBase index to export")
	flags.StringVarP(&Exporter.Field, "field", "f", "", "Field to export")
	flags.StringVarP(&Exporter.Path, "output-file", "o", "", "File to write export to - default stdout")
	ctl.SetTLSConfig(flags, "", &Exporter.TLS.CertificatePath, &Exporter.TLS.CertificateKeyPath, &Exporter.TLS.CACertPath, &Exporter.TLS.SkipVerify, &Exporter.TLS.EnableClientVerification)

	return exportCmd
}
