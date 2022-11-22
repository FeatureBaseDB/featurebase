// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v3/ctl"
	"github.com/molecula/featurebase/v3/logger"
)

var Exporter *ctl.ExportCommand

func newExportCommand(logdest logger.Logger) *cobra.Command {
	Exporter = ctl.NewExportCommand(logdest)
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
		RunE: usageErrorWrapper(Exporter),
	}
	flags := exportCmd.Flags()

	flags.StringVarP(&Exporter.Host, "host", "", "localhost:10101", "host:port of FeatureBase.")
	flags.StringVarP(&Exporter.Index, "index", "i", "", "FeatureBase index to export")
	flags.StringVarP(&Exporter.Field, "field", "f", "", "Field to export")
	flags.StringVarP(&Exporter.Path, "output-file", "o", "", "File to write export to - default stdout")
	ctl.SetTLSConfig(flags, "", &Exporter.TLS.CertificatePath, &Exporter.TLS.CertificateKeyPath, &Exporter.TLS.CACertPath, &Exporter.TLS.SkipVerify, &Exporter.TLS.EnableClientVerification)

	return exportCmd
}
