// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	flags.StringVarP(&Exporter.Index, "index", "i", "", "Pilosa index to export")
	flags.StringVarP(&Exporter.Frame, "frame", "f", "", "Frame to export")
	flags.StringVarP(&Exporter.View, "view", "v", "standard", "View to export - default standard")
	flags.StringVarP(&Exporter.Path, "output-file", "o", "", "File to write export to - default stdout")
	ctl.SetTLSConfig(flags, &Exporter.TLS.CertificatePath, &Exporter.TLS.CertificateKeyPath, &Exporter.TLS.SkipVerify)

	return exportCmd
}

func init() {
	subcommandFns["export"] = NewExportCommand
}
