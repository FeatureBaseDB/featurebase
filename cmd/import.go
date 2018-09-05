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

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Importer *ctl.ImportCommand

// newImportCommand runs the Pilosa import subcommand for ingesting bulk data.
func newImportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Importer = ctl.NewImportCommand(stdin, stdout, stderr)
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Bulk load data into pilosa.",
		Long: `Bulk imports one or more CSV files to a host's index and field. The data
of the CSV file are grouped by shard for the most efficient import.

The format of the CSV file is:

	ROWID,COLUMNID,[TIME]

The file should contain no headers. The TIME column is optional and can be
omitted. If it is present then its format should be YYYY-MM-DDTHH:MM.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			Importer.Paths = args
			return Importer.Run(context.Background())
		},
	}

	flags := importCmd.Flags()
	flags.StringVarP(&Importer.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&Importer.Index, "index", "i", "", "Pilosa index to import into.")
	flags.StringVarP(&Importer.Field, "field", "f", "", "Field to import into.")
	flags.BoolVar(&Importer.IndexKeys, "index-keys", false, "use keys=true when creating an index")
	flags.BoolVar(&Importer.FieldKeys, "field-keys", false, "use keys=true when creating a field")
	flags.IntVarP(&Importer.BufferSize, "buffer-size", "s", 10000000, "Number of bits to buffer/sort before importing.")
	flags.BoolVarP(&Importer.Sort, "sort", "", false, "Enables sorting before import.")
	flags.BoolVarP(&Importer.CreateSchema, "create", "e", false, "Create the schema if it does not exist before import.")
	//flags.Var(&Importer.FieldOptions.TimeQuantum, "field-time-quantum", "Time quantum for the field")
	//flags.StringVar(&Importer.FieldOptions.CacheType, "field-cache-type", pilosa.CacheTypeRanked, "Cache type for the field; valid values: none, lru, ranked")
	//flags.Uint32Var(&Importer.FieldOptions.CacheSize, "field-cache-size", 50000, "Cache size for the field")
	ctl.SetTLSConfig(flags, &Importer.TLS.CertificatePath, &Importer.TLS.CertificateKeyPath, &Importer.TLS.SkipVerify)

	return importCmd
}
