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

	"github.com/pilosa/pilosa"

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
	flags.BoolVar(&Importer.IndexOptions.Keys, "index-keys", false, "Specify keys=true when creating an index")
	flags.BoolVar(&Importer.FieldOptions.Keys, "field-keys", false, "Specify keys=true when creating a field")
	flags.Int64Var(&Importer.FieldOptions.Min, "field-min", 0, "Specify the minimum for an int field on creation")
	flags.Int64Var(&Importer.FieldOptions.Max, "field-max", 0, "Specify the maximum for an int field on creation")
	flags.StringVar(&Importer.FieldOptions.CacheType, "field-cache-type", pilosa.CacheTypeRanked, "Specify the cache type for a set field on creation. One of: none, lru, ranked")
	flags.Uint32Var(&Importer.FieldOptions.CacheSize, "field-cache-size", 50000, "Specify the cache size for a set field on creation")
	flags.Var(&Importer.FieldOptions.TimeQuantum, "field-time-quantum", "Specify the time quantum for a time field on creation. One of: D, DH, H, M, MD, MDH, Y, YM, YMD, YMDH")
	flags.IntVarP(&Importer.BufferSize, "buffer-size", "s", 10000000, "Number of bits to buffer/sort before importing.")
	flags.BoolVarP(&Importer.Sort, "sort", "", false, "Enables sorting before import.")
	flags.BoolVarP(&Importer.CreateSchema, "create", "e", false, "Create the schema if it does not exist before import.")
	flags.BoolVarP(&Importer.Clear, "clear", "", false, "Clear the data provided in the import.")
	ctl.SetTLSConfig(flags, &Importer.TLS.CertificatePath, &Importer.TLS.CertificateKeyPath, &Importer.TLS.SkipVerify)

	return importCmd
}
